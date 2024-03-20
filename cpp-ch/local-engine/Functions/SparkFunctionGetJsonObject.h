/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <memory>
#include <stack>
#include <string_view>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionSQLJSON.h>
#include <Functions/IFunction.h>
#include <Functions/JSONPath/Generator/GeneratorJSONPath.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Interpreters/Context.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>
#include <base/range.h>
#include <Poco/StringTokenizer.h>
#include <Common/Exception.h>
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}
namespace local_engine
{
// We notice that, `get_json_object` have different behavior with `JSON_VALUE/JSON_QUERY`.
// - ('{"x":[{"y":1},{"y":2}]}' '$.x[*].y'), `json_value` return only one element, but `get_json_object` return
//   return a list.
// - ('{"x":[{"y":1}]}' '$.x[*].y'), `json_query`'s result is '[1]',
//   but `get_json_object`'s result is '1'
//

struct GetJsonObject
{
    static constexpr auto name{"get_json_object"};
};

template <typename JSONParser, typename JSONStringSerializer>
class GetJsonObjectImpl
{
public:
    using Element = typename JSONParser::Element;

    static DB::DataTypePtr getReturnType(const char *, const DB::ColumnsWithTypeAndName &, const DB::ContextPtr &)
    {
        auto nested_type = std::make_shared<DB::DataTypeString>();
        return std::make_shared<DB::DataTypeNullable>(nested_type);
    }

    static size_t getNumberOfIndexArguments(const DB::ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    bool insertResultToColumn(
        DB::IColumn & dest, const Element & root, DB::GeneratorJSONPath<JSONParser> & generator_json_path, const DB::ContextPtr &)
    {
        Element current_element = root;
        DB::VisitorStatus status;
        std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        /// Create json array of results: [res1, res2, ...]
        bool success = false;
        std::vector<Element> elements;
        while ((status = generator_json_path.getNextItem(current_element)) != DB::VisitorStatus::Exhausted)
        {
            if (status == DB::VisitorStatus::Ok)
            {
                success = true;
                elements.push_back(current_element);
            }
            else if (status == DB::VisitorStatus::Error)
            {
                /// ON ERROR
                /// Here it is possible to handle errors with ON ERROR (as described in ISO/IEC TR 19075-6),
                ///  however this functionality is not implemented yet
            }
            current_element = root;
        }
        if (!success)
            return false;
        DB::ColumnNullable & nullable_col_str = assert_cast<DB::ColumnNullable &>(dest);
        DB::ColumnString * col_str = assert_cast<DB::ColumnString *>(&nullable_col_str.getNestedColumn());
        JSONStringSerializer serializer(*col_str);
        if (elements.size() == 1) [[likely]]
        {
            nullable_col_str.getNullMapData().push_back(0);
            if (elements[0].isString())
            {
                auto str = elements[0].getString();
                serializer.addRawString(str);
            }
            else
            {
                serializer.addElement(elements[0]);
            }
        }
        else
        {
            const char * array_begin = "[";
            const char * array_end = "]";
            const char * comma = ", ";
            bool flag = false;
            serializer.addRawData(array_begin, 1);
            for (auto & element : elements)
            {
                nullable_col_str.getNullMapData().push_back(0);
                if (flag)
                    serializer.addRawData(comma, 2);
                serializer.addElement(element);
                flag = true;
            }
            serializer.addRawData(array_end, 1);
        }
        serializer.commit();
        return true;
    }

private:
};

/// Flatten a json string into a tuple.
/// Not use JSONExtract here, since the json path is a complicated expression.
class FlattenJSONStringOnRequiredFunction : public DB::IFunction
{
public:
    static constexpr auto name = "flattenJSONStringOnRequired";

    static DB::FunctionPtr create(const DB::ContextPtr & context) { return std::make_shared<FlattenJSONStringOnRequiredFunction>(context); }
    explicit FlattenJSONStringOnRequiredFunction(DB::ContextPtr context_) : context(context_) { }
    ~FlattenJSONStringOnRequiredFunction() override = default;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        String json_fields;
        if (const auto * json_fields_col = typeid_cast<const DB::ColumnConst *>(arguments[1].column.get()))
        {
            json_fields = json_fields_col->getDataAt(0).toString();
        }
        else
        {
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument of function {} must be a non-constant column", getName());
        }

        Poco::StringTokenizer tokenizer(json_fields, "|");
        std::vector<String> names;
        DB::DataTypes types;
        DB::DataTypePtr str_type = std::make_shared<DB::DataTypeString>();
        str_type = DB::makeNullable(str_type);
        for (const auto & field : tokenizer)
        {
            names.push_back(field);
            types.push_back(str_type);
        }
        return std::make_shared<DB::DataTypeTuple>(types, names);
    }

    /// The second argument is required json fields sperated by '|'.
    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
#if USE_SIMDJSON
        if (context->getSettingsRef().allow_simdjson)
        {
            return innerExecuteImpl<
                DB::SimdJSONParser,
                GetJsonObjectImpl<DB::SimdJSONParser, DB::JSONStringSerializer<DB::SimdJSONParser::Element, DB::SimdJSONElementFormatter>>>(
                arguments);
        }
#endif
        return innerExecuteImpl<
            DB::DummyJSONParser,
            GetJsonObjectImpl<DB::DummyJSONParser, DB::DefaultJSONStringSerializer<DB::DummyJSONParser::Element>>>(arguments);
    }

private:
    DB::ContextPtr context;

    void parseAbnormalJson(char * dst, std::string_view & json) const
    {
        const char * json_chars = json.data();
        const size_t json_size = json.size();
        UInt8 NULL_CHAR = 0x0000;
        UInt8 SPACE_CHAR = 0x0020;
        std::stack<char> tmp;
        size_t cursor = 0;
        for (size_t i = 0; i <= json_size; ++i)
        {
            if (*(json_chars + i) > NULL_CHAR && *(json_chars + i) < SPACE_CHAR)
                continue;
            else
            {
                char ch = *(json_chars + i);
                dst[cursor++] = ch;
                if (ch == '{')
                    tmp.push('{');
                else if (ch == '}')
                {
                    if (tmp.top() == '{')
                        tmp.pop();
                }
                if (tmp.empty())
                    break;
            }
        }
        std::string_view result{dst, cursor};
        json = result;
    }

    template <typename JSONParser, typename Impl>
    DB::ColumnPtr innerExecuteImpl(const DB::ColumnsWithTypeAndName & arguments) const
    {
        DB::DataTypePtr str_type = std::make_shared<DB::DataTypeString>();
        str_type = DB::makeNullable(str_type);
        DB::MutableColumns tuple_columns;
        std::vector<DB::ASTPtr> json_path_asts;

        std::vector<String> required_fields;
        const auto & first_column = arguments[0];
        if (const auto * required_fields_col = typeid_cast<const DB::ColumnConst *>(arguments[1].column.get()))
        {
            std::string json_fields = required_fields_col->getDataAt(0).toString();
            Poco::StringTokenizer tokenizer(json_fields, "|");
            bool path_parsed = true;
            for (const auto & field : tokenizer)
            {
                required_fields.push_back(field);
                tuple_columns.emplace_back(str_type->createColumn());

                const char * query_begin = reinterpret_cast<const char *>(required_fields.back().c_str());
                const char * query_end = required_fields.back().c_str() + required_fields.back().size();
                DB::Tokens tokens(query_begin, query_end);
                UInt32 max_parser_depth = static_cast<UInt32>(context->getSettingsRef().max_parser_depth);
                UInt32 max_parser_backtracks = static_cast<UInt32>(context->getSettingsRef().max_parser_backtracks);
                DB::IParser::Pos token_iterator(tokens, max_parser_depth, max_parser_backtracks);
                DB::ASTPtr json_path_ast;
                DB::ParserJSONPath path_parser;
                DB::Expected expected;
                if (!path_parser.parse(token_iterator, json_path_ast, expected))
                    path_parsed = false;
                json_path_asts.push_back(json_path_ast);
            }
            if (!path_parsed)
            {
                for (size_t i = 0; i < first_column.column->size(); ++i)
                    for (size_t j = 0; j < tuple_columns.size(); ++j)
                        tuple_columns[j]->insertDefault();
                return DB::ColumnTuple::create(std::move(tuple_columns));
            }
        }
        else
        {
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument of function {} must be a non-constant column", getName());
        }

        if (!isString(first_column.type))
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} should be a string containing JSON, illegal type: "
                "{}",
                String(name),
                first_column.type->getName());

        const DB::ColumnPtr & arg_json = first_column.column;
        const auto * col_json_const = typeid_cast<const DB::ColumnConst *>(arg_json.get());
        const auto * col_json_string
            = typeid_cast<const DB::ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());
        if (!col_json_string)
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {}", arg_json->getName());
        const DB::ColumnString::Chars & chars = col_json_string->getChars();
        const DB::ColumnString::Offsets & offsets = col_json_string->getOffsets();

        Impl impl;
        JSONParser parser;
        using Element = typename JSONParser::Element;
        Element document;
        bool document_ok = false;
        if (col_json_const)
        {
            std::string_view json{reinterpret_cast<const char *>(chars.data()), offsets[0] - 1};
            document_ok = parser.parse(json, document);
            if (!document_ok)
            {
                char dst[json.size()];
                parseAbnormalJson(dst, json);
                document_ok = parser.parse(json, document);
            }
        }

        size_t tuple_size = tuple_columns.size();
        std::vector<std::shared_ptr<DB::GeneratorJSONPath<JSONParser>>> generator_json_paths;
        std::transform(
            json_path_asts.begin(),
            json_path_asts.end(),
            std::back_inserter(generator_json_paths),
            [](const auto & ast) { return std::make_shared<DB::GeneratorJSONPath<JSONParser>>(ast); });

        for (const auto i : collections::range(0, arguments[0].column->size()))
        {
            if (!col_json_const)
            {
                std::string_view json{reinterpret_cast<const char *>(&chars[offsets[i - 1]]), offsets[i] - offsets[i - 1] - 1};
                document_ok = parser.parse(json, document);
                if (!document_ok)
                {
                    char dst[json.size()];
                    parseAbnormalJson(dst, json);
                    document_ok = parser.parse(json, document);
                }
            }
            if (document_ok)
            {
                for (size_t j = 0; j < tuple_size; ++j)
                {
                    generator_json_paths[j]->reinitialize();
                    if (!impl.insertResultToColumn(*tuple_columns[j], document, *generator_json_paths[j], context))
                        tuple_columns[j]->insertDefault();
                }
            }
            else
            {
                for (size_t j = 0; j < tuple_size; ++j)
                    tuple_columns[j]->insertDefault();
            }
        }

        return DB::ColumnTuple::create(std::move(tuple_columns));
    }
};

}
