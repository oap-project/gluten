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
#include <type_traits>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/Exception.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_COLUMN;
}
}

namespace local_engine
{
class SparkFunctionStrToMap : public DB::IFunction
{
public:
    using Pos = const char *;
    static constexpr auto name = "spark_str_to_map";
    static DB::FunctionPtr create(const DB::ContextPtr) { return std::make_shared<SparkFunctionStrToMap>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }
    DB::ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3)
        {
            throw DB::Exception(
                DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 3 arguments, passed {}",
                getName(),
                arguments.size());
        }

        if (!DB::WhichDataType(DB::removeNullable(arguments[0].type)).isString()
            || !DB::WhichDataType(DB::removeNullable(arguments[1].type)).isString()
            || !DB::WhichDataType(DB::removeNullable(arguments[2].type)).isString())
        {
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "All arguments for function {} must be String", getName());
        }
        auto map_typ = std::make_shared<DB::DataTypeMap>(
            std::make_shared<DB::DataTypeString>(), makeNullable(std::make_shared<DB::DataTypeString>()));
        if (arguments[0].type->isNullable())
            return std::make_shared<DB::DataTypeNullable>(map_typ);
        else
            return map_typ;
    }

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        auto map_col = result_type->createColumn();
        auto pair_delim = (*arguments[1].column)[0].safeGet<String>();
        auto pair_delim_len = pair_delim.size();
        auto kv_delim = (*arguments[2].column)[0].safeGet<String>();
        auto kv_delim_len = kv_delim.size();
        const DB::IColumn * arg0 = arguments[0].column.get();
        bool is_nullable = false;
        if (arg0->isNullable())
        {
            arg0 = DB::checkAndGetColumn<DB::ColumnNullable>(arg0);
            is_nullable = true;
        }
        const auto * str_col = DB::checkAndGetColumn<DB::ColumnString>(arguments[0].column.get());
        if (!str_col) [[unlikely]]
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "argument 0 for function {} must be String", getName());
        const DB::ColumnString::Chars & str_vec = str_col->getChars();
        const DB::ColumnString::Offsets & str_offsets = str_col->getOffsets();
        map_col->reserve(str_offsets.size());

        DB::ColumnString::Offset prev_offset = 0;
        for (size_t i = 0, n = str_offsets.size(); i < n; ++i)
        {
            if (is_nullable && str_col->isNullAt(i))
            {
                map_col->insertDefault();
            }
            else
            {
                DB::Map map;
                Pos pair_begin = reinterpret_cast<const char *>(&str_vec[prev_offset]);
                Pos str_end = reinterpret_cast<const char *>(&str_vec[str_offsets[i]]);
                while (pair_begin < str_end)
                {
                    // Get next pair.
                    auto next_pair_begin
                        = static_cast<const char *>(memmem(pair_begin, str_end - pair_begin, pair_delim.c_str(), pair_delim_len));
                    if (!next_pair_begin) [[unlikely]]
                        next_pair_begin = str_end - 1;
                    Pos value_begin
                        = static_cast<const char *>(memmem(pair_begin, next_pair_begin - pair_begin, kv_delim.c_str(), kv_delim_len));
                    DB::Field key;
                    DB::Field value;
                    if (!value_begin)
                    {
                        key = std::string_view(pair_begin, next_pair_begin - pair_begin);
                        value = DB::Null();
                    }
                    else
                    {
                        key = std::string_view(pair_begin, value_begin - pair_begin);
                        value = std::string_view(value_begin + kv_delim_len, next_pair_begin - value_begin - kv_delim_len);
                    }
                    DB::Tuple tuple(2);
                    tuple[0] = std::move(key);
                    tuple[1] = std::move(value);
                    map.emplace_back(std::move(tuple));

                    pair_begin = next_pair_begin + pair_delim_len;
                }
                map_col->insert(map);
            }
            prev_offset = str_offsets[i];
        }
        return map_col;
    }
};
REGISTER_FUNCTION(SparkFunctionStrToMap)
{
    factory.registerFunction<SparkFunctionStrToMap>();
}
}
