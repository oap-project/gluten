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
#include <memory>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/FunctionsStringSearchToString.h>
#include <Functions/IFunction.h>
#include <Functions/URL/domain.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
/// allow to return null.
template <typename Extractor>
struct ExtractNullableSubstringImpl
{
    static void vector(
        const DB::ColumnString::Chars & data,
        const DB::ColumnString::Offsets & offsets,
        DB::ColumnString::Chars & res_data,
        DB::ColumnString::Offsets & res_offsets,
        DB::IColumn & null_map)
    {
        size_t size = offsets.size();
        res_offsets.resize_exact(size);
        res_data.reserve_exact(size * Extractor::getReserveLengthForElement());
        null_map.reserve(size);

        size_t prev_offset = 0;
        size_t res_offset = 0;

        /// Matched part.
        DB::Pos start;
        size_t length;

        for (size_t i = 0; i < size; ++i)
        {
            Extractor::execute(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1, start, length);

            res_data.resize_exact(res_data.size() + length + 1);
            if (start)
            {
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
                null_map.insert(0);
            }
            else
            {
                null_map.insert(1);
            }
            res_offset += length + 1;
            res_data[res_offset - 1] = 0;

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }
};
template <typename Impl, typename Name, bool is_injective = false>
class FunctionStringToNullableString : public DB::IFunction
{
public:
    static constexpr auto name = Name::name;
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<FunctionStringToNullableString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isInjective(const DB::ColumnsWithTypeAndName &) const override { return is_injective; }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        if (!DB::isStringOrFixedString(arguments[0]))
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        return DB::makeNullable(arguments[0]);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DB::ColumnPtr
    executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const DB::ColumnPtr column = arguments[0].column;
        auto null_map = DB::DataTypeUInt8().createColumn();
        if (const DB::ColumnString * col = checkAndGetColumn<DB::ColumnString>(column.get()))
        {
            auto col_res = DB::ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), *null_map);
            return DB::ColumnNullable::create(std::move(col_res), std::move(null_map));
        }
        else
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};

template <typename Impl, typename Name>
class FunctionsStringSearchToNullableString : public DB::IFunction
{
public:
    static constexpr auto name = Name::name;
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<FunctionsStringSearchToNullableString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    DB::ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[1]->getName(), getName());

        return DB::makeNullable(std::make_shared<DB::DataTypeString>());
    }

    DB::ColumnPtr
    executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const DB::ColumnPtr column = arguments[0].column;
        const DB::ColumnPtr column_needle = arguments[1].column;

        const DB::ColumnConst * col_needle = typeid_cast<const DB::ColumnConst *>(&*column_needle);
        if (!col_needle)
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be constant string", getName());

        if (const DB::ColumnString * col = DB::checkAndGetColumn<DB::ColumnString>(column.get()))
        {
            auto col_res = DB::ColumnString::create();
            auto null_map = DB::DataTypeUInt8().createColumn();

            DB::ColumnString::Chars & vec_res = col_res->getChars();
            DB::ColumnString::Offsets & offsets_res = col_res->getOffsets();
            Impl::vector(col->getChars(), col->getOffsets(), col_needle->getValue<String>(), vec_res, offsets_res, *null_map);

            return DB::ColumnNullable::create(std::move(col_res), std::move(null_map));
        }
        else
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};

/// Different from CH extractURLParameters which returns an array result.
struct NameSparkExtractURLQuery
{
    static constexpr auto name = "spark_parse_url_query";
};

struct SparkExtractURLQuery
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(DB::Pos data, size_t size, DB::Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;
        DB::Pos pos = data;
        DB::Pos end = data + size;
        const static String protocol_delim = "://";
        DB::Pos protocol_delim_pos = static_cast<DB::Pos>(memmem(pos, end - pos, protocol_delim.data(), protocol_delim.size()));
        DB::Pos query_string_begin = nullptr;
        if (protocol_delim_pos)
            query_string_begin = find_first_symbols<'?', '#'>(pos, end);
        else
            query_string_begin = find_first_symbols<'?', '#', ':'>(pos, end);
        if (query_string_begin && query_string_begin < end)
        {
            if (*query_string_begin != '?')
            {
                res_data = nullptr;
                res_size = 0;
                return;
            }
            res_data = query_string_begin + 1;
            DB::Pos query_string_end = find_first_symbols<'#'>(res_data, end);
            if (query_string_end && query_string_end < end)
                res_size = query_string_end - res_data;
            else
                res_size = end - res_data;
        }
        else
        {
            res_data = nullptr;
            res_size = 0;
        }
    }
};
using SparkFunctionURLQuery = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLQuery>, NameSparkExtractURLQuery>;
REGISTER_FUNCTION(SparkFunctionURLQuery)
{
    factory.registerFunction<SparkFunctionURLQuery>();
}

struct NameSparkExtractURLOneQuery
{
    static constexpr auto name = "spark_parse_url_one_query";
};
struct SparkExtractURLOneQuery
{
    static void vector(
        const DB::ColumnString::Chars & data,
        const DB::ColumnString::Offsets & offsets,
        std::string pattern,
        DB::ColumnString::Chars & res_data,
        DB::ColumnString::Offsets & res_offsets,
        DB::IColumn & null_map)
    {
        const static String protocol_delim = "://";
        res_data.reserve_exact(data.size() / 5);
        res_offsets.resize_exact(offsets.size());

        pattern += '=';
        const char * param_str = pattern.c_str();
        size_t param_len = pattern.size();

        DB::ColumnString::Offset prev_offset = 0;
        DB::ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            DB::ColumnString::Offset cur_offset = offsets[i];

            const char * str = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * end = reinterpret_cast<const char *>(&data[cur_offset]);

            /// Find query string or fragment identifier.
            /// Note that we support parameters in fragment identifier in the same way as in query string.
            DB::Pos protocol_delim_pos = static_cast<DB::Pos>(memmem(str, end - str, protocol_delim.data(), protocol_delim.size()));
            DB::Pos query_string_begin = nullptr;
            if (protocol_delim_pos)
                query_string_begin = find_first_symbols<'?', '#'>(protocol_delim_pos, end);
            else
                query_string_begin = find_first_symbols<'?', '#', ':'>(str, end);

            if (*query_string_begin != '?')
                query_string_begin = end;

            /// Will point to the beginning of "name=value" pair. Then it will be reassigned to the beginning of "value".
            const char * param_begin = nullptr;

            if (query_string_begin + 1 < end)
            {
                param_begin = query_string_begin + 1;

                while (true)
                {
                    param_begin = static_cast<const char *>(memmem(param_begin, end - param_begin, param_str, param_len));

                    if (!param_begin)
                        break;

                    if (param_begin[-1] != '?' && param_begin[-1] != '#' && param_begin[-1] != '&')
                    {
                        /// Parameter name is different but has the same suffix.
                        param_begin += param_len;
                        continue;
                    }
                    else
                    {
                        param_begin += param_len;
                        break;
                    }
                }
            }

            if (param_begin)
            {
                const char * param_end = find_first_symbols<'&', '#'>(param_begin, end);
                if (param_end == end)
                    param_end = param_begin + strlen(param_begin);

                size_t param_size = param_end - param_begin;

                res_data.resize_exact(res_offset + param_size + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], param_begin, param_size);
                res_offset += param_size;
                null_map.insert(0);
            }
            else
            {
                /// No parameter found, put empty string in result.
                res_data.resize_exact(res_offset + 1);
                null_map.insert(1);
            }

            res_data[res_offset] = 0;
            ++res_offset;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }
};
using SparkFunctionURLOneQuery = FunctionsStringSearchToNullableString<SparkExtractURLOneQuery, NameSparkExtractURLOneQuery>;
REGISTER_FUNCTION(SparkFunctionURLOneQuery)
{
    factory.registerFunction<SparkFunctionURLOneQuery>();
}


struct SparkExtractURLHost
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(DB::Pos data, size_t size, DB::Pos & res_data, size_t & res_size)
    {
        DB::Pos end = data + size;
        const static String protocol_delim = "://";
        DB::Pos protocol_delim_start = static_cast<DB::Pos>(memmem(data, size, protocol_delim.data(), protocol_delim.size()));
        if (!protocol_delim_start)
        {
            res_data = nullptr;
            res_size = 0;
            return;
        }
        DB::Pos userinfo_delim_pos = find_first_symbols<'@'>(protocol_delim_start + protocol_delim.size(), end);
        std::string_view host;
        if (userinfo_delim_pos && userinfo_delim_pos < end)
            host = DB::getURLHost(userinfo_delim_pos + 1, end - userinfo_delim_pos);
        else
            host = DB::getURLHost(protocol_delim_start + protocol_delim.size(), end - protocol_delim_start - protocol_delim.size());

        if (host.empty())
        {
            res_data = data;
            res_size = 0;
        }
        else
        {
            res_data = host.data();
            res_size = host.size();
        }
    }
};

struct NameSparkExtractURLHost
{
    static constexpr auto name = "spark_parse_url_host";
};
using SparkFunctionURLHost = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLHost>, NameSparkExtractURLHost>;
REGISTER_FUNCTION(SparkFunctionURLHost)
{
    factory.registerFunction<SparkFunctionURLHost>();
}

struct NameSparkExtractURLPath
{
    static constexpr auto name = "spark_parse_url_path";
};
struct SparkExtractURLPath
{
    static size_t getReserveLengthForElement() { return 25; }

    static void execute(DB::Pos data, size_t size, DB::Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;
        DB::Pos pos = data;
        DB::Pos end = data + size;
        const static String protocol_delim = "://";
        const auto * start_pos = static_cast<DB::Pos>(memmem(pos, end - pos, protocol_delim.data(), protocol_delim.size()));
        if (start_pos)
        {
            start_pos += protocol_delim.size();
            const auto * path_start_pos = find_first_symbols<'/', '#', '?'>(start_pos, end);
            if (path_start_pos && path_start_pos < end)
            {
                if (*path_start_pos != '/')
                    return;
                res_data = path_start_pos;
                const auto * path_end_pos = find_first_symbols<'?', '#'>(path_start_pos, end);
                if (path_end_pos && path_end_pos < end)
                    res_size = path_end_pos - path_start_pos;
                else
                    res_size = end - path_start_pos;
            }
        }
    }
};
using SparkFunctionURLPath = DB::FunctionStringToString<DB::ExtractSubstringImpl<SparkExtractURLPath>, NameSparkExtractURLPath>;
REGISTER_FUNCTION(SparkFunctionURLPath)
{
    factory.registerFunction<SparkFunctionURLPath>();
}

struct NameSparkExtractUserInfo
{
    static constexpr auto name = "spark_parse_url_userinfo";
};
struct SparkExtractURLUserInfo
{
    static size_t getReserveLengthForElement() { return 25; }
    static void execute(DB::Pos data, size_t size, DB::Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;
        DB::Pos pos = data;
        DB::Pos end = data + size;
        const static String protocol_delim = "://";
        const static String userinfo_delim = "@";
        DB::Pos protocol_delim_start = static_cast<DB::Pos>(memmem(pos, end - pos, protocol_delim.data(), protocol_delim.size()));
        if (!protocol_delim_start)
        {
            res_data = nullptr;
            res_size = 0;
            return;
        }
        res_data = protocol_delim_start + protocol_delim.size();
        DB::Pos userinfo_delim_start = find_first_symbols<'@'>(res_data, end);
        if (!userinfo_delim_start || userinfo_delim_start >= end)
        {
            res_data = nullptr;
            res_size = 0;
            return;
        }
        res_size = userinfo_delim_start - res_data;
    }
};
using SparkFunctionURLUserInfo
    = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLUserInfo>, NameSparkExtractUserInfo>;
REGISTER_FUNCTION(SparkFunctionURLUserInfo)
{
    factory.registerFunction<SparkFunctionURLUserInfo>();
}

struct NameSparkExtractURLRef
{
    static constexpr auto name = "spark_parse_url_ref";
};
struct SparkExtractURLRef
{
    static size_t getReserveLengthForElement() { return 25; }
    static void execute(DB::Pos data, size_t size, DB::Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;
        DB::Pos pos = data;
        DB::Pos end = data + size;
        const static String ref_delim = "#";
        const auto * ref_delim_pos = find_first_symbols<'#'>(pos, end);
        if (ref_delim_pos && ref_delim_pos < end)
        {
            res_data = ref_delim_pos + 1;
            res_size = end - res_data;
        }
        else
        {
            res_data = nullptr;
            res_size = 0;
        }
    }
};
using SparkFunctionURLRef = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLRef>, NameSparkExtractURLRef>;
REGISTER_FUNCTION(SparkFunctionURLRef)
{
    factory.registerFunction<SparkFunctionURLRef>();
}

struct NameSparkExtractURLFile
{
    static constexpr auto name = "spark_parse_url_file";
};
struct SparkExtractURLFile
{
    static size_t getReserveLengthForElement() { return 25; }
    static void execute(DB::Pos data, size_t size, DB::Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;
        DB::Pos pos = data;
        DB::Pos end = data + size;
        const static String protocol_delim = "://";
        const static String slash_delim = "/";
        const static String query_delim = "?";
        const auto * protocol_delim_pos = static_cast<DB::Pos>(memmem(pos, end - pos, protocol_delim.data(), protocol_delim.size()));
        if (!protocol_delim_pos)
        {
            auto colon_pos = find_first_symbols<':'>(pos, end);
            if (colon_pos && colon_pos + 1 < end)
            {
                res_data = nullptr;
                return;
            }
            res_size = size;
            return;
        }
        DB::Pos file_begin_pos = find_first_symbols<'/', '?', '#'>(protocol_delim_pos + protocol_delim.size(), end);
        if (file_begin_pos && file_begin_pos < end)
        {
            if (*file_begin_pos == '#')
                return;
            res_data = file_begin_pos;
            DB::Pos ref_delim_pos = find_first_symbols<'#'>(file_begin_pos + 1, end);
            if (ref_delim_pos && ref_delim_pos < end)
                res_size = ref_delim_pos - res_data;
            else
                res_size = end - res_data;
        }
    }
};
using SparkFunctionURLFile = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLFile>, NameSparkExtractURLFile>;
REGISTER_FUNCTION(SparkFunctionURLFile)
{
    factory.registerFunction<SparkFunctionURLFile>();
}

struct NameSparkExtractURLAuthority
{
    static constexpr auto name = "spark_parse_url_authority";
};
struct SparkExtractURLAuthority
{
    static size_t getReserveLengthForElement() { return 25; }
    static void execute(DB::Pos data, size_t size, DB::Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;
        DB::Pos pos = data;
        DB::Pos end = data + size;
        const static String protocol_delim = "://";
        const auto * protocol_delim_pos = static_cast<DB::Pos>(memmem(pos, end - pos, protocol_delim.data(), protocol_delim.size()));
        if (!protocol_delim_pos)
        {
            res_data = nullptr;
            res_size = 0;
            return;
        }
        res_data = protocol_delim_pos + protocol_delim.size();
        DB::Pos end_pos = find_first_symbols<'/', '?', '#'>(res_data, end);
        if (end_pos)
            res_size = end_pos - res_data;
        else
            res_size = end - res_data - 1;
    }
};
using SparkFunctionURLAuthority
    = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLAuthority>, NameSparkExtractURLAuthority>;
REGISTER_FUNCTION(SparkFunctionURLAuthority)
{
    factory.registerFunction<SparkFunctionURLAuthority>();
}

struct NameSparkExtractURLInvalid
{
    static constexpr auto name = "spark_parse_url_invalid";
};

struct SparkExtractURLInvalid
{
    static size_t getReserveLengthForElement() { return 1; }
    static void execute(DB::Pos, size_t, DB::Pos & res_data, size_t & res_size)
    {
        res_data = nullptr;
        res_size = 0;
    }
};
using SparkFunctionURLInvalid
    = FunctionStringToNullableString<ExtractNullableSubstringImpl<SparkExtractURLInvalid>, NameSparkExtractURLInvalid>;
REGISTER_FUNCTION(SparkFunctionURLInvalid)
{
    factory.registerFunction<SparkFunctionURLInvalid>();
}
}
