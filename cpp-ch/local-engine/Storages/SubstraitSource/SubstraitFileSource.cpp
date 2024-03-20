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
#include <functional>
#include <memory>

#include <substrait/plan.pb.h>
#include <magic_enum.hpp>
#include <Poco/URI.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
// When run query "select count(*) from t", there is no any column to be read.
// The number of rows is the only needed information. To handle these cases, we
// build blocks with a const virtual column to indicate how many rows is in it.
static DB::Block getRealHeader(const DB::Block & header)
{
    return header ? header : BlockUtil::buildRowCountHeader();
}

SubstraitFileSource::SubstraitFileSource(
    const DB::ContextPtr & context_, const DB::Block & header_, const substrait::ReadRel::LocalFiles & file_infos)
    : DB::SourceWithKeyCondition(getRealHeader(header_), false), context(context_), output_header(header_), to_read_header(output_header)
{
    if (file_infos.items_size())
    {
        /// Initialize files
        const Poco::URI file_uri(file_infos.items().Get(0).uri_file());
        read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context);
        for (const auto & item : file_infos.items())
            files.emplace_back(FormatFileUtil::createFile(context, read_buffer_builder, item));

        /// File partition keys are read from the file path
        const auto partition_keys = files[0]->getFilePartitionKeys();
        for (const auto & key : partition_keys)
            if (to_read_header.findByName(key))
                to_read_header.erase(key);
    }
}

void SubstraitFileSource::setKeyCondition(const DB::ActionsDAGPtr & filter_actions_dag, DB::ContextPtr context_)
{
    setKeyConditionImpl(filter_actions_dag, context_, to_read_header);
    if (filter_actions_dag)
        column_index_filter = std::make_shared<ColumnIndexFilter>(filter_actions_dag, context_);
}

DB::Chunk SubstraitFileSource::generate()
{
    while (true)
    {
        if (!tryPrepareReader())
        {
            /// all files finished
            return {};
        }

        DB::Chunk chunk;
        if (file_reader->pull(chunk))
            return chunk;

        /// try to read from next file
        file_reader.reset();
    }
}

bool SubstraitFileSource::tryPrepareReader()
{
    if (file_reader)
        return true;

    if (current_file_index >= files.size())
        return false;

    auto current_file = files[current_file_index];
    current_file_index += 1;

    if (!current_file->supportSplit() && current_file->getStartOffset())
    {
        /// For the files do not support split strategy, the task with not 0 offset will generate empty data
        file_reader = std::make_unique<EmptyFileReader>(current_file);
        return true;
    }

    if (!to_read_header)
    {
        auto total_rows = current_file->getTotalRows();
        if (total_rows.has_value())
            file_reader = std::make_unique<ConstColumnsFileReader>(current_file, context, output_header, *total_rows);
        else
        {
            /// For text/json format file, we can't get total rows from file metadata.
            /// So we add a dummy column to indicate the number of rows.
            file_reader
                = std::make_unique<NormalFileReader>(current_file, context, getRealHeader(to_read_header), getRealHeader(output_header));
        }
    }
    else
        file_reader = std::make_unique<NormalFileReader>(current_file, context, to_read_header, output_header);

    file_reader->applyKeyCondition(key_condition, column_index_filter);
    return true;
}

DB::ColumnPtr FileReaderWrapper::createConstColumn(DB::DataTypePtr data_type, const DB::Field & field, size_t rows)
{
    auto nested_type = DB::removeNullable(data_type);
    auto column = nested_type->createColumnConst(rows, field);

    if (data_type->isNullable())
        column = DB::ColumnNullable::create(column, DB::ColumnUInt8::create(rows, 0));
    return column;
}

DB::ColumnPtr FileReaderWrapper::createColumn(const String & value, DB::DataTypePtr type, size_t rows)
{
    if (StringUtils::isNullPartitionValue(value))
    {
        if (!type->isNullable())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Partition column is null value,but column data type is not nullable.");
        auto nested_type = static_cast<const DB::DataTypeNullable &>(*type).getNestedType();
        auto column = nested_type->createColumnConstWithDefaultValue(rows);
        return DB::ColumnNullable::create(column, DB::ColumnUInt8::create(rows, 1));
    }
    else
    {
        auto field = buildFieldFromString(value, type);
        return createConstColumn(type, field, rows);
    }
}

#define BUILD_INT_FIELD(type) \
    [](DB::ReadBuffer & in, const String &) \
    { \
        type value = 0; \
        DB::readIntText(value, in); \
        return DB::Field(value); \
    }

#define BUILD_FP_FIELD(type) \
    [](DB::ReadBuffer & in, const String &) \
    { \
        type value = 0.0; \
        DB::readFloatText(value, in); \
        return DB::Field(value); \
    }

DB::Field FileReaderWrapper::buildFieldFromString(const String & str_value, DB::DataTypePtr type)
{
    using FieldBuilder = std::function<DB::Field(DB::ReadBuffer &, const String &)>;
    static std::map<std::string, FieldBuilder> field_builders
        = {{"Int8", BUILD_INT_FIELD(Int8)},
           {"Int16", BUILD_INT_FIELD(Int16)},
           {"Int32", BUILD_INT_FIELD(Int32)},
           {"Int64", BUILD_INT_FIELD(Int64)},
           {"Float32", BUILD_FP_FIELD(Float32)},
           {"Float64", BUILD_FP_FIELD(Float64)},
           {"String", [](DB::ReadBuffer &, const String & val) { return DB::Field(val); }},
           {"Date",
            [](DB::ReadBuffer & in, const String &)
            {
                DayNum value;
                readDateText(value, in);
                return DB::Field(value);
            }},
           {"Date32",
            [](DB::ReadBuffer & in, const String &)
            {
                ExtendedDayNum value;
                readDateText(value, in);
                return DB::Field(value.toUnderType());
            }},
           {"Bool",
            [](DB::ReadBuffer & in, const String &)
            {
                bool value;
                readBoolTextWord(value, in, true);
                return DB::Field(value);
            }},
           {"DateTime64(6)",
            [](DB::ReadBuffer &, const String & s)
            {
                std::string decoded; // s: "2023-07-12 05%3A05%3A33.798" (spark encoded it) => decoded: "2023-07-12 05:05:33.798"
                Poco::URI::decode(s, decoded);

                std::string to_read;
                if (decoded.length() > 23) // we see cases when spark mistakely? encode the URI twice, so we need to decode twice
                    Poco::URI::decode(decoded, to_read);
                else
                    to_read = decoded;

                DB::ReadBufferFromString read_buffer(to_read);
                DB::DateTime64 value;
                DB::readDateTime64Text(value, 6, read_buffer);
                return DB::Field(value);
            }}

        };

    auto nested_type = DB::removeNullable(type);
    DB::ReadBufferFromString read_buffer(str_value);
    auto it = field_builders.find(nested_type->getName());
    if (it == field_builders.end())
    {
        DB::WhichDataType which(nested_type->getTypeId());
        if (which.isDecimal32())
        {
            auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal32> &>(*nested_type);
            DB::Decimal32 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal32>(value, dataTypeDecimal.getScale());
        }
        else if (which.isDecimal64())
        {
            auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal64> &>(*nested_type);
            DB::Decimal64 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal64>(value, dataTypeDecimal.getScale());
        }
        else if (which.isDecimal128())
        {
            auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal128> &>(*nested_type);
            DB::Decimal128 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal128>(value, dataTypeDecimal.getScale());
        }
        else if (which.isDecimal256())
        {
            auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal256> &>(*nested_type);
            DB::Decimal256 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal256>(value, dataTypeDecimal.getScale());
        }

        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unsupported data type {}", nested_type->getName());
    }
    return it->second(read_buffer, str_value);
}

ConstColumnsFileReader::ConstColumnsFileReader(FormatFilePtr file_, DB::ContextPtr context_, const DB::Block & header_, size_t block_size_)
    : FileReaderWrapper(file_), context(context_), header(header_), remained_rows(0), block_size(block_size_)
{
    auto rows = file->getTotalRows();
    if (!rows)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot get total rows number from file : {}", file->getURIPath());
    remained_rows = *rows;
}

bool ConstColumnsFileReader::pull(DB::Chunk & chunk)
{
    if (!remained_rows) [[unlikely]]
        return false;

    size_t to_read_rows = 0;
    if (remained_rows < block_size)
    {
        to_read_rows = remained_rows;
        remained_rows = 0;
    }
    else
    {
        to_read_rows = block_size;
        remained_rows -= block_size;
    }
    DB::Columns res_columns;
    if (const size_t col_num = header.columns())
    {
        res_columns.reserve(col_num);
        const auto & partition_values = file->getFilePartitionValues();
        for (size_t pos = 0; pos < col_num; ++pos)
        {
            auto col_with_name_and_type = header.getByPosition(pos);
            auto type = col_with_name_and_type.type;
            const auto & name = col_with_name_and_type.name;
            auto it = partition_values.find(name);
            if (it == partition_values.end()) [[unlikely]]
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknow partition column : {}", name);
            res_columns.emplace_back(createColumn(it->second, type, to_read_rows));
        }
    }
    else
    {
        // the original header is empty, build a block to represent the row count.
        res_columns = BlockUtil::buildRowCountChunk(to_read_rows).detachColumns();
    }

    chunk = DB::Chunk(std::move(res_columns), to_read_rows);
    return true;
}

NormalFileReader::NormalFileReader(
    const FormatFilePtr & file_, const DB::ContextPtr & context_, const DB::Block & to_read_header_, const DB::Block & output_header_)
    : FileReaderWrapper(file_), context(context_), to_read_header(to_read_header_), output_header(output_header_)
{
    input_format = file->createInputFormat(to_read_header);
}

bool NormalFileReader::pull(DB::Chunk & chunk)
{
    DB::Chunk raw_chunk = input_format->input->generate();
    const size_t rows = raw_chunk.getNumRows();
    if (!rows)
        return false;

    const auto read_columns = raw_chunk.detachColumns();
    auto columns_with_name_and_type = output_header.getColumnsWithTypeAndName();
    auto partition_values = file->getFilePartitionValues();

    DB::Columns res_columns;
    res_columns.reserve(columns_with_name_and_type.size());
    for (auto & column : columns_with_name_and_type)
    {
        if (to_read_header.has(column.name))
        {
            auto pos = to_read_header.getPositionByName(column.name);
            res_columns.push_back(read_columns[pos]);
        }
        else
        {
            auto it = partition_values.find(column.name);
            if (it == partition_values.end())
            {
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR, "Not found column({}) from file({}) partition keys.", column.name, file->getURIPath());
            }
            res_columns.push_back(createColumn(it->second, column.type, rows));
        }
    }

    chunk = DB::Chunk(std::move(res_columns), rows);
    return true;
}

}
