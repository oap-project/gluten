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
#include "FormatFile.h"

#include <memory>
#include <IO/ReadBufferFromFile.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>

#if USE_PARQUET
#include <Storages/SubstraitSource/ParquetFormatFile.h>
#endif

#if USE_ORC
#include <Storages/SubstraitSource/ORCFormatFile.h>
#endif

#if USE_HIVE
#include <Storages/SubstraitSource/ExcelTextFormatFile.h>
#include <Storages/SubstraitSource/TextFormatFile.h>
#endif

#include <Storages/SubstraitSource/JSONFormatFile.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}
namespace local_engine
{
FormatFile::FormatFile(
    DB::ContextPtr context_,
    const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_,
    const ReadBufferBuilderPtr & read_buffer_builder_)
    : context(context_), file_info(file_info_), read_buffer_builder(read_buffer_builder_)
{
    PartitionValues part_vals = StringUtils::parsePartitionTablePath(file_info.uri_file());
    String partition_values_str = "[";
    for (size_t i = 0; i < part_vals.size(); ++i)
    {
        const auto & part = part_vals[i];
        partition_keys.push_back(part.first);
        partition_values[part.first] = part.second;
        if (i > 0)
            partition_values_str += ", ";
        partition_values_str += part.first + "=" + part.second;
    }
    partition_values_str += "]";
    LOG_INFO(
        &Poco::Logger::get("FormatFile"),
        "Reading File path: {}, format: {}, range: {}, partition_index: {}, partition_values: {}",
        file_info.uri_file(),
        file_info.file_format_case(),
        std::to_string(file_info.start()) + "-" + std::to_string(file_info.start() + file_info.length()),
        file_info.partition_index(),
        partition_values_str);
}

FormatFilePtr FormatFileUtil::createFile(
    DB::ContextPtr context, ReadBufferBuilderPtr read_buffer_builder, const substrait::ReadRel::LocalFiles::FileOrFiles & file)
{
#if USE_PARQUET
    if (file.has_parquet())
    {
        bool useLocalFormat = context->getConfigRef().getBool("use_local_format", false);
        return std::make_shared<ParquetFormatFile>(context, file, read_buffer_builder, useLocalFormat);
    }
#endif

#if USE_ORC
    if (file.has_orc())
        return std::make_shared<ORCFormatFile>(context, file, read_buffer_builder);
#endif

#if USE_HIVE
    if (file.has_text())
    {
        if (context->getSettings().has(BackendInitializerUtil::USE_EXCEL_PARSER)
            && context->getSettings().getString(BackendInitializerUtil::USE_EXCEL_PARSER) == "'true'")
            return std::make_shared<ExcelTextFormatFile>(context, file, read_buffer_builder);
        else
            return std::make_shared<TextFormatFile>(context, file, read_buffer_builder);
    }
#endif

    if (file.has_json())
        return std::make_shared<JSONFormatFile>(context, file, read_buffer_builder);
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Format not supported:{}", file.DebugString());
    __builtin_unreachable();
}
}
