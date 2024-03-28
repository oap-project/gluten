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
#include <boost/algorithm/string/case_conv.hpp>

#include "ORCOutputFormatFile.h"
#include "OutputFormatFile.h"
#include "ParquetOutputFormatFile.h"
#include "TextOutputFormatFile.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

}
namespace local_engine
{

using namespace DB;

OutputFormatFile::OutputFormatFile(
    DB::ContextPtr context_,
    const std::string & file_uri_,
    WriteBufferBuilderPtr write_buffer_builder_,
    const std::vector<std::string> & preferred_column_names_)
    : context(context_), file_uri(file_uri_), write_buffer_builder(write_buffer_builder_), preferred_column_names(preferred_column_names_)
{
}

Block OutputFormatFile::creatHeaderWithPreferredColumnNames(const Block & header)
{
    if (!preferred_column_names.empty())
    {
        /// Create a new header with the preferred column name
        DB::NamesAndTypesList names_types_list = header.getNamesAndTypesList();
        DB::ColumnsWithTypeAndName cols;
        size_t index = 0;
        for (const auto & name_type : header.getNamesAndTypesList())
        {
            if (name_type.name.starts_with("__bucket_value__"))
                continue;

            DB::ColumnWithTypeAndName col(name_type.type->createColumn(), name_type.type, preferred_column_names.at(index++));
            cols.emplace_back(std::move(col));
        }
        assert(preferred_column_names.size() == index);
        return {std::move(cols)};
    }
    else
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "preferred_column_names is empty");
}

OutputFormatFilePtr OutputFormatFileUtil::createFile(
    DB::ContextPtr context,
    local_engine::WriteBufferBuilderPtr write_buffer_builder,
    const std::string & file_uri,
    const std::vector<std::string> & preferred_column_names,
    const std::string & format_hint)
{
#if USE_PARQUET
    if (boost::to_lower_copy(file_uri).ends_with(".parquet") || "parquet" == boost::to_lower_copy(format_hint))
        return std::make_shared<ParquetOutputFormatFile>(context, file_uri, write_buffer_builder, preferred_column_names);
#endif

#if USE_ORC
    if (boost::to_lower_copy(file_uri).ends_with(".orc") || "orc" == boost::to_lower_copy(format_hint))
        return std::make_shared<ORCOutputFormatFile>(context, file_uri, write_buffer_builder, preferred_column_names);
#endif

    if ("text" == boost::to_lower_copy(format_hint) || "csv" == boost::to_lower_copy(format_hint))
	return std::make_shared<TextOutputFormatFile>(context, file_uri, write_buffer_builder, preferred_column_names);

    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Format not supported for file :{}", file_uri);
}
}
