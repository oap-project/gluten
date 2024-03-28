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

#include "TextOutputFormatFile.h"

#if USE_ORC
#    include <Formats/FormatFactory.h>
#    include <Processors/Formats/Impl/CSVRowOutputFormat.h>

namespace local_engine
{
TextOutputFormatFile::TextOutputFormatFile(
    DB::ContextPtr context_,
    const std::string & file_uri_,
    WriteBufferBuilderPtr write_buffer_builder_,
    const std::vector<std::string> & preferred_column_names_)
    : OutputFormatFile(context_, file_uri_, write_buffer_builder_, preferred_column_names_)
{
}

OutputFormatFile::OutputFormatPtr TextOutputFormatFile::createOutputFormat(const DB::Block & header)
{
    auto res = std::make_shared<OutputFormatFile::OutputFormat>();
    res->write_buffer = write_buffer_builder->build(file_uri);

    auto new_header = creatHeaderWithPreferredColumnNames(header);
    // TODO: align all spark text config with ch text config
    auto format_settings = DB::getFormatSettings(context);
    auto output_format = std::make_shared<DB::CSVRowOutputFormat>(*(res->write_buffer), new_header, false, false, format_settings);
    res->output = output_format;
    return res;
}
}
#endif
