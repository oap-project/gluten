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
#include <config.h>

#if USE_ORC
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <base/types.h>


namespace local_engine
{
struct StripeInformation
{
    UInt64 index;
    UInt64 offset;
    UInt64 length;
    UInt64 num_rows;
    UInt64 start_row;
};

class ORCFormatFile : public FormatFile
{
public:
    explicit ORCFormatFile(
        DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~ORCFormatFile() override = default;

    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;

    std::optional<size_t> getTotalRows() override;

    bool supportSplit() const override { return true; }

    String getFileFormat() const override { return "ORC"; }

private:
    mutable std::mutex mutex;
    std::optional<size_t> total_rows;

    std::vector<StripeInformation> collectRequiredStripes(UInt64 & total_stripes);
    std::vector<StripeInformation> collectRequiredStripes(DB::ReadBuffer * read_buffer, UInt64 & total_strpes);
};
}

#endif
