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
#include <optional>
#include <vector>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Parser/TypeParser.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <substrait/plan.pb.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{
class FormatFile
{
public:
    struct InputFormat
    {
        std::unique_ptr<DB::ReadBuffer> read_buffer;
        DB::InputFormatPtr input;
    };
    using InputFormatPtr = std::shared_ptr<InputFormat>;

    FormatFile(
        DB::ContextPtr context_,
        const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_,
        const ReadBufferBuilderPtr & read_buffer_builder_);
    virtual ~FormatFile() = default;

    /// Create a new input format for reading this file
    virtual InputFormatPtr createInputFormat(const DB::Block & header) = 0;

    /// Spark would split a large file into small segements and read in different tasks
    /// If this file doesn't support the split feacture, only the task with offset 0 will generate data.
    virtual bool supportSplit() const { return false; }

    /// Try to get rows from file metadata
    virtual std::optional<size_t> getTotalRows() { return {}; }

    /// Get partition keys from file path
    inline const std::vector<String> & getFilePartitionKeys() const { return partition_keys; }

    inline const std::map<String, String> & getFilePartitionValues() const { return partition_values; }

    virtual String getURIPath() const { return file_info.uri_file(); }

    virtual size_t getStartOffset() const { return file_info.start(); }
    virtual size_t getLength() const { return file_info.length(); }
    virtual String getFileFormat() const = 0;

protected:
    DB::ContextPtr context;
    substrait::ReadRel::LocalFiles::FileOrFiles file_info;
    ReadBufferBuilderPtr read_buffer_builder;
    std::vector<String> partition_keys;
    std::map<String, String> partition_values;
    // std::optional<SourceFilter> filter;
    std::shared_ptr<const DB::KeyCondition> key_condition;
};
using FormatFilePtr = std::shared_ptr<FormatFile>;
using FormatFiles = std::vector<FormatFilePtr>;

class FormatFileUtil
{
public:
    static FormatFilePtr
    createFile(DB::ContextPtr context, ReadBufferBuilderPtr read_buffer_builder, const substrait::ReadRel::LocalFiles::FileOrFiles & file);
};
}
