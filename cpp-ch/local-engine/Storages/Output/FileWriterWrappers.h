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

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/Output/OutputFormatFile.h>

namespace local_engine
{

class FileWriterWrapper
{
public:
    explicit FileWriterWrapper(OutputFormatFilePtr file_) : file(file_) { }
    virtual ~FileWriterWrapper() = default;
    virtual void consume(DB::Block & block) = 0;
    virtual void close() = 0;

protected:
    OutputFormatFilePtr file;
};

using FileWriterWrapperPtr = std::shared_ptr<FileWriterWrapper>;

class NormalFileWriter : public FileWriterWrapper
{
public:
    //TODO: EmptyFileReader and ConstColumnsFileReader ?
    //TODO: to support complex types
    NormalFileWriter(OutputFormatFilePtr file_, DB::ContextPtr context_);
    ~NormalFileWriter() override = default;
    void consume(DB::Block & block) override;
    void close() override;

private:
    DB::ContextPtr context;

    OutputFormatFile::OutputFormatPtr output_format;
    std::unique_ptr<DB::QueryPipeline> pipeline;
    std::unique_ptr<DB::PushingPipelineExecutor> writer;
};

FileWriterWrapper * createFileWriterWrapper(
    const std::string & file_uri, const std::vector<std::string> & preferred_column_names, const std::string & format_hint);
}
