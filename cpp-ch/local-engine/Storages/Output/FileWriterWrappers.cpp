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
#include "FileWriterWrappers.h"
#include <algorithm>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/QueryPipeline.h>

namespace local_engine
{

NormalFileWriter::NormalFileWriter(OutputFormatFilePtr file_, DB::ContextPtr context_) : FileWriterWrapper(file_), context(context_)
{
}


void NormalFileWriter::consume(DB::Block & block)
{
    if (!writer) [[unlikely]]
    {
        // init the writer at first block
        output_format = file->createOutputFormat(block.cloneEmpty());
        pipeline = std::make_unique<DB::QueryPipeline>(output_format->output);
        writer = std::make_unique<DB::PushingPipelineExecutor>(*pipeline);
    }

    /// Although gluten will append MaterializingTransform to the end of the pipeline before native insert in most cases, there are some cases in which MaterializingTransform won't be appended.
    /// e.g. https://github.com/oap-project/gluten/issues/2900
    /// So we need to do materialize here again to make sure all blocks passed to native writer are all materialized.
    /// Note: duplicate materialization on block doesn't has any side affect.
    writer->push(materializeBlock(block));
}

void NormalFileWriter::close()
{
    /// When insert into a table with empty dataset, NormalFileWriter::consume would be never called.
    /// So we need to skip when writer is nullptr.
    if (writer)
        writer->finish();
}

FileWriterWrapper * createFileWriterWrapper(
    const std::string & file_uri, const std::vector<std::string> & preferred_column_names, const std::string & format_hint)
{
    // the passed in file_uri is exactly what is expected to see in the output folder
    // e.g /xxx/中文/timestamp_field=2023-07-13 03%3A00%3A17.622/abc.parquet
    LOG_INFO(&Poco::Logger::get("FileWriterWrappers"), "Create native writer, format_hint: {}, file: {}", format_hint, file_uri);
    std::string encoded;
    Poco::URI::encode(file_uri, "", encoded); // encode the space and % seen in the file_uri
    Poco::URI poco_uri(encoded);
    auto context = DB::Context::createCopy(local_engine::SerializedPlanParser::global_context);
    auto write_buffer_builder = WriteBufferBuilderFactory::instance().createBuilder(poco_uri.getScheme(), context);
    auto file = OutputFormatFileUtil::createFile(context, write_buffer_builder, encoded, preferred_column_names, format_hint);
    return new NormalFileWriter(file, context);
}

}
