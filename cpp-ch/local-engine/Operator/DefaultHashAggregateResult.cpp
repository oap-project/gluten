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
#include "DefaultHashAggregateResult.h"
#include <Columns/ColumnNullable.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <Operator/ExpandTransorm.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CHUtil.h>


namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

/// A more special case, the aggregate functions is also empty.
/// We add a fake block to downstream.
DB::Block adjustOutputHeader(const DB::Block & original_block)
{
    if (original_block)
        return original_block;
    return BlockUtil::buildRowCountHeader();
}

class DefaultHashAggrgateResultTransform : public DB::IProcessor
{
public:
    explicit DefaultHashAggrgateResultTransform(const DB::Block & input_)
        : DB::IProcessor({input_}, {adjustOutputHeader(input_)}), header(input_)
    {
    }
    ~DefaultHashAggrgateResultTransform() override = default;
    void work() override
    {
        if (has_input)
        {
            has_input = false;
            has_output = true;
        }
    }
    Status prepare() override
    {
        auto & output = outputs.front();
        auto & input = inputs.front();
        if (output.isFinished())
        {
            input.close();
            return Status::Finished;
        }
        if (has_output)
        {
            if (output.canPush())
            {
                output.push(std::move(output_chunk));
                has_output = false;
                has_outputed = true;
            }
            return Status::PortFull;
        }

        if (has_input)
            return Status::Ready;

        if (input.isFinished())
        {
            if (has_outputed)
            {
                output.finish();
                return Status::Finished;
            }
            DB::Columns result_cols;
            if (header)
            {
                for (const auto & col : header.getColumnsWithTypeAndName())
                {
                    auto result_col = col.type->createColumnConst(1, col.type->getDefault());
                    result_cols.emplace_back(result_col);
                }
            }
            else
            {
                auto cnt_chunk = BlockUtil::buildRowCountChunk(1);
                result_cols = cnt_chunk.detachColumns();
            }
            has_input = true;
            output_chunk = DB::Chunk(result_cols, 1);
            auto info = std::make_shared<DB::AggregatedChunkInfo>();
            output_chunk.setChunkInfo(info);
            return Status::Ready;
        }

        input.setNeeded();
        if (input.hasData())
        {
            output_chunk = input.pull(true);
            if (!output_chunk.hasChunkInfo())
            {
                auto info = std::make_shared<DB::AggregatedChunkInfo>();
                output_chunk.setChunkInfo(info);
            }
            has_input = true;
            return Status::Ready;
        }
        return Status::NeedData;
    }

    String getName() const override { return "DefaultHashAggrgateResultTransform"; }

private:
    DB::Block header;
    bool has_input = false;
    bool has_output = false;
    bool has_outputed = false;
    DB::Chunk output_chunk;
};

DefaultHashAggregateResultStep::DefaultHashAggregateResultStep(const DB::DataStream & input_stream_)
    : DB::ITransformingStep(input_stream_, input_stream_.header, getTraits())
{
}

void DefaultHashAggregateResultStep::transformPipeline(
    DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    auto num_streams = pipeline.getNumStreams();
    pipeline.resize(1);
    auto build_transform = [&](DB::OutputPortRawPtrs outputs)
    {
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto op = std::make_shared<DefaultHashAggrgateResultTransform>(output->getHeader());
            new_processors.push_back(op);
            DB::connect(*output, op->getInputs().front());
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
    pipeline.resize(num_streams);
}

void DefaultHashAggregateResultStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}

void DefaultHashAggregateResultStep::updateOutputStream()
{
    createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
}
}
