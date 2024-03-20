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
#include <Parser/ExpandField.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

namespace local_engine
{
// For handling substrait expand node.
// The implementation in spark for groupingsets/rollup/cube is different from Clickhouse.
// We have to ways to support groupingsets/rollup/cube
// - rewrite the substrait plan in local engine and reuse the implementation of clickhouse. This
//   may be more complex.
// - implement new transform to do the expandation. It's more simple, but may suffer some performance
//   issues. We try this first.
class ExpandTransform : public DB::IProcessor
{
public:
    using Status = DB::IProcessor::Status;
    ExpandTransform(const DB::Block & input_, const DB::Block & output_, const ExpandField & project_set_exprs_);

    Status prepare() override;
    void work() override;

    String getName() const override { return "ExpandTransform"; }

private:
    ExpandField project_set_exprs;
    bool has_input = false;
    bool has_output = false;
    size_t expand_expr_iterator = 0;

    DB::Chunk input_chunk;
    DB::Chunk output_chunk;
};
}
