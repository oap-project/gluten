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
#include <Core/Defines.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace local_engine
{

class NativeReader
{
public:
    // For improving the parsing performance
    struct ColumnParseUtil
    {
        DB::DataTypePtr type = nullptr;
        std::string name;
        DB::SerializationPtr serializer = nullptr;
        size_t avg_value_size_hint = 0;

        // for aggregate data
        size_t aggregate_state_size = 0;
        size_t aggregate_state_align = 0;
        DB::AggregateFunctionPtr aggregate_function = nullptr;

        std::function<void(DB::ReadBuffer &, DB::ColumnPtr &, size_t, ColumnParseUtil &)> parse;
    };

    NativeReader(
        DB::ReadBuffer & istr_, Int64 max_block_size_ = DB::DEFAULT_BLOCK_SIZE, Int64 max_block_bytes_ = DB::DEFAULT_BLOCK_SIZE * 256)
        : istr(istr_)
        , max_block_size(max_block_size_ != 0 ? static_cast<size_t>(max_block_size_) : DB::DEFAULT_BLOCK_SIZE)
        , max_block_bytes(max_block_bytes_ != 0 ? static_cast<size_t>(max_block_bytes_) : DB::DEFAULT_BLOCK_SIZE * 256)
    {
    }

    DB::Block getHeader() const;

    DB::Block read();

private:
    DB::ReadBuffer & istr;
    /// Try to merge small blocks into a larger one. It's helpful for reducing memory allocations.
    size_t max_block_size;
    /// Avoid generating overly large blocks.
    size_t max_block_bytes;
    DB::Block header;

    std::vector<ColumnParseUtil> columns_parse_util;

    void updateAvgValueSizeHints(const DB::Block & block);

    DB::Block prepareByFirstBlock();
    bool appendNextBlock(DB::Block & result_block);
};

}
