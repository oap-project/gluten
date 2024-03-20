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
#include <vector>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Chunk.h>
#include <base/types.h>
#include <substrait/plan.pb.h>
#include <Common/PODArray.h>

namespace local_engine
{
struct PartitionInfo
{
    DB::IColumn::Selector partition_selector;
    std::vector<size_t> partition_start_points;
    size_t partition_num;

    static PartitionInfo fromSelector(DB::IColumn::Selector selector, size_t partition_num);
};

class SelectorBuilder
{
public:
    virtual ~SelectorBuilder() = default;
    virtual PartitionInfo build(DB::Block & block) = 0;
};

class RoundRobinSelectorBuilder : public SelectorBuilder
{
public:
    explicit RoundRobinSelectorBuilder(size_t parts_num_) : parts_num(parts_num_) { }
    ~RoundRobinSelectorBuilder() override = default;
    PartitionInfo build(DB::Block & block) override;

private:
    size_t parts_num;
    Int32 pid_selection = 0;
};

class HashSelectorBuilder : public SelectorBuilder
{
public:
    explicit HashSelectorBuilder(UInt32 parts_num_, const std::vector<size_t> & exprs_index_, const std::string & hash_function_name_);
    ~HashSelectorBuilder() override = default;
    PartitionInfo build(DB::Block & block) override;

private:
    UInt32 parts_num;
    std::vector<size_t> exprs_index;
    std::string hash_function_name;
    DB::FunctionBasePtr hash_function;
};

class RangeSelectorBuilder : public SelectorBuilder
{
public:
    explicit RangeSelectorBuilder(const std::string & options_, const size_t partition_num_);
    ~RangeSelectorBuilder() override = default;
    PartitionInfo build(DB::Block & block) override;

private:
    DB::SortDescription sort_descriptions;
    std::vector<size_t> sorting_key_columns;
    struct SortFieldTypeInfo
    {
        DB::DataTypePtr inner_type;
        bool is_nullable = false;
    };
    std::vector<SortFieldTypeInfo> sort_field_types;
    DB::Block range_bounds_block;

    // If the ordering keys have expressions, we caculate the expressions here.
    std::mutex actions_dag_mutex;
    std::unique_ptr<substrait::Plan> projection_plan_pb;
    std::atomic<bool> has_init_actions_dag;
    std::unique_ptr<DB::ExpressionActions> projection_expression_actions;
    size_t partition_num;

    void initSortInformation(Poco::JSON::Array::Ptr orderings);
    void initRangeBlock(Poco::JSON::Array::Ptr range_bounds);
    void initActionsDAG(const DB::Block & block);

    template <typename T>
    void safeInsertFloatValue(const Poco::Dynamic::Var & field_value, DB::MutableColumnPtr & col);

    void computePartitionIdByBinarySearch(DB::Block & block, DB::IColumn::Selector & selector);
    int compareRow(
        const DB::Columns & columns,
        const std::vector<size_t> & required_columns,
        size_t row,
        const DB::Columns & bound_columns,
        size_t bound_row);

    int binarySearchBound(
        const DB::Columns & bound_columns,
        Int64 l,
        Int64 r,
        const DB::Columns & columns,
        const std::vector<size_t> & used_cols,
        size_t row);
};

}
