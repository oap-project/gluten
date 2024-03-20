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
#include <mutex>
#include <stack>
#include <jni.h>
#include <Core/Defines.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Shuffle/SelectorBuilder.h>
#include <Shuffle/ShuffleSplitter.h>
#include <base/types.h>
#include <Common/BlockIterator.h>

namespace local_engine
{
class NativeSplitter : BlockIterator
{
public:
    struct Options
    {
        size_t buffer_size = DB::DEFAULT_BLOCK_SIZE;
        size_t partition_num;
        std::string exprs_buffer;
        std::string schema_buffer;
        std::string hash_algorithm;
    };

    struct Holder
    {
        std::unique_ptr<NativeSplitter> splitter = nullptr;
    };

    static jclass iterator_class;
    static jmethodID iterator_has_next;
    static jmethodID iterator_next;
    static std::unique_ptr<NativeSplitter> create(const std::string & short_name, Options options, jobject input);

    NativeSplitter(Options options, jobject input);
    virtual ~NativeSplitter();

    bool hasNext();
    DB::Block * next();
    int32_t nextPartitionId() const;

protected:
    virtual void computePartitionId(DB::Block &) = 0;

    Options options;
    PartitionInfo partition_info;
    std::vector<size_t> output_columns_indicies;
    DB::Block output_header;

private:
    void split(DB::Block & block);
    int64_t inputNext();
    bool inputHasNext();

    std::vector<std::shared_ptr<ColumnsBuffer>> partition_buffer;
    std::stack<std::pair<int32_t, std::unique_ptr<DB::Block>>> output_buffer;
    int32_t next_partition_id = -1;
    jobject input;
};

class HashNativeSplitter : public NativeSplitter
{
public:
    HashNativeSplitter(NativeSplitter::Options options_, jobject input);
    ~HashNativeSplitter() override = default;

private:
    void computePartitionId(DB::Block & block) override;

    std::unique_ptr<HashSelectorBuilder> selector_builder;
};

class RoundRobinNativeSplitter : public NativeSplitter
{
public:
    RoundRobinNativeSplitter(NativeSplitter::Options options_, jobject input);
    ~RoundRobinNativeSplitter() override = default;

private:
    void computePartitionId(DB::Block & block) override;

    std::unique_ptr<RoundRobinSelectorBuilder> selector_builder;
};

class RangePartitionNativeSplitter : public NativeSplitter
{
public:
    RangePartitionNativeSplitter(NativeSplitter::Options options_, jobject input);
    ~RangePartitionNativeSplitter() override = default;

private:
    void computePartitionId(DB::Block & block) override;

    std::unique_ptr<RangeSelectorBuilder> selector_builder;
};

}
