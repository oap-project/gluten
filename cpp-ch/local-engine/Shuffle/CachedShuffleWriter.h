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
#include <algorithm>
#include <memory>
#include <jni.h>
#include <Shuffle/PartitionWriter.h>
#include <Shuffle/SelectorBuilder.h>
#include <Shuffle/ShuffleSplitter.h>
#include <Shuffle/ShuffleWriterBase.h>

namespace local_engine
{

class IPartitionWriter;
class CachedShuffleWriter : public ShuffleWriterBase
{
public:
    friend class IPartitionWriter;

    friend class IHashBasedPartitionWriter;
    friend class LocalHashBasedPartitionWriter;
    friend class CelebornHashBasedPartitionWriter;

    friend class ISortBasedPartitionWriter;
    friend class CelebornSortedBasedPartitionWriter;

    explicit CachedShuffleWriter(const String & short_name, const SplitOptions & options, jobject rss_pusher = nullptr);
    ~CachedShuffleWriter() override = default;

    void split(DB::Block & block) override;
    size_t evictPartitions() override;
    SplitResult stop() override;

    // bool useSortBasedShuffle() const { return options.partition_num >= 1000; }
    bool useSortBasedShuffle() const { return false; }

private:
    void initOutputIfNeeded(DB::Block & block);

    bool stopped = false;
    DB::Block output_header;
    SplitOptions options;
    SplitResult split_result;
    std::unique_ptr<SelectorBuilder> partitioner;
    std::vector<size_t> output_columns_indicies;
    std::unique_ptr<IPartitionWriter> partition_writer;
};
}



