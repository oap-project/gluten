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
#include <Interpreters/Context.h>
#include <Interpreters/SquashingTransform.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Poco/StringTokenizer.h>

namespace DB
{
struct BlockWithPartition;
class MergeTreeData;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
}

namespace local_engine
{

struct PartInfo
{
    String part_name;
    size_t mark_count;
    size_t disk_size;
    size_t row_count;
    std::unordered_map<String, String> partition_values;
    String bucket_id;
};

class SparkMergeTreeWriter
{
public:
    static String partInfosToJson(const std::vector<PartInfo> & part_infos);
    SparkMergeTreeWriter(
        DB::MergeTreeData & storage_,
        const DB::StorageMetadataPtr & metadata_snapshot_,
        const DB::ContextPtr & context_,
        const String & uuid_,
        const String & partition_dir_ = "",
        const String & bucket_dir_ = "")
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_)
        , uuid(uuid_)
        , partition_dir(partition_dir_)
        , bucket_dir(bucket_dir_)
    {
        const DB::Settings & settings = context->getSettingsRef();
        squashing_transform
            = std::make_unique<DB::SquashingTransform>(settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);
        if (!partition_dir.empty())
        {
            Poco::StringTokenizer partitions(partition_dir, "/");
            for (const auto & partition : partitions)
            {
                Poco::StringTokenizer key_value(partition, "=");
                chassert(key_value.count() == 2);
                partition_values.emplace(key_value[0], key_value[1]);
            }
        }
        header = metadata_snapshot->getSampleBlock();
    }

    void write(DB::Block & block);
    void finalize();
    std::vector<PartInfo> getAllPartInfo();

private:
    DB::MergeTreeDataWriter::TemporaryPart
    writeTempPart(DB::BlockWithPartition & block_with_partition, const DB::StorageMetadataPtr & metadata_snapshot);
    DB::MergeTreeDataWriter::TemporaryPart
    writeTempPartAndFinalize(DB::BlockWithPartition & block_with_partition, const DB::StorageMetadataPtr & metadata_snapshot);
    void saveFileStatus(const DB::MergeTreeDataWriter::TemporaryPart & temp_part) const;

    String uuid;
    String partition_dir;
    String bucket_dir;
    DB::MergeTreeData & storage;
    DB::StorageMetadataPtr metadata_snapshot;
    DB::ContextPtr context;
    std::unique_ptr<DB::SquashingTransform> squashing_transform;
    int part_num = 1;
    std::vector<DB::MergeTreeDataPartPtr> new_parts;
    std::unordered_map<String, String> partition_values;
    DB::Block header;
};

}
