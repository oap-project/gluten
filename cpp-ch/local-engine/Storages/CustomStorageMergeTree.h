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

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/StorageMergeTree.h>

namespace local_engine
{
using namespace DB;

class CustomMergeTreeSink;

class CustomStorageMergeTree final : public MergeTreeData
{
    friend class CustomMergeTreeSink;

    friend class MergeSparkMergeTreeTask;

public:
    static void wrapRangesInDataParts(DB::ReadFromMergeTree & source, DB::RangesInDataParts ranges);
    CustomStorageMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag = false);
    std::string getName() const override;
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & executor) override;
    std::map<std::string, MutationCommands> getUnfinishedMutationCommands() const override;
    DataPartsVector loadDataPartsWithNames(std::unordered_set<std::string> parts);


    MergeTreeDataWriter writer;
    MergeTreeDataSelectExecutor reader;
    MergeTreeDataMergerMutator merger_mutator;

    static std::atomic<int> part_num;

private:
    SimpleIncrement increment;

    void startBackgroundMovesIfNeeded() override;
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;
    LoadPartResult loadDataPart(
        const MergeTreePartInfo & part_info,
        const String & part_name,
        const DiskPtr & part_disk_ptr,
        MergeTreeDataPartState to_state);

protected:
    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr context) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context) override;
    PartitionCommandsResultInfo
    attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr context) override;
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) override;
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;
    MutationCommands getAlterMutationCommandsForPart(const DataPartPtr & /*part*/) const override { return {}; }
    void attachRestoredParts(MutableDataPartsVector && /*parts*/) override { throw std::runtime_error("not implement"); }
};

}
