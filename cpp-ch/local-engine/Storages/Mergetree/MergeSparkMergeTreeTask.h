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

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeMutateSelectedEntry.h>
#include <Interpreters/MergeTreeTransactionHolder.h>

using namespace DB;


namespace local_engine
{
class CustomStorageMergeTree;


class MergeSparkMergeTreeTask : public IExecutableTask
{
public:
    MergeSparkMergeTreeTask(
        CustomStorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        bool deduplicate_,
        Names deduplicate_by_columns_,
        bool cleanup_,
        MergeMutateSelectedEntryPtr merge_mutate_entry_,
        TableLockHolder table_lock_holder_,
        IExecutableTask::TaskResultCallback & task_result_callback_)
        : storage(storage_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , deduplicate(deduplicate_)
        , deduplicate_by_columns(std::move(deduplicate_by_columns_))
        , cleanup(cleanup_)
        , merge_mutate_entry(std::move(merge_mutate_entry_))
        , table_lock_holder(std::move(table_lock_holder_))
        , task_result_callback(task_result_callback_)
    {
        for (auto & item : merge_mutate_entry->future_part->parts)
            priority.value += item->getBytesOnDisk();
    }

    bool executeStep() override;
    void onCompleted() override;
    StorageID getStorageID() const override;
    Priority getPriority() const override { return priority; }
    String getQueryId() const override { return getStorageID().getShortName() + "::" + merge_mutate_entry->future_part->name; }

    void setCurrentTransaction(MergeTreeTransactionHolder && txn_holder_, MergeTreeTransactionPtr && txn_)
    {
        txn_holder = std::move(txn_holder_);
        txn = std::move(txn_);
    }

private:
    void prepare();
    void finish();

    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINISH,

        SUCCESS
    };

    State state{State::NEED_PREPARE};

    CustomStorageMergeTree & storage;

    StorageMetadataPtr metadata_snapshot;
    bool deduplicate;
    Names deduplicate_by_columns;
    bool cleanup;
    MergeMutateSelectedEntryPtr merge_mutate_entry{nullptr};
    TableLockHolder table_lock_holder;
    FutureMergedMutatedPartPtr future_part{nullptr};
    MergeTreeData::MutableDataPartPtr new_part;
    std::unique_ptr<Stopwatch> stopwatch_ptr{nullptr};
    using MergeListEntryPtr = std::unique_ptr<MergeListEntry>;
    MergeListEntryPtr merge_list_entry;

    Priority priority;

    std::function<void(const ExecutionStatus &)> write_part_log;
    std::function<void()> transfer_profile_counters_to_initial_query;
    IExecutableTask::TaskResultCallback task_result_callback;
    MergeTaskPtr merge_task{nullptr};

    MergeTreeTransactionHolder txn_holder;
    MergeTreeTransactionPtr txn;

    ProfileEvents::Counters profile_counters;

    ContextMutablePtr task_context;

    ContextMutablePtr createTaskContext() const;
};


using MergeSparkMergeTreeTaskPtr = std::shared_ptr<MergeSparkMergeTreeTask>;


[[ maybe_unused ]] static void executeHere(MergeSparkMergeTreeTaskPtr task)
{
    while (task->executeStep()) {}
}


}
