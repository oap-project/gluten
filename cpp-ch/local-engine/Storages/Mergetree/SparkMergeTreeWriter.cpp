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
#include "SparkMergeTreeWriter.h"

#include <Disks/createVolume.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <rapidjson/prettywriter.h>

using namespace DB;

namespace local_engine
{

Block removeColumnSuffix(const DB::Block & block)
{
    ColumnsWithTypeAndName columns;
    for (int i = 0; i < block.columns(); ++i)
    {
        auto name = block.getByPosition(i).name;
        Poco::StringTokenizer splits(name, "#");
        auto column = block.getByPosition(i);
        column.name = splits[0];
        columns.emplace_back(column);
    }
    return Block(columns);
}

void SparkMergeTreeWriter::write(DB::Block & block)
{
    auto new_block = removeColumnSuffix(block);
    auto converter = ActionsDAG::makeConvertingActions(new_block.getColumnsWithTypeAndName(), header.getColumnsWithTypeAndName(), DB::ActionsDAG::MatchColumnsMode::Position);;
    if (converter)
    {
        ExpressionActions do_convert = ExpressionActions(converter);
        do_convert.execute(new_block);
    }

    auto blocks_with_partition = MergeTreeDataWriter::splitBlockIntoParts(squashing_transform->add(new_block), 10, metadata_snapshot, context);
        for (auto & item : blocks_with_partition)
        {
            new_parts.emplace_back(writeTempPartAndFinalize(item, metadata_snapshot).part);
        part_num++;
    }
}

void SparkMergeTreeWriter::finalize()
{
    auto block = squashing_transform->add({});
    if (block.rows())
    {
        auto blocks_with_partition = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), 10, metadata_snapshot, context);
        for (auto & item : blocks_with_partition)
            new_parts.emplace_back(writeTempPartAndFinalize(item, metadata_snapshot).part);
    }
}

DB::MergeTreeDataWriter::TemporaryPart
SparkMergeTreeWriter::writeTempPartAndFinalize(
    DB::BlockWithPartition & block_with_partition,
    const DB::StorageMetadataPtr & metadata_snapshot)
{
    auto temp_part = writeTempPart(block_with_partition, metadata_snapshot);
    temp_part.finalize();
    saveFileStatus(temp_part);
    return temp_part;
}

void SparkMergeTreeWriter::saveFileStatus(const DB::MergeTreeDataWriter::TemporaryPart & temp_part) const
{
    auto & data_part_storage = temp_part.part->getDataPartStorage();

    const DiskPtr disk = storage.getStoragePolicy()->getAnyDisk();
    if (!disk->isRemote()) return;
    if (auto *const disk_metadata = dynamic_cast<MetadataStorageFromDisk *>(disk->getMetadataStorage().get()))
    {
        const auto out = data_part_storage.writeFile("metadata.gluten", DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
        for (const auto it = data_part_storage.iterate(); it->isValid(); it->next())
        {
            auto content = disk_metadata->readFileToString(it->path());
            writeString(it->name(), *out);
            writeChar('\t', *out);
            writeIntText(content.length(), *out);
            writeChar('\n', *out);
            writeString(content, *out);
        }
        out->finalize();
    }
}

MergeTreeDataWriter::TemporaryPart SparkMergeTreeWriter::writeTempPart(
    BlockWithPartition & block_with_partition, const StorageMetadataPtr & metadata_snapshot)
{
    MergeTreeDataWriter::TemporaryPart temp_part;
    Block & block = block_with_partition.block;

    auto columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());

    for (auto & column : columns)
        if (column.type->hasDynamicSubcolumns())
            column.type = block.getByName(column.name).type;

    auto minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
    minmax_idx->update(block, storage.getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));

    MergeTreePartition partition(block_with_partition.partition);

    MergeTreePartInfo new_part_info(partition.getID(metadata_snapshot->getPartitionKey().sample_block), 1, 1, 0);

    std::string part_dir;
    if (!partition_dir.empty() && !bucket_dir.empty())
    {
        part_dir = fmt::format("{}/{}/{}_{:03d}", partition_dir, bucket_dir, uuid, part_num);
    }
    else if (!partition_dir.empty())
    {
        part_dir = fmt::format("{}/{}_{:03d}", partition_dir, uuid, part_num);
    }
    else if (!bucket_dir.empty())
    {
        part_dir = fmt::format("{}/{}_{:03d}", bucket_dir, uuid, part_num);
    }
    else
    {
        part_dir = fmt::format("{}_{:03d}", uuid, part_num);
    }

    String part_name = part_dir;

    temp_part.temporary_directory_lock = storage.getTemporaryPartDirectoryHolder(part_dir);

    auto indices = MergeTreeIndexFactory::instance().getMany(metadata_snapshot->getSecondaryIndices());

    /// If we need to calculate some columns to sort.
    if (metadata_snapshot->hasSortingKey() || metadata_snapshot->hasSecondaryIndices())
        storage.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, indices)->execute(block);

    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(sort_columns[i], 1, 1);

    /// Sort
    IColumn::Permutation * perm_ptr = nullptr;
    IColumn::Permutation perm;
    if (!sort_description.empty())
    {
        if (!isAlreadySorted(block, sort_description))
        {
            stableGetPermutation(block, sort_description, perm);
            perm_ptr = &perm;
        }
    }

    Names partition_key_columns = metadata_snapshot->getPartitionKey().column_names;

    /// Size of part would not be greater than block.bytes() + epsilon
    size_t expected_size = block.bytes();

    /// If optimize_on_insert is true, block may become empty after merge.
    /// There is no need to create empty part.
    if (expected_size == 0)
        return temp_part;

    VolumePtr volume = storage.getStoragePolicy()->getVolume(0);
    VolumePtr data_part_volume = std::make_shared<SingleDiskVolume>(volume->getName(), volume->getDisk(), volume->max_data_part_size);
    auto new_data_part = storage.getDataPartBuilder(part_name, data_part_volume, part_dir)
                             .withPartFormat(storage.choosePartFormat(expected_size, block.rows()))
                             .withPartInfo(new_part_info)
                             .build();

    auto data_part_storage = new_data_part->getDataPartStoragePtr();


    const auto & data_settings = storage.getSettings();

    SerializationInfo::Settings settings{data_settings->ratio_of_defaults_for_sparse_serialization, true};
    SerializationInfoByName infos(columns, settings);
    infos.add(block);

    new_data_part->setColumns(columns, infos, metadata_snapshot->getMetadataVersion());
    new_data_part->rows_count = block.rows();
    new_data_part->partition = std::move(partition);
    new_data_part->minmax_idx = std::move(minmax_idx);

    SyncGuardPtr sync_guard;
    if (new_data_part->isStoredOnDisk())
    {
        /// The name could be non-unique in case of stale files from previous runs.
        String full_path = new_data_part->getDataPartStorage().getFullPath();

        if (new_data_part->getDataPartStorage().exists())
        {
            // LOG_WARNING(log, "Removing old temporary directory {}", full_path);
            data_part_storage->removeRecursive();
        }

        data_part_storage->createDirectories();

        if (storage.getSettings()->fsync_part_directory)
        {
            const auto disk = data_part_volume->getDisk();
            sync_guard = disk->getDirectorySyncGuard(full_path);
        }
    }

    /// This effectively chooses minimal compression method:
    ///  either default lz4 or compression method with zero thresholds on absolute and relative part size.
    auto compression_codec = storage.getContext()->chooseCompressionCodec(0, 0);

    auto out = std::make_unique<MergedBlockOutputStream>(
        new_data_part,
        metadata_snapshot,
        columns,
        indices,
        MergeTreeStatisticsFactory::instance().getMany(metadata_snapshot->getColumns()),
        compression_codec,
        context->getCurrentTransaction(),
        false,
        false,
        context->getWriteSettings());

    out->writeWithPermutation(block, perm_ptr);


    auto finalizer = out->finalizePartAsync(new_data_part, data_settings->fsync_after_insert, nullptr, nullptr);

    temp_part.part = new_data_part;
    temp_part.streams.emplace_back(MergeTreeDataWriter::TemporaryPart::Stream{.stream = std::move(out), .finalizer = std::move(finalizer)});

    return temp_part;
}

std::vector<PartInfo> SparkMergeTreeWriter::getAllPartInfo()
{
    std::vector<PartInfo> res;
    for (const MergeTreeDataPartPtr & part : new_parts)
        res.emplace_back(PartInfo{part->name, part->getMarksCount(), part->getBytesOnDisk(), part->rows_count, partition_values, bucket_dir});
    return res;
}

String SparkMergeTreeWriter::partInfosToJson(const std::vector<PartInfo> & part_infos)
{
    rapidjson::StringBuffer result;
    rapidjson::Writer<rapidjson::StringBuffer> writer(result);
    writer.StartArray();
    for (const auto & item : part_infos)
    {
        writer.StartObject();
        writer.Key("part_name");
        writer.String(item.part_name.c_str());
        writer.Key("mark_count");
        writer.Uint(item.mark_count);
        writer.Key("disk_size");
        writer.Uint(item.disk_size);
        writer.Key("row_count");
        writer.Uint(item.row_count);
        writer.Key("bucket_id");
        writer.String(item.bucket_id.c_str());
        writer.Key("partition_values");
        writer.StartObject();
        for (const auto & key_value : item.partition_values)
        {
            writer.Key(key_value.first.c_str());
            writer.String(key_value.second.c_str());
        }
        writer.EndObject();
        writer.EndObject();
    }
    writer.EndArray();
    return result.GetString();
}

}