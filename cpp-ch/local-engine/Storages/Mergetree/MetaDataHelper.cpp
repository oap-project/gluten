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
#include "MetaDataHelper.h"
#include <filesystem>

#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>

using namespace DB;

namespace local_engine
{

std::unordered_map<String, String> extractPartMetaData(ReadBuffer & in)
{
    std::unordered_map<String, String> result;
    while (!in.eof())
    {
        String name;
        readString(name, in);
        assertChar('\t', in);
        UInt64 size;
        readIntText(size, in);
        assertChar('\n', in);
        String data;
        data.resize(size);
        in.read(data.data(), size);
        result.emplace(name, data);
    }
    return result;
}

void restoreMetaData(CustomStorageMergeTreePtr & storage, const MergeTreeTable & mergeTreeTable, ContextPtr & context)
{
    auto data_disk = storage->getStoragePolicy()->getAnyDisk();
    if (!data_disk->isRemote())
        return;

    std::unordered_set<String> not_exists_part;
    DB::MetadataStorageFromDisk * metadata_storage = static_cast<MetadataStorageFromDisk *>(data_disk->getMetadataStorage().get());
    auto metadata_disk = metadata_storage->getDisk();
    auto table_path = std::filesystem::path(mergeTreeTable.relative_path);
    for (const auto & part : mergeTreeTable.getPartNames())
    {
        auto part_path = table_path / part;
        if (!metadata_disk->exists(part_path))
            not_exists_part.emplace(part);
    }

    if (not_exists_part.empty())
        return;

    if (auto lock = storage->lockForAlter(context->getSettingsRef().lock_acquire_timeout))
    {
        auto s3 = data_disk->getObjectStorage();

        if (!metadata_disk->exists(table_path))
            metadata_disk->createDirectories(table_path.generic_string());

        for (const auto & part : not_exists_part)
        {
            auto part_path = table_path / part;
            auto metadata_file_path = part_path / "metadata.gluten";

            if (metadata_disk->exists(part_path))
                continue;
            else
                metadata_disk->createDirectories(part_path);
            auto key = s3->generateObjectKeyForPath(metadata_file_path.generic_string());
            StoredObject metadata_object(key.serialize());
            auto part_metadata = extractPartMetaData(*s3->readObject(metadata_object));
            for (const auto & item : part_metadata)
            {
                auto item_path = part_path / item.first;
                auto out = metadata_disk->writeFile(item_path);
                out->write(item.second.data(), item.second.size());
            }
        }
    }
}

}