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
#include "StorageMergeTreeFactory.h"

namespace local_engine
{
StorageMergeTreeFactory & StorageMergeTreeFactory::instance()
{
    static StorageMergeTreeFactory ret;
    return ret;
}

void StorageMergeTreeFactory::freeStorage(StorageID id)
{
    auto table_name = id.database_name + "." + id.table_name + "@" + toString(id.uuid);
    std::lock_guard lock(storage_map_mutex);
    if (storage_map.contains(table_name))
    {
        storage_map.erase(table_name);
    }
}

CustomStorageMergeTreePtr
StorageMergeTreeFactory::getStorage(StorageID id, ColumnsDescription columns, std::function<CustomStorageMergeTreePtr()> creator)
{
    auto table_name = id.database_name + "." + id.table_name + "@" + toString(id.uuid);
    std::lock_guard lock(storage_map_mutex);
    if (!storage_map.contains(table_name))
    {
        if (storage_map.contains(table_name))
        {
            std::set<std::string> existed_columns = storage_columns_map.at(table_name);
            for (const auto & column : columns)
            {
                if (!existed_columns.contains(column.name))
                {
                    storage_map.erase(table_name);
                    storage_columns_map.erase(table_name);
                }
            }
        }
        if (!storage_map.contains(table_name))
        {
            storage_map.emplace(table_name, creator());
            storage_columns_map.emplace(table_name, std::set<std::string>());
            for (const auto & column : storage_map.at(table_name)->getInMemoryMetadataPtr()->columns)
            {
                storage_columns_map.at(table_name).emplace(column.name);
            }
        }
    }
    return storage_map.at(table_name);
}

StorageInMemoryMetadataPtr StorageMergeTreeFactory::getMetadata(StorageID id, std::function<StorageInMemoryMetadataPtr()> creator)
{
    auto table_name = id.database_name + "." + id.table_name + "@" + toString(id.uuid);

    std::lock_guard lock(metadata_map_mutex);
    if (!metadata_map.contains(table_name))
    {
        if (!metadata_map.contains(table_name))
            metadata_map.emplace(table_name, creator());
    }
    return metadata_map.at(table_name);
}
DataPartsVector StorageMergeTreeFactory::getDataParts(StorageID id, std::unordered_set<String> part_name)
{
    DataPartsVector res;
    auto table_name = id.database_name + "." + id.table_name + "@" + toString(id.uuid);
    std::lock_guard lock(datapart_mutex);
    CustomStorageMergeTreePtr storage_merge_tree;
    {
        std::lock_guard storage_lock(storage_map_mutex);
        storage_merge_tree = storage_map.at(table_name);
    }
    std::unordered_set<String> missing_names;

    if (!datapart_map.contains(table_name)) [[unlikely]]
    {
        datapart_map.emplace(table_name, std::unordered_map<String, DataPartPtr>());
    }

    for (const auto & name : part_name)
    {
        if (!datapart_map[table_name].contains(name))
        {
            missing_names.emplace(name);
        }
        else
        {
            res.emplace_back(datapart_map[table_name].at(name));
        }
    }
    auto missing_parts = storage_merge_tree->loadDataPartsWithNames(missing_names);
    for (const auto & part : missing_parts)
    {
        res.emplace_back(part);
        datapart_map[table_name].emplace(part->name, part);
    }
    return res;
}
void StorageMergeTreeFactory::addDataPartToCache(StorageID id, String part_name, DataPartPtr part)
{
    auto table_name = id.database_name + "." + id.table_name + "@" + toString(id.uuid);
    std::lock_guard lock(datapart_mutex);
    if (!datapart_map.contains(table_name))
    {
        std::unordered_map<String, DataPartPtr> item;
        item.emplace(part_name, part);
        datapart_map.emplace(table_name, item);
    }
    else
    {
        datapart_map[table_name].emplace(part_name, part);
    }
}


std::unordered_map<std::string, CustomStorageMergeTreePtr> StorageMergeTreeFactory::storage_map;
std::unordered_map<std::string, std::set<std::string>> StorageMergeTreeFactory::storage_columns_map;
std::unordered_map<std::string, std::unordered_map<std::string, DataPartPtr>> StorageMergeTreeFactory::datapart_map;
std::mutex StorageMergeTreeFactory::storage_map_mutex;
std::mutex StorageMergeTreeFactory::datapart_mutex;

std::unordered_map<std::string, StorageInMemoryMetadataPtr> StorageMergeTreeFactory::metadata_map;
std::mutex StorageMergeTreeFactory::metadata_map_mutex;

}
