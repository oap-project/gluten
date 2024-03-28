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
#include <Storages/CustomStorageMergeTree.h>
#include <Interpreters/MergeTreeTransaction.h>

namespace local_engine
{
using CustomStorageMergeTreePtr = std::shared_ptr<CustomStorageMergeTree>;
using StorageInMemoryMetadataPtr = std::shared_ptr<DB::StorageInMemoryMetadata>;

class StorageMergeTreeFactory
{
public:
    static StorageMergeTreeFactory & instance();
    static void freeStorage(StorageID id);
    static CustomStorageMergeTreePtr
    getStorage(StorageID id, ColumnsDescription columns, std::function<CustomStorageMergeTreePtr()> creator);
    static StorageInMemoryMetadataPtr getMetadata(StorageID id, std::function<StorageInMemoryMetadataPtr()> creator);
    static DataPartsVector getDataParts(StorageID id, std::unordered_set<String> part_name);
    static void addDataPartToCache(StorageID id, String part_name, DataPartPtr part);
    static void clear()
    {
        storage_columns_map.clear();
        storage_map.clear();
        datapart_map.clear();
        metadata_map.clear();
    }

private:
    static std::unordered_map<std::string, CustomStorageMergeTreePtr> storage_map;
    static std::unordered_map<std::string, std::set<std::string>> storage_columns_map;
    static std::unordered_map<std::string, std::unordered_map<std::string, DataPartPtr>> datapart_map;
    static std::mutex storage_map_mutex;
    static std::mutex datapart_mutex;

    static std::unordered_map<std::string, StorageInMemoryMetadataPtr> metadata_map;
    static std::mutex metadata_map_mutex;
};

struct TempStorageFreer
{
    StorageID id;
    ~TempStorageFreer()
    {
        StorageMergeTreeFactory::instance().freeStorage(id);
    }
};
}
