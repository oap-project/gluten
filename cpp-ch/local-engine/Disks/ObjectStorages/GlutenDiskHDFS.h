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

#include <config.h>

#include <Disks/ObjectStorages/DiskObjectStorage.h>
#if USE_HDFS
#include <Disks/ObjectStorages/GlutenHDFSObjectStorage.h>
#endif

namespace local_engine
{
#if USE_HDFS
class GlutenDiskHDFS : public DB::DiskObjectStorage
{
public:
    GlutenDiskHDFS(
        const String & name_,
        const String & object_key_prefix_,
        DB::MetadataStoragePtr metadata_storage_,
        DB::ObjectStoragePtr object_storage_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix)
        : DiskObjectStorage(name_, object_key_prefix_, metadata_storage_, object_storage_, config, config_prefix)
    {
        chassert(dynamic_cast<local_engine::GlutenHDFSObjectStorage *>(object_storage_.get()) != nullptr);
        object_key_prefix = object_key_prefix_;
        hdfs_object_storage = dynamic_cast<local_engine::GlutenHDFSObjectStorage *>(object_storage_.get());
        hdfsSetWorkingDirectory(hdfs_object_storage->getHDFSFS(), "/");
    }

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void removeDirectory(const String & path) override;

    DB::DiskObjectStoragePtr createDiskObjectStorage() override;
private:
    String path2AbsPath(const String & path);

    GlutenHDFSObjectStorage * hdfs_object_storage;
    String object_key_prefix;
};
#endif
}

