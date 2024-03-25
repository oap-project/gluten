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
#include "config.h"
#include <Disks/DiskFactory.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>

#if USE_HDFS
#include <Disks/ObjectStorages/GlutenDiskHDFS.h>
#endif

#include "registerGlutenDisks.h"

namespace local_engine
{
#if USE_AWS_S3
void registerGlutenS3ObjectStorage(DB::ObjectStorageFactory & factory);
#endif

#if USE_HDFS
void registerGlutenHDFSObjectStorage(DB::ObjectStorageFactory & factory);
#endif

void registerGlutenDisks(bool global_skip_access_check)
{
    auto & factory = DB::DiskFactory::instance();
    auto creator = [global_skip_access_check](
                       const String & name,
                       const Poco::Util::AbstractConfiguration & config,
                       const String & config_prefix,
                       DB::ContextPtr context,
                       const DB::DisksMap & /* map */,
                       bool,
                       bool) -> DB::DiskPtr
    {
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        auto object_storage = DB::ObjectStorageFactory::instance().create(name, config, config_prefix, context, skip_access_check);
        auto metadata_storage = DB::MetadataStorageFactory::instance().create(name, config, config_prefix, object_storage, "local");

        DB::DiskObjectStoragePtr disk = std::make_shared<DB::DiskObjectStorage>(
            name,
            object_storage->getCommonKeyPrefix(),
            std::move(metadata_storage),
            std::move(object_storage),
            config,
            config_prefix);

        disk->startup(context, skip_access_check);
        return disk;
    };

    auto & object_factory = DB::ObjectStorageFactory::instance();
#if USE_AWS_S3
    registerGlutenS3ObjectStorage(object_factory);
    factory.registerDiskType("s3_gluten", creator); /// For compatibility
#endif

#if USE_HDFS
    auto hdfs_creator = [global_skip_access_check](
                            const String & name,
                            const Poco::Util::AbstractConfiguration & config,
                            const String & config_prefix,
                            DB::ContextPtr context,
                            const DB::DisksMap & /* map */,
                            bool,
                            bool) -> DB::DiskPtr
    {
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        auto object_storage = DB::ObjectStorageFactory::instance().create(name, config, config_prefix, context, skip_access_check);
        auto metadata_storage = DB::MetadataStorageFactory::instance().create(name, config, config_prefix, object_storage, "local");

        DB::DiskObjectStoragePtr disk = std::make_shared<local_engine::GlutenDiskHDFS>(
            name, object_storage->getCommonKeyPrefix(), std::move(metadata_storage), std::move(object_storage), config, config_prefix);

        disk->startup(context, skip_access_check);
        return disk;
    };

    registerGlutenHDFSObjectStorage(object_factory);
    factory.registerDiskType("hdfs_gluten", hdfs_creator); /// For compatibility
#endif
}
}
