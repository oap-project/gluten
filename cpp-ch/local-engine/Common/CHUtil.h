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
#include <filesystem>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/CastOverloadResolver.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Storages/IStorage.h>
#include <base/types.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>

namespace local_engine
{
class BlockUtil
{
public:
    // Build a header block with a virtual column which will be
    // use to indicate the number of rows in a block.
    // Commonly seen in the following quries:
    // - select count(1) from t
    // - select 1 from t
    static DB::Block buildRowCountHeader();
    static DB::Chunk buildRowCountChunk(UInt64 rows);
    static DB::Block buildRowCountBlock(UInt64 rows);

    static DB::Block buildHeader(const DB::NamesAndTypesList & names_types_list);

    static constexpr UInt64 FLAT_STRUCT = 1;
    static constexpr UInt64 FLAT_NESTED_TABLE = 2;
    /// If it's a struct without named fields, also force to flatten it.
    static constexpr UInt64 FLAT_STRUCT_FORCE = 4;

    // flatten the struct and array(struct) columns.
    // It's different from Nested::flattend()
    static DB::Block flattenBlock(
        const DB::Block & block,
        UInt64 flags = FLAT_STRUCT | FLAT_NESTED_TABLE,
        bool recursively = false,
        const std::unordered_set<size_t> & columns_to_skip_flatten = {});

    static DB::Block concatenateBlocksMemoryEfficiently(std::vector<DB::Block> && blocks);
};

class PODArrayUtil
{
public:
    /// To allocate n bytes, PODArray will allocate n + pad_left + pad_right bytes in fact. So when
    /// we want to allocate 2^k bytes, 2^(k+1) bytes are allocated. This makes the memory usage far
    /// more than we expected, and easy to cause OOM. For example, we want to limit the max block size to be
    /// 64k rows, CH will make the memory usage equal to 128k rows, and half of the reserved memory is not used.
    /// So we adjust the size by considering the padding bytes, the return value may be samller then n.
    static size_t adjustMemoryEfficientSize(size_t n);
};

/// Use this class to extract element columns from columns of nested type in a block, e.g. named Tuple.
/// It can extract a column from a multiple nested type column, e.g. named Tuple in named Tuple
/// Keeps some intermediate data to avoid rebuild them multi-times.
class NestedColumnExtractHelper
{
public:
    explicit NestedColumnExtractHelper(const DB::Block & block_, bool case_insentive_);
    std::optional<DB::ColumnWithTypeAndName> extractColumn(const String & column_name);

private:
    std::optional<DB::ColumnWithTypeAndName>
    extractColumn(const String & original_column_name, const String & column_name_prefix, const String & column_name_suffix);
    const DB::Block & block;
    bool case_insentive;
    std::map<String, DB::BlockPtr> nested_tables;

    const DB::ColumnWithTypeAndName * findColumn(const DB::Block & block, const std::string & name) const;
};

class PlanUtil
{
public:
    static std::string explainPlan(DB::QueryPlan & plan);
};

class MergeTreeUtil
{
public:
    using Path = std::filesystem::path;
    static std::vector<Path> getAllMergeTreeParts(const Path & storage_path);
    static DB::NamesAndTypesList getSchemaFromMergeTreePart(const Path & part_path);
};

class ActionsDAGUtil
{
public:
    static const DB::ActionsDAG::Node * convertNodeType(
        DB::ActionsDAGPtr & actions_dag,
        const DB::ActionsDAG::Node * node,
        const std::string & type_name,
        const std::string & result_name = "",
        DB::CastType cast_type = DB::CastType::nonAccurate);
};

class QueryPipelineUtil
{
public:
    static String explainPipeline(DB::QueryPipeline & pipeline);
};

void registerAllFunctions();
void registerGlutenDisks();

class BackendFinalizerUtil;
class JNIUtils;
class BackendInitializerUtil
{
public:
    /// Initialize two kinds of resources
    /// 1. global level resources like global_context/shared_context, notice that they can only be initialized once in process lifetime
    /// 2. session level resources like settings/configs, they can be initialized multiple times following the lifetime of executor/driver
    static void init(std::string * plan);
    static void init_json(std::string * plan_json);
    static void updateConfig(const DB::ContextMutablePtr &, std::string *);


    // use excel text parser
    inline static const std::string USE_EXCEL_PARSER = "use_excel_serialization";
    inline static const std::string EXCEL_EMPTY_AS_NULL = "use_excel_serialization.empty_as_null";
    inline static const std::string EXCEL_NUMBER_FORCE = "use_excel_serialization.number_force";
    inline static const std::string EXCEL_QUOTE_STRICT = "use_excel_serialization.quote_strict";
    inline static const String CH_BACKEND_PREFIX = "spark.gluten.sql.columnar.backend.ch";

    inline static const String CH_RUNTIME_CONFIG = "runtime_config";
    inline static const String CH_RUNTIME_CONFIG_PREFIX = CH_BACKEND_PREFIX + "." + CH_RUNTIME_CONFIG + ".";
    inline static const String CH_RUNTIME_CONFIG_FILE = CH_RUNTIME_CONFIG_PREFIX + "config_file";

    inline static const String CH_RUNTIME_SETTINGS = "runtime_settings";
    inline static const String CH_RUNTIME_SETTINGS_PREFIX = CH_BACKEND_PREFIX + "." + CH_RUNTIME_SETTINGS + ".";

    inline static const String SETTINGS_PATH = "local_engine.settings";
    inline static const String LIBHDFS3_CONF_KEY = "hdfs.libhdfs3_conf";
    inline static const std::string HADOOP_S3_ACCESS_KEY = "fs.s3a.access.key";
    inline static const std::string HADOOP_S3_SECRET_KEY = "fs.s3a.secret.key";
    inline static const std::string HADOOP_S3_ENDPOINT = "fs.s3a.endpoint";
    inline static const std::string HADOOP_S3_ASSUMED_ROLE = "fs.s3a.assumed.role.arn";
    inline static const std::string HADOOP_S3_ASSUMED_SESSION_NAME = "fs.s3a.assumed.role.session.name";
    // not hadoop official
    inline static const std::string HADOOP_S3_ASSUMED_EXTERNAL_ID = "fs.s3a.assumed.role.externalId";
    // hadoop official, this is used to ignore the cached client
    inline static const std::string HADOOP_S3_CLIENT_CACHE_IGNORE = "fs.s3a.client.cached.ignore";
    inline static const std::string SPARK_HADOOP_PREFIX = "spark.hadoop.";
    inline static const std::string S3A_PREFIX = "fs.s3a.";

    /// On yarn mode, native writing on hdfs cluster takes yarn container user as the user passed to libhdfs3, which
    /// will cause permission issue because yarn container user is not the owner of the hdfs dir to be written.
    /// So we need to get the spark user from env and pass it to libhdfs3.
    inline static std::optional<String> spark_user;

private:
    friend class BackendFinalizerUtil;
    friend class JNIUtils;

    static DB::Context::ConfigurationPtr initConfig(std::map<std::string, std::string> & backend_conf_map);
    static void initLoggers(DB::Context::ConfigurationPtr config);
    static void initEnvs(DB::Context::ConfigurationPtr config);
    static void initSettings(std::map<std::string, std::string> & backend_conf_map, DB::Settings & settings);

    static void initContexts(DB::Context::ConfigurationPtr config);
    static void initCompiledExpressionCache(DB::Context::ConfigurationPtr config);
    static void registerAllFactories();
    static void applyGlobalConfigAndSettings(DB::Context::ConfigurationPtr, DB::Settings &);
    static void updateNewSettings(const DB::ContextMutablePtr &, const DB::Settings &);


    static std::map<std::string, std::string> getBackendConfMap(std::string * plan);

    inline static std::once_flag init_flag;
    inline static Poco::Logger * logger;
};

class BackendFinalizerUtil
{
public:
    /// Release global level resources like global_context/shared_context. Invoked only once in the lifetime of process when JVM is shuting down.
    static void finalizeGlobally();

    /// Release session level resources like StorageJoinBuilder. Invoked every time executor/driver shutdown.
    static void finalizeSessionally();
};

// Ignore memory track, memory should free before IgnoreMemoryTracker deconstruction
class IgnoreMemoryTracker
{
public:
    explicit IgnoreMemoryTracker(size_t limit_) : limit(limit_) { DB::CurrentThread::get().untracked_memory_limit += limit; }
    ~IgnoreMemoryTracker() { DB::CurrentThread::get().untracked_memory_limit -= limit; }

private:
    size_t limit;
};

class DateTimeUtil
{
public:
    static Int64 currentTimeMillis();
};

class MemoryUtil
{
public:
    static UInt64 getCurrentMemoryUsage(size_t depth = 1);
    static UInt64 getMemoryRSS();
};

}
