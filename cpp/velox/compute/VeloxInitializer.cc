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
#include <filesystem>

#include "VeloxInitializer.h"

#include <folly/executors/IOThreadPoolExecutor.h>

#include "config/GlutenConfig.h"
#include "operators/functions/RegistrationAllFunctions.h"
#include "operators/plannodes/RowVectorStream.h"
#include "utils/exception.h"
#include "velox/common/file/FileSystems.h"
#include "velox/serializers/PrestoSerializer.h"
#ifdef ENABLE_HDFS
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#endif
#ifdef ENABLE_S3
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#endif
#include "jni/JniFileSystem.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"

DECLARE_int32(split_preload_per_driver);
DECLARE_bool(SkipRowSortInWindowOp);

using namespace facebook;

namespace {

const std::string kHiveConnectorId = "test-hive";
const std::string kVeloxCacheEnabled = "spark.gluten.sql.columnar.backend.velox.cacheEnabled";

// memory cache
const std::string kVeloxMemCacheSize = "spark.gluten.sql.columnar.backend.velox.memCacheSize";
const std::string kVeloxMemCacheSizeDefault = "1073741824";

// ssd cache
const std::string kVeloxSsdCacheSize = "spark.gluten.sql.columnar.backend.velox.ssdCacheSize";
const std::string kVeloxSsdCacheSizeDefault = "1073741824";
const std::string kVeloxSsdCachePath = "spark.gluten.sql.columnar.backend.velox.ssdCachePath";
const std::string kVeloxSsdCachePathDefault = "/tmp/";
const std::string kVeloxSsdCacheShards = "spark.gluten.sql.columnar.backend.velox.ssdCacheShards";
const std::string kVeloxSsdCacheShardsDefault = "1";
const std::string kVeloxSsdCacheIOThreads = "spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads";
const std::string kVeloxSsdCacheIOThreadsDefault = "1";
const std::string kVeloxSsdODirectEnabled = "spark.gluten.sql.columnar.backend.velox.ssdODirect";

const std::string kVeloxIOThreads = "spark.gluten.sql.columnar.backend.velox.IOThreads";
const std::string kVeloxIOThreadsDefault = "0";

const std::string kVeloxSplitPreloadPerDriver = "spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver";
const std::string kVeloxSplitPreloadPerDriverDefault = "2";

// spill, mem ratios and thresholds
const std::string kSpillStrategy = "spark.gluten.sql.columnar.backend.velox.spillStrategy";
const std::string kMemoryCapRatio = "spark.gluten.sql.columnar.backend.velox.memoryCapRatio";
const std::string kSpillThresholdRatio = "spark.gluten.sql.columnar.backend.velox.spillMemoryThresholdRatio";

} // namespace

namespace gluten {

void VeloxInitializer::printConf(const std::unordered_map<std::string, std::string>& conf) {
  std::ostringstream oss;
  oss << "STARTUP: VeloxInitializer conf = {\n";
  for (auto& [k, v] : conf) {
    oss << " {" << k << ", " << v << "}\n";
  }
  oss << "}\n";
  oss << "memPoolOptions = {";
  oss << " alignment:" << memPoolOptions_.alignment;
  oss << ", capacity:" << (memPoolOptions_.capacity >> 20) << "M";
  oss << ", trackUsage:" << (int)memPoolOptions_.trackUsage;
  oss << " }\n";
  oss << "spillThreshold = " << (spillThreshold_ >> 20) << "M";
  LOG(INFO) << oss.str();
}

void VeloxInitializer::init(const std::unordered_map<std::string, std::string>& conf) {
  // In spark, planner takes care the parititioning and sorting, so the rows are sorted.
  // There is no need to sort the rows in window op again.
  FLAGS_SkipRowSortInWindowOp = true;
  // Setup and register.
  velox::filesystems::registerLocalFileSystem();
  gluten::registerJniFileSystem(); // JNI filesystem, for spilling-to-heap if we have extra JVM heap spaces

  // spill mode
  std::string spillStrategy;
  {
    auto got = conf.find(kSpillStrategy);
    if (got == conf.end()) {
      // not found
      spillStrategy = "threshold";
    } else {
      spillStrategy = got->second;
    }
  }

  // mem cap ratio
  float_t memCapRatio = 0.75;
  {
    auto got = conf.find(kMemoryCapRatio);
    if (got != conf.end()) {
      memCapRatio = std::stof(got->second);
    }
  }

  // mem tracker
  int64_t maxMemory = facebook::velox::memory::kMaxMemory;
  if (spillStrategy == "threshold") {
    auto got = conf.find(kSparkOffHeapMemory); // per executor, shared by tasks for creating iterator
    if (got != conf.end()) {
      maxMemory = (long)(memCapRatio * (double)std::stol(got->second));
    }
  }

  memPoolOptions_ = {.alignment = facebook::velox::memory::MemoryAllocator::kMaxAlignment, .capacity = maxMemory};

  // spill threshold ratio (out of the memory cap)
  float_t spillThresholdRatio = 0.6;
  {
    auto got = conf.find(kSpillThresholdRatio);
    if (got != conf.end()) {
      spillThresholdRatio = std::stof(got->second);
    }
  }

  spillThreshold_ = (int64_t)(spillThresholdRatio * (float_t)maxMemory);

#ifdef ENABLE_HDFS
  velox::filesystems::registerHdfsFileSystem();
#endif

  std::unordered_map<std::string, std::string> configurationValues;
#ifdef ENABLE_S3
  velox::filesystems::registerS3FileSystem();

  std::string awsAccessKey = conf.at("spark.hadoop.fs.s3a.access.key");
  std::string awsSecretKey = conf.at("spark.hadoop.fs.s3a.secret.key");
  std::string awsEndpoint = conf.at("spark.hadoop.fs.s3a.endpoint");
  std::string sslEnabled = conf.at("spark.hadoop.fs.s3a.connection.ssl.enabled");
  std::string pathStyleAccess = conf.at("spark.hadoop.fs.s3a.path.style.access");
  std::string useInstanceCredentials = conf.at("spark.hadoop.fs.s3a.use.instance.credentials");
  std::string iamRole = conf.at("spark.hadoop.fs.s3a.iam.role");
  std::string iamRoleSessionName = conf.at("spark.hadoop.fs.s3a.iam.role.session.name");

  const char* envAwsAccessKey = std::getenv("AWS_ACCESS_KEY_ID");
  if (envAwsAccessKey != nullptr) {
    awsAccessKey = std::string(envAwsAccessKey);
  }
  const char* envAwsSecretKey = std::getenv("AWS_SECRET_ACCESS_KEY");
  if (envAwsSecretKey != nullptr) {
    awsSecretKey = std::string(envAwsSecretKey);
  }
  const char* envAwsEndpoint = std::getenv("AWS_ENDPOINT");
  if (envAwsEndpoint != nullptr) {
    awsEndpoint = std::string(envAwsEndpoint);
  }

  std::unordered_map<std::string, std::string> s3Config({});
  if (useInstanceCredentials == "true") {
    s3Config.insert({
        {"hive.s3.use-instance-credentials", useInstanceCredentials},
    });
  } else if (!iamRole.empty()) {
    s3Config.insert({
        {"hive.s3.iam-role", iamRole},
    });
    if (!iamRoleSessionName.empty()) {
      s3Config.insert({
          {"hive.s3.iam-role-session-name", iamRoleSessionName},
      });
    }
  } else {
    s3Config.insert({
        {"hive.s3.aws-access-key", awsAccessKey},
        {"hive.s3.aws-secret-key", awsSecretKey},
    });
  }

  s3Config.insert({
      {"hive.s3.endpoint", awsEndpoint},
      {"hive.s3.ssl.enabled", sslEnabled},
      {"hive.s3.path-style-access", pathStyleAccess},
  });

  configurationValues.merge(s3Config);
#endif

  initCache(conf);
  initIOExecutor(conf);

#ifdef GLUTEN_PRINT_DEBUG
  printConf(conf);
#endif

  auto properties = std::make_shared<const velox::core::MemConfig>(configurationValues);
  auto hiveConnector =
      velox::connector::getConnectorFactory(velox::connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, properties, ioExecutor_.get());

  registerConnector(hiveConnector);
  velox::parquet::registerParquetReaderFactory(velox::parquet::ParquetReaderType::NATIVE);
  velox::dwrf::registerDwrfReaderFactory();
  velox::dwrf::registerOrcReaderFactory();
  // Register Velox functions
  registerAllFunctions();
  if (!facebook::velox::isRegisteredVectorSerde()) {
    // serde, for spill
    facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  velox::exec::Operator::registerOperator(std::make_unique<RowVectorStreamOperatorTranslator>());
}

velox::memory::MemoryAllocator* VeloxInitializer::getAsyncDataCache() const {
  return asyncDataCache_.get();
}

void VeloxInitializer::initCache(const std::unordered_map<std::string, std::string>& conf) {
  auto key = conf.find(kVeloxCacheEnabled);
  if (key != conf.end() && boost::algorithm::to_lower_copy(conf.at(kVeloxCacheEnabled)) == "true") {
    FLAGS_ssd_odirect = true;
    if (conf.find(kVeloxSsdODirectEnabled) != conf.end() &&
        boost::algorithm::to_lower_copy(conf.at(kVeloxSsdODirectEnabled)) == "false") {
      FLAGS_ssd_odirect = false;
    }
    uint64_t memCacheSize = std::stol(kVeloxMemCacheSizeDefault);
    uint64_t ssdCacheSize = std::stol(kVeloxSsdCacheSizeDefault);
    int32_t ssdCacheShards = std::stoi(kVeloxSsdCacheShardsDefault);
    int32_t ssdCacheIOThreads = std::stoi(kVeloxSsdCacheIOThreadsDefault);
    std::string ssdCachePathPrefix = kVeloxSsdCachePathDefault;
    for (auto& [k, v] : conf) {
      if (k == kVeloxMemCacheSize)
        memCacheSize = std::stol(v);
      if (k == kVeloxSsdCacheSize)
        ssdCacheSize = std::stol(v);
      if (k == kVeloxSsdCacheShards)
        ssdCacheShards = std::stoi(v);
      if (k == kVeloxSsdCachePath)
        ssdCachePathPrefix = v;
      if (k == kVeloxSsdCacheIOThreads)
        ssdCacheIOThreads = std::stoi(v);
    }
    cachePathPrefix_ = ssdCachePathPrefix;
    cacheFilePrefix_ = getCacheFilePrefix();
    std::string ssdCachePath = ssdCachePathPrefix + "/" + cacheFilePrefix_;
    ssdCacheExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ssdCacheIOThreads);
    auto ssd =
        std::make_unique<velox::cache::SsdCache>(ssdCachePath, ssdCacheSize, ssdCacheShards, ssdCacheExecutor_.get());

    std::error_code ec;
    const std::filesystem::space_info si = std::filesystem::space(ssdCachePathPrefix, ec);
    if (si.available < ssdCacheSize) {
      VELOX_FAIL(
          "not enough space for ssd cache in " + ssdCachePath + " cache size: " + std::to_string(ssdCacheSize) +
          "free space: " + std::to_string(si.available))
    }

    velox::memory::MmapAllocator::Options options;
    options.capacity = memCacheSize;
    auto allocator = std::make_shared<velox::memory::MmapAllocator>(options);
    if (ssdCacheSize == 0) {
      LOG(INFO) << "AsyncDataCache will do memory caching only as ssd cache size is 0";
      asyncDataCache_ = std::make_shared<velox::cache::AsyncDataCache>(allocator, memCacheSize, nullptr);
    } else {
      asyncDataCache_ = std::make_shared<velox::cache::AsyncDataCache>(allocator, memCacheSize, std::move(ssd));
    }

    VELOX_CHECK_NOT_NULL(dynamic_cast<velox::cache::AsyncDataCache*>(asyncDataCache_.get()))
    LOG(INFO) << "STARTUP: Using AsyncDataCache memory cache size: " << memCacheSize
              << ", ssdCache prefix: " << ssdCachePath << ", ssdCache size: " << ssdCacheSize
              << ", ssdCache shards: " << ssdCacheShards << ", ssdCache IO threads: " << ssdCacheIOThreads;
  }
}

void VeloxInitializer::initIOExecutor(const std::unordered_map<std::string, std::string>& conf) {
  int32_t ioThreads = std::stoi(kVeloxIOThreadsDefault);
  auto got = conf.find(kVeloxIOThreads);
  if (got != conf.end()) {
    ioThreads = std::stoi(got->second);
  }
  int32_t splitPreloadPerDriver = std::stoi(kVeloxSplitPreloadPerDriverDefault);
  got = conf.find(kVeloxSplitPreloadPerDriver);
  if (got != conf.end()) {
    splitPreloadPerDriver = std::stoi(got->second);
  }
  if (ioThreads > 0) {
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ioThreads);
    FLAGS_split_preload_per_driver = splitPreloadPerDriver;
  }

  if (splitPreloadPerDriver > 0 && ioThreads > 0) {
    LOG(INFO) << "STARTUP: Using split preloading, Split preload per driver: " << splitPreloadPerDriver
              << ", IO threads: " << ioThreads;
  }
}

void VeloxInitializer::create(const std::unordered_map<std::string, std::string>& conf) {
  std::lock_guard<std::mutex> lockGuard(mutex_);
  if (instance_ != nullptr) {
    assert(false);
    throw gluten::GlutenException("VeloxInitializer already set");
  }
  instance_.reset(new gluten::VeloxInitializer(conf));
}

std::shared_ptr<VeloxInitializer> VeloxInitializer::get() {
  std::lock_guard<std::mutex> lockGuard(mutex_);
  if (instance_ == nullptr) {
    LOG(INFO)
        << "VeloxInitializer not set, using default VeloxInitializer instance. This should only happen in test code.";
    static const std::unordered_map<std::string, std::string> kEmptyConf;
    static std::shared_ptr<VeloxInitializer> defaultInstance{new gluten::VeloxInitializer(kEmptyConf)};
    return defaultInstance;
  }
  return instance_;
}

} // namespace gluten
