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

#include "VeloxRuntime.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>

#include "VeloxBackend.h"
#include "compute/ResultIterator.h"
#include "compute/Runtime.h"
#include "compute/VeloxPlanConverter.h"
#include "config/GlutenConfig.h"
#include "operators/serializer/VeloxRowToColumnarConverter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "utils/ConfigExtractor.h"
#include "utils/VeloxArrowUtils.h"

using namespace facebook;

namespace gluten {

VeloxRuntime::VeloxRuntime(const std::unordered_map<std::string, std::string>& confMap) : Runtime(confMap) {
  // Refresh session config.
  veloxCfg_ = std::make_shared<const facebook::velox::core::MemConfigMutable>(confMap_);
  debugModeEnabled_ = veloxCfg_->get<bool>(kDebugModeEnabled, false);
  FLAGS_minloglevel = veloxCfg_->get<uint32_t>(kGlogSeverityLevel, FLAGS_minloglevel);
  FLAGS_v = veloxCfg_->get<uint32_t>(kGlogVerboseLevel, FLAGS_v);
}

void VeloxRuntime::parsePlan(const uint8_t* data, int32_t size, std::optional<std::string> dumpFile) {
  if (debugModeEnabled_ || dumpFile.has_value()) {
    try {
      auto planJson = substraitFromPbToJson("Plan", data, size, dumpFile);
      LOG_IF(INFO, debugModeEnabled_) << std::string(50, '#') << " received substrait::Plan: " << taskInfo_ << std::endl
                                      << planJson;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting Substrait plan to JSON: " << e.what();
    }
  }

  GLUTEN_CHECK(parseProtobuf(data, size, &substraitPlan_) == true, "Parse substrait plan failed");
}

void VeloxRuntime::parseSplitInfo(const uint8_t* data, int32_t size, std::optional<std::string> dumpFile) {
  if (debugModeEnabled_ || dumpFile.has_value()) {
    try {
      auto splitJson = substraitFromPbToJson("ReadRel.LocalFiles", data, size, dumpFile);
      LOG_IF(INFO, debugModeEnabled_) << std::string(50, '#')
                                      << " received substrait::ReadRel.LocalFiles: " << taskInfo_ << std::endl
                                      << splitJson;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting Substrait plan to JSON: " << e.what();
    }
  }
  ::substrait::ReadRel_LocalFiles localFile;
  GLUTEN_CHECK(parseProtobuf(data, size, &localFile) == true, "Parse substrait plan failed");
  localFiles_.push_back(localFile);
}

void VeloxRuntime::getInfoAndIds(
    const std::unordered_map<velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
    const std::unordered_set<velox::core::PlanNodeId>& leafPlanNodeIds,
    std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    std::vector<velox::core::PlanNodeId>& scanIds,
    std::vector<velox::core::PlanNodeId>& streamIds) {
  for (const auto& leafPlanNodeId : leafPlanNodeIds) {
    auto it = splitInfoMap.find(leafPlanNodeId);
    if (it == splitInfoMap.end()) {
      throw std::runtime_error("Could not find leafPlanNodeId.");
    }
    auto splitInfo = it->second;
    if (splitInfo->isStream) {
      streamIds.emplace_back(leafPlanNodeId);
    } else {
      scanInfos.emplace_back(splitInfo);
      scanIds.emplace_back(leafPlanNodeId);
    }
  }
}

std::string VeloxRuntime::planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) {
  std::vector<std::shared_ptr<ResultIterator>> inputs;
  auto veloxMemoryPool = gluten::defaultLeafVeloxMemoryPool();
  VeloxPlanConverter veloxPlanConverter(inputs, veloxMemoryPool.get(), sessionConf, std::nullopt, true);
  auto veloxPlan = veloxPlanConverter.toVeloxPlan(substraitPlan_, localFiles_);
  return veloxPlan->toString(details, true);
}

void VeloxRuntime::injectWriteFilesTempPath(const std::string& path) {
  writeFilesTempPath_ = path;
}

std::shared_ptr<ResultIterator> VeloxRuntime::createResultIterator(
    MemoryManager* memoryManager,
    const std::string& spillDir,
    const std::vector<std::shared_ptr<ResultIterator>>& inputs,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  LOG_IF(INFO, debugModeEnabled_) << "VeloxRuntime session config:" << printConfig(confMap_);

  VeloxPlanConverter veloxPlanConverter(
      inputs, getLeafVeloxPool(memoryManager).get(), sessionConf, writeFilesTempPath_);
  veloxPlan_ = veloxPlanConverter.toVeloxPlan(substraitPlan_, std::move(localFiles_));

  // Scan node can be required.
  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(veloxPlanConverter.splitInfos(), veloxPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto* vmm = toVeloxMemoryManager(memoryManager);
  auto wholestageIter = std::make_unique<WholeStageResultIterator>(
      vmm, veloxPlan_, scanIds, scanInfos, streamIds, spillDir, sessionConf, taskInfo_);
  return std::make_shared<ResultIterator>(std::move(wholestageIter), this);
}

std::shared_ptr<ColumnarToRowConverter> VeloxRuntime::createColumnar2RowConverter(MemoryManager* memoryManager) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_shared<VeloxColumnarToRowConverter>(ctxVeloxPool);
}

std::shared_ptr<ColumnarBatch> VeloxRuntime::createOrGetEmptySchemaBatch(int32_t numRows) {
  auto& lookup = emptySchemaBatchLoopUp_;
  if (lookup.find(numRows) == lookup.end()) {
    const std::shared_ptr<ColumnarBatch>& batch = gluten::createZeroColumnBatch(numRows);
    lookup.emplace(numRows, batch); // the batch will be released after Spark task ends
  }
  return lookup.at(numRows);
}

std::shared_ptr<ColumnarBatch> VeloxRuntime::select(
    MemoryManager* memoryManager,
    std::shared_ptr<ColumnarBatch> batch,
    std::vector<int32_t> columnIndices) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  auto veloxBatch = gluten::VeloxColumnarBatch::from(ctxVeloxPool.get(), batch);
  auto outputBatch = veloxBatch->select(ctxVeloxPool.get(), std::move(columnIndices));
  return outputBatch;
}

std::shared_ptr<RowToColumnarConverter> VeloxRuntime::createRow2ColumnarConverter(
    MemoryManager* memoryManager,
    struct ArrowSchema* cSchema) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_shared<VeloxRowToColumnarConverter>(cSchema, ctxVeloxPool);
}

std::shared_ptr<ShuffleWriter> VeloxRuntime::createShuffleWriter(
    int numPartitions,
    std::unique_ptr<PartitionWriter> partitionWriter,
    ShuffleWriterOptions options,
    MemoryManager* memoryManager) {
  auto ctxPool = getLeafVeloxPool(memoryManager);
  auto arrowPool = memoryManager->getArrowMemoryPool();
  GLUTEN_ASSIGN_OR_THROW(
      auto shuffle_writer,
      VeloxShuffleWriter::create(numPartitions, std::move(partitionWriter), std::move(options), ctxPool, arrowPool));
  return shuffle_writer;
}

std::shared_ptr<Datasource> VeloxRuntime::createDatasource(
    const std::string& filePath,
    MemoryManager* memoryManager,
    std::shared_ptr<arrow::Schema> schema) {
  static std::atomic_uint32_t id{0UL};
  auto veloxPool = getAggregateVeloxPool(memoryManager)->addAggregateChild("datasource." + std::to_string(id++));
  // Pass a dedicate pool for S3 and GCS sinks as can't share veloxPool
  // with parquet writer.
  auto s3SinkPool = getLeafVeloxPool(memoryManager);
  auto gcsSinkPool = getLeafVeloxPool(memoryManager);

  return std::make_shared<VeloxParquetDatasource>(filePath, veloxPool, s3SinkPool, gcsSinkPool, schema);
}

std::shared_ptr<ShuffleReader> VeloxRuntime::createShuffleReader(
    std::shared_ptr<arrow::Schema> schema,
    ShuffleReaderOptions options,
    arrow::MemoryPool* pool,
    MemoryManager* memoryManager) {
  auto rowType = facebook::velox::asRowType(gluten::fromArrowSchema(schema));
  auto codec = gluten::createArrowIpcCodec(options.compressionType, options.codecBackend);
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  auto deserializerFactory = std::make_unique<gluten::VeloxColumnarBatchDeserializerFactory>(
      schema, std::move(codec), rowType, options.batchSize, pool, ctxVeloxPool);
  return std::make_shared<VeloxShuffleReader>(std::move(deserializerFactory));
}

std::unique_ptr<ColumnarBatchSerializer> VeloxRuntime::createColumnarBatchSerializer(
    MemoryManager* memoryManager,
    arrow::MemoryPool* arrowPool,
    struct ArrowSchema* cSchema) {
  auto ctxVeloxPool = getLeafVeloxPool(memoryManager);
  return std::make_unique<VeloxColumnarBatchSerializer>(arrowPool, ctxVeloxPool, cSchema);
}

void VeloxRuntime::dumpConf(const std::string& path) {
  auto backendConf = VeloxBackend::get()->getBackendConf();
  auto allConf = backendConf;

  for (const auto& pair : confMap_) {
    allConf.insert_or_assign(pair.first, pair.second);
  }

  // Open file "velox.conf" for writing, automatically creating it if it doesn't exist,
  // or overwriting it if it does.
  std::ofstream outFile(path);
  if (!outFile.is_open()) {
    LOG(ERROR) << "Failed to open file for writing: " << path;
    return;
  }

  // Calculate the maximum key length for alignment.
  size_t maxKeyLength = 0;
  for (const auto& pair : allConf) {
    maxKeyLength = std::max(maxKeyLength, pair.first.length());
  }

  // Write each key-value pair to the file with adjusted spacing for alignment
  outFile << "[Backend Conf]" << std::endl;
  for (const auto& pair : backendConf) {
    outFile << std::left << std::setw(maxKeyLength + 1) << pair.first << ' ' << pair.second << std::endl;
  }
  outFile << std::endl << "[Session Conf]" << std::endl;
  for (const auto& pair : confMap_) {
    outFile << std::left << std::setw(maxKeyLength + 1) << pair.first << ' ' << pair.second << std::endl;
  }

  outFile.close();
}

} // namespace gluten
