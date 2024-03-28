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

#include "WholeStageResultIterator.h"
#include "compute/Runtime.h"
#include "memory/VeloxMemoryManager.h"
#include "operators/serializer/VeloxColumnarBatchSerializer.h"
#include "operators/serializer/VeloxColumnarToRowConverter.h"
#include "operators/writer/VeloxParquetDatasource.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"

namespace gluten {

// This kind string must be same with VeloxBackend#name in java side.
inline static const std::string kVeloxRuntimeKind{"velox"};

class VeloxRuntime final : public Runtime {
 public:
  explicit VeloxRuntime(const std::unordered_map<std::string, std::string>& confMap);

  void parsePlan(const uint8_t* data, int32_t size, std::optional<std::string> dumpFile) override;

  void parseSplitInfo(const uint8_t* data, int32_t size, std::optional<std::string> dumpFile) override;

  static std::shared_ptr<facebook::velox::memory::MemoryPool> getAggregateVeloxPool(MemoryManager* memoryManager) {
    return toVeloxMemoryManager(memoryManager)->getAggregateMemoryPool();
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> getLeafVeloxPool(MemoryManager* memoryManager) {
    return toVeloxMemoryManager(memoryManager)->getLeafMemoryPool();
  }

  static VeloxMemoryManager* toVeloxMemoryManager(MemoryManager* memoryManager) {
    if (auto veloxMemoryManager = dynamic_cast<VeloxMemoryManager*>(memoryManager)) {
      return veloxMemoryManager;
    } else {
      GLUTEN_CHECK(false, "Velox memory manager should be used for Velox runtime.");
    }
  }

  MemoryManager* createMemoryManager(
      const std::string& name,
      std::shared_ptr<MemoryAllocator> allocator,
      std::unique_ptr<AllocationListener> listener) override {
    return new VeloxMemoryManager(name, allocator, std::move(listener));
  }

  // FIXME This is not thread-safe?
  std::shared_ptr<ResultIterator> createResultIterator(
      MemoryManager* memoryManager,
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs = {},
      const std::unordered_map<std::string, std::string>& sessionConf = {}) override;

  std::shared_ptr<ColumnarToRowConverter> createColumnar2RowConverter(MemoryManager* memoryManager) override;

  std::shared_ptr<ColumnarBatch> createOrGetEmptySchemaBatch(int32_t numRows) override;

  std::shared_ptr<ColumnarBatch> select(
      MemoryManager* memoryManager,
      std::shared_ptr<ColumnarBatch> batch,
      std::vector<int32_t> columnIndices) override;

  std::shared_ptr<RowToColumnarConverter> createRow2ColumnarConverter(
      MemoryManager* memoryManager,
      struct ArrowSchema* cSchema) override;

  std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions options,
      MemoryManager* memoryManager) override;

  Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    auto iter = static_cast<WholeStageResultIterator*>(rawIter);
    return iter->getMetrics(exportNanos);
  }

  std::shared_ptr<Datasource> createDatasource(
      const std::string& filePath,
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::Schema> schema) override;

  std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ShuffleReaderOptions options,
      arrow::MemoryPool* pool,
      MemoryManager* memoryManager) override;

  std::unique_ptr<ColumnarBatchSerializer> createColumnarBatchSerializer(
      MemoryManager* memoryManager,
      arrow::MemoryPool* arrowPool,
      struct ArrowSchema* cSchema) override;

  std::string planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) override;

  void injectWriteFilesTempPath(const std::string& path) override;

  void dumpConf(const std::string& path) override;

  std::shared_ptr<const facebook::velox::core::PlanNode> getVeloxPlan() {
    return veloxPlan_;
  }

  bool debugModeEnabled() const {
    return debugModeEnabled_;
  }

  static void getInfoAndIds(
      const std::unordered_map<facebook::velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
      const std::unordered_set<facebook::velox::core::PlanNodeId>& leafPlanNodeIds,
      std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
      std::vector<facebook::velox::core::PlanNodeId>& scanIds,
      std::vector<facebook::velox::core::PlanNodeId>& streamIds);

 private:
  std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan_;
  std::shared_ptr<const facebook::velox::Config> veloxCfg_;
  bool debugModeEnabled_{false};

  std::unordered_map<int32_t, std::shared_ptr<ColumnarBatch>> emptySchemaBatchLoopUp_;
};

} // namespace gluten
