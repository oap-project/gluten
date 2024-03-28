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

#include "compute/VeloxRuntime.h"

#include <gtest/gtest.h>

namespace gluten {

class DummyRuntime final : public Runtime {
 public:
  DummyRuntime(const std::unordered_map<std::string, std::string>& conf) : Runtime(conf) {}

  void parsePlan(const uint8_t* data, int32_t size, std::optional<std::string> dumpFile) override {}

  void parseSplitInfo(const uint8_t* data, int32_t size, std::optional<std::string> dumpFile) override {}

  std::shared_ptr<ResultIterator> createResultIterator(
      MemoryManager* memoryManager,
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs,
      const std::unordered_map<std::string, std::string>& sessionConf) override {
    auto resIter = std::make_unique<DummyResultIterator>();
    auto iter = std::make_shared<ResultIterator>(std::move(resIter));
    return iter;
  }
  MemoryManager* createMemoryManager(
      const std::string& name,
      std::shared_ptr<MemoryAllocator> ptr,
      std::unique_ptr<AllocationListener> uniquePtr) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ColumnarBatch> createOrGetEmptySchemaBatch(int32_t numRows) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ColumnarToRowConverter> createColumnar2RowConverter(MemoryManager* memoryManager) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<RowToColumnarConverter> createRow2ColumnarConverter(
      MemoryManager* memoryManager,
      struct ArrowSchema* cSchema) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions,
      MemoryManager* memoryManager) override {
    throw GlutenException("Not yet implemented");
  }
  Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    static Metrics m(1);
    return &m;
  }
  std::shared_ptr<Datasource> createDatasource(
      const std::string& filePath,
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::Schema> schema) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ShuffleReaderOptions options,
      arrow::MemoryPool* pool,
      MemoryManager* memoryManager) override {
    throw GlutenException("Not yet implemented");
  }
  std::unique_ptr<ColumnarBatchSerializer> createColumnarBatchSerializer(
      MemoryManager* memoryManager,
      arrow::MemoryPool* arrowPool,
      struct ArrowSchema* cSchema) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ColumnarBatch> select(MemoryManager*, std::shared_ptr<ColumnarBatch>, std::vector<int32_t>) override {
    throw GlutenException("Not yet implemented");
  }
  std::string planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) override {
    throw GlutenException("Not yet implemented");
  }
  void injectWriteFilesTempPath(const std::string& path) override {
    throw GlutenException("Not yet implemented");
  }

  void dumpConf(const std::string& path) override {
    throw GlutenException("Not yet implemented");
  }

 private:
  ResourceMap<std::shared_ptr<ResultIterator>> resultIteratorHolder_;

  class DummyResultIterator : public ColumnarBatchIterator {
   public:
    std::shared_ptr<ColumnarBatch> next() override {
      if (!hasNext_) {
        return nullptr;
      }
      hasNext_ = false;

      return gluten::createZeroColumnBatch(1);
    }

   private:
    bool hasNext_ = true;
  };
};

static Runtime* dummyRuntimeFactory(const std::unordered_map<std::string, std::string> conf) {
  return new DummyRuntime(conf);
}

TEST(TestRuntime, CreateRuntime) {
  Runtime::registerFactory("DUMMY", dummyRuntimeFactory);
  auto runtime = Runtime::create("DUMMY");
  ASSERT_EQ(typeid(*runtime), typeid(DummyRuntime));
  Runtime::release(runtime);
}

TEST(TestRuntime, GetResultIterator) {
  auto runtime = std::make_shared<DummyRuntime>(std::unordered_map<std::string, std::string>());
  auto iter = runtime->createResultIterator(nullptr, "/tmp/test-spill", {}, {});
  ASSERT_TRUE(iter->hasNext());
  auto next = iter->next();
  ASSERT_NE(next, nullptr);
  ASSERT_FALSE(iter->hasNext());
  next = iter->next();
  ASSERT_EQ(next, nullptr);
}

} // namespace gluten
