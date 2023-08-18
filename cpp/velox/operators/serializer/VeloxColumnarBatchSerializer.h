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

#include "memory/ColumnarBatch.h"
#include "operators/serializer/ColumnarBatchSerde.h"
#include "velox/serializers/PrestoSerializer.h"

namespace gluten {

class VeloxColumnarBatchSerializer final : public ColumnarBatchSerializer {
 public:
  VeloxColumnarBatchSerializer(
      std::shared_ptr<facebook::velox::memory::MemoryPool>,
      std::shared_ptr<arrow::MemoryPool>);

  std::shared_ptr<arrow::Buffer> serializeColumnarBatches(
      const std::vector<std::shared_ptr<ColumnarBatch>>& batches) override;

 private:
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
  std::unique_ptr<facebook::velox::serializer::presto::PrestoVectorSerde> serde_;
  std::shared_ptr<arrow::MemoryPool> arrowPool_;
};

} // namespace gluten
