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

#include "VeloxColumnarToColumnarConverter.h"
#include "memory/VeloxColumnarBatch.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/row/UnsafeRowDeserializers.h"

using namespace facebook;
using namespace facebook::velox;
namespace gluten {
VeloxColumnarToColumnarConverter::VeloxColumnarToColumnarConverter(
    struct ArrowSchema* cSchema,
    std::shared_ptr<memory::MemoryPool> memoryPool)
    : ColumnarToColumnarConverter(), cSchema_(cSchema), pool_(memoryPool) {}

VeloxColumnarToColumnarConverter::~VeloxColumnarToColumnarConverter() {
  if (cSchema_) {
    ArrowSchemaRelease(cSchema_);
  }
}

std::shared_ptr<ColumnarBatch> VeloxColumnarToColumnarConverter::convert(ArrowArray* cArray) {
  auto vp = importFromArrowAsViewer(*cSchema_, *cArray, ArrowUtils::getBridgeOptions(), pool_.get());
  return std::make_shared<VeloxColumnarBatch>(std::dynamic_pointer_cast<RowVector>(vp));
}
} // namespace gluten
