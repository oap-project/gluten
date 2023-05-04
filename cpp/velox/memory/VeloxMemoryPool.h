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

#include "memory/MemoryAllocator.h"
#include "velox/common/memory/Memory.h"

namespace gluten {
class WrappedVeloxMemoryPool;

std::shared_ptr<facebook::velox::memory::MemoryPool> AsWrappedVeloxAggregateMemoryPool(
    MemoryAllocator* allocator,
    const facebook::velox::memory::MemoryPool::Options& options);

std::shared_ptr<facebook::velox::memory::MemoryPool> GetDefaultWrappedVeloxAggregateMemoryPool();

std::shared_ptr<facebook::velox::memory::MemoryPool> GetDefaultVeloxLeafMemoryPool();

} // namespace gluten
