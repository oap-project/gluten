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

#include "VeloxMemoryManager.h"
#ifdef ENABLE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/MemoryReclaimer.h"

#include "memory/ArrowMemoryPool.h"
#include "utils/exception.h"

DECLARE_int32(gluten_velox_aysnc_timeout_on_task_stopping);

namespace gluten {

using namespace facebook;

/// We assume in a single Spark task. No thread-safety should be guaranteed.
class ListenableArbitrator : public velox::memory::MemoryArbitrator {
 public:
  ListenableArbitrator(const Config& config, AllocationListener* listener)
      : MemoryArbitrator(config), listener_(listener) {}

  std::string kind() const override {
    return kind_;
  }

  uint64_t growCapacity(velox::memory::MemoryPool* pool, uint64_t targetBytes) override {
    std::lock_guard<std::recursive_mutex> l(mutex_);
    return growPoolLocked(pool, targetBytes);
  }

  uint64_t shrinkCapacity(velox::memory::MemoryPool* pool, uint64_t targetBytes) override {
    std::lock_guard<std::recursive_mutex> l(mutex_);
    return releaseMemoryLocked(pool, targetBytes);
  }

  bool growCapacity(
      velox::memory::MemoryPool* pool,
      const std::vector<std::shared_ptr<velox::memory::MemoryPool>>& candidatePools,
      uint64_t targetBytes) override {
    GLUTEN_CHECK(candidatePools.size() == 1, "ListenableArbitrator should only be used within a single root pool");
    auto candidate = candidatePools.back();
    GLUTEN_CHECK(pool->root() == candidate.get(), "Illegal state in ListenableArbitrator");
    {
      std::lock_guard<std::recursive_mutex> l(mutex_);
      growPoolLocked(pool, targetBytes);
    }
    return true;
  }

  uint64_t shrinkCapacity(
      const std::vector<std::shared_ptr<velox::memory::MemoryPool>>& pools,
      uint64_t targetBytes,
      bool allowSpill = true,
      bool allowAbort = false) override {
    facebook::velox::exec::MemoryReclaimer::Stats status;
    GLUTEN_CHECK(pools.size() == 1, "Should shrink a single pool at a time");
    std::lock_guard<std::recursive_mutex> l(mutex_); // FIXME: Do we have recursive locking for this mutex?
    auto pool = pools.at(0);
    const uint64_t oldCapacity = pool->capacity();
    pool->reclaim(targetBytes, 0, status); // ignore the output
    pool->shrink(0);
    const uint64_t newCapacity = pool->capacity();
    uint64_t total = oldCapacity - newCapacity;
    listener_->allocationChanged(-total);
    return total;
  }

  Stats stats() const override {
    Stats stats; // no-op
    return stats;
  }

  std::string toString() const override {
    return fmt::format("ARBITRATOR[{}] CAPACITY {} {}", kind_, velox::succinctBytes(capacity_), stats().toString());
  }

 private:
  uint64_t growPoolLocked(velox::memory::MemoryPool* pool, uint64_t bytes) {
    listener_->allocationChanged(bytes);
    return pool->grow(bytes);
  }

  uint64_t releaseMemoryLocked(velox::memory::MemoryPool* pool, uint64_t bytes) {
    uint64_t freeBytes = pool->shrink(0);
    listener_->allocationChanged(-freeBytes);
    return freeBytes;
  }

  gluten::AllocationListener* listener_;
  std::recursive_mutex mutex_;
  inline static std::string kind_ = "GLUTEN";
};

class ArbitratorFactoryRegister {
 public:
  explicit ArbitratorFactoryRegister(gluten::AllocationListener* listener) : listener_(listener) {
    static std::atomic_uint32_t id{0UL};
    kind_ = "GLUTEN_ARBITRATOR_FACTORY_" + std::to_string(id++);
    velox::memory::MemoryArbitrator::registerFactory(
        kind_,
        [this](
            const velox::memory::MemoryArbitrator::Config& config) -> std::unique_ptr<velox::memory::MemoryArbitrator> {
          return std::make_unique<ListenableArbitrator>(config, listener_);
        });
  }

  virtual ~ArbitratorFactoryRegister() {
    velox::memory::MemoryArbitrator::unregisterFactory(kind_);
  }

  const std::string& getKind() const {
    return kind_;
  }

 private:
  std::string kind_;
  gluten::AllocationListener* listener_;
};

VeloxMemoryManager::VeloxMemoryManager(
    const std::string& name,
    std::shared_ptr<MemoryAllocator> allocator,
    std::unique_ptr<AllocationListener> listener)
    : MemoryManager(), name_(name), listener_(std::move(listener)) {
  glutenAlloc_ = std::make_unique<ListenableMemoryAllocator>(allocator.get(), listener_.get());
  arrowPool_ = std::make_unique<ArrowMemoryPool>(glutenAlloc_.get());

  ArbitratorFactoryRegister afr(listener_.get());
  velox::memory::MemoryManagerOptions mmOptions{
      .alignment = velox::memory::MemoryAllocator::kMaxAlignment,
      .trackDefaultUsage = true, // memory usage tracking
      .checkUsageLeak = true, // leak check
      .debugEnabled = false, // debug
      .coreOnAllocationFailureEnabled = false,
      .allocatorCapacity = velox::memory::kMaxMemory,
      .arbitratorKind = afr.getKind(),
      .memoryPoolInitCapacity = 0,
      .memoryPoolTransferCapacity = 32 << 20,
      .memoryReclaimWaitMs = 0};
  veloxMemoryManager_ = std::make_unique<velox::memory::MemoryManager>(mmOptions);

  veloxAggregatePool_ = veloxMemoryManager_->addRootPool(
      name_ + "_root",
      velox::memory::kMaxMemory, // the 3rd capacity
      facebook::velox::memory::MemoryReclaimer::create());

  veloxLeafPool_ = veloxAggregatePool_->addLeafChild(name_ + "_default_leaf");
}

namespace {
MemoryUsageStats collectMemoryUsageStatsInternal(const velox::memory::MemoryPool* pool) {
  MemoryUsageStats stats;
  stats.set_current(pool->currentBytes());
  stats.set_peak(pool->peakBytes());
  // walk down root and all children
  pool->visitChildren([&](velox::memory::MemoryPool* pool) -> bool {
    stats.mutable_children()->emplace(pool->name(), collectMemoryUsageStatsInternal(pool));
    return true;
  });
  return stats;
}

int64_t shrinkVeloxMemoryPool(velox::memory::MemoryManager* mm, velox::memory::MemoryPool* pool, int64_t size) {
  std::string poolName{pool->root()->name() + "/" + pool->name()};
  std::string logPrefix{"Shrink[" + poolName + "]: "};
  VLOG(2) << logPrefix << "Trying to shrink " << size << " bytes of data...";
  VLOG(2) << logPrefix << "Pool has reserved " << pool->currentBytes() << "/" << pool->root()->reservedBytes() << "/"
          << pool->root()->capacity() << "/" << pool->root()->maxCapacity() << " bytes.";
  VLOG(2) << logPrefix << "Shrinking...";
  const uint64_t oldCapacity = pool->capacity();
  mm->arbitrator()->shrinkCapacity(pool, 0);
  const uint64_t newCapacity = pool->capacity();
  int64_t shrunken = oldCapacity - newCapacity;
  VLOG(2) << logPrefix << shrunken << " bytes released from shrinking.";
  return shrunken;
}
} // namespace

const MemoryUsageStats VeloxMemoryManager::collectMemoryUsageStats() const {
  return collectMemoryUsageStatsInternal(veloxAggregatePool_.get());
}

const int64_t VeloxMemoryManager::shrink(int64_t size) {
  return shrinkVeloxMemoryPool(veloxMemoryManager_.get(), veloxAggregatePool_.get(), size);
}

namespace {
void holdInternal(
    std::vector<std::shared_ptr<facebook::velox::memory::MemoryPool>>& heldVeloxPools,
    const velox::memory::MemoryPool* pool) {
  pool->visitChildren([&](velox::memory::MemoryPool* child) -> bool {
    auto shared = child->shared_from_this();
    heldVeloxPools.push_back(shared);
    holdInternal(heldVeloxPools, child);
    return true;
  });
}
} // namespace

void VeloxMemoryManager::hold() {
  holdInternal(heldVeloxPools_, veloxAggregatePool_.get());
}

bool VeloxMemoryManager::tryDestructSafe() {
  // Velox memory pools considered safe to destruct when no alive allocations.
  for (const auto& pool : heldVeloxPools_) {
    if (pool && pool->currentBytes() != 0) {
      return false;
    }
  }
  if (veloxLeafPool_ && veloxLeafPool_->currentBytes() != 0) {
    return false;
  }
  if (veloxAggregatePool_ && veloxAggregatePool_->currentBytes() != 0) {
    return false;
  }
  heldVeloxPools_.clear();
  veloxLeafPool_.reset();
  veloxAggregatePool_.reset();

  // Velox memory manager considered safe to destruct when no alive pools.
  if (veloxMemoryManager_) {
    if (veloxMemoryManager_->numPools() > 1) {
      return false;
    }
    if (veloxMemoryManager_->numPools() == 1) {
      // Assert the pool is spill pool
      // See https://github.com/facebookincubator/velox/commit/e6f84e8ac9ef6721f527a2d552a13f7e79bdf72e
      int32_t spillPoolCount = 0;
      veloxMemoryManager_->testingDefaultRoot().visitChildren([&](velox::memory::MemoryPool* child) -> bool {
        if (child == veloxMemoryManager_->spillPool()) {
          spillPoolCount++;
        }
        return true;
      });
      GLUTEN_CHECK(spillPoolCount == 1, "Illegal pool count state: spillPoolCount: " + std::to_string(spillPoolCount));
    }
    if (veloxMemoryManager_->numPools() < 1) {
      GLUTEN_CHECK(false, "Unreachable code");
    }
  }
  veloxMemoryManager_.reset();

  // Applies similar rule for Arrow memory pool.
  if (arrowPool_ && arrowPool_->bytes_allocated() != 0) {
    return false;
  }
  arrowPool_.reset();

  // Successfully destructed.
  return true;
}

VeloxMemoryManager::~VeloxMemoryManager() {
  static const uint32_t kWaitTimeoutMs = FLAGS_gluten_velox_aysnc_timeout_on_task_stopping; // 30s by default
  uint32_t accumulatedWaitMs = 0UL;
  for (int32_t tryCount = 0; accumulatedWaitMs < kWaitTimeoutMs; tryCount++) {
    if (tryDestructSafe()) {
      if (tryCount > 0) {
        LOG(INFO) << "All the outstanding memory resources successfully released. ";
      }
      break;
    }
    uint32_t waitMs = 50 * static_cast<uint32_t>(pow(1.5, tryCount)); // 50ms, 75ms, 112.5ms ...
    LOG(INFO) << "There are still outstanding Velox memory allocations. Waiting for " << waitMs
              << " ms to let possible async tasks done... ";
    usleep(waitMs * 1000);
    accumulatedWaitMs += waitMs;
  }
#ifdef ENABLE_JEMALLOC
  je_gluten_malloc_stats_print(NULL, NULL, NULL);
#endif
}

} // namespace gluten
