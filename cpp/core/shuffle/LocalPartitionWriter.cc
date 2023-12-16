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
#include <random>
#include <thread>

#include <boost/stacktrace.hpp>
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/Payload.h"
#include "shuffle/Spill.h"
#include "shuffle/Utils.h"
#include "utils/StringUtil.h"
#include "utils/Timer.h"

namespace gluten {

class LocalPartitionWriter::LocalEvictor {
 public:
  LocalEvictor(
      uint32_t numPartitions,
      ShuffleWriterOptions* options,
      const std::string& spillFile,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec)
      : options_(options), numPartitions_(numPartitions), spillFile_(spillFile), pool_(pool), codec_(codec) {}

  virtual ~LocalEvictor() {}

  virtual arrow::Status evict(uint32_t partitionId, std::unique_ptr<Payload> payload) = 0;

  virtual arrow::Status spill() = 0;

  virtual Evict::type evictType() = 0;

  virtual arrow::Result<std::unique_ptr<Spill>> finish() = 0;

  int64_t getEvictTime() {
    return evictTime_;
  }

 protected:
  ShuffleWriterOptions* options_;
  uint32_t numPartitions_;
  std::string spillFile_;
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;

  int64_t evictTime_{0};
};

class LocalPartitionWriter::PayloadMerger {
 public:
  PayloadMerger(ShuffleWriterOptions* options, arrow::MemoryPool* pool, arrow::util::Codec* codec, bool hasComplexType)
      : options_(options), pool_(pool), codec_(codec), hasComplexType_(hasComplexType) {}

  arrow::Result<std::optional<std::unique_ptr<Payload>>> merge(
      uint32_t partitionId,
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      const std::vector<bool>* isValidityBuffer,
      bool reuseBuffers) {
    if (hasComplexType_) {
      // TODO: Merging complex type is not supported.
      ARROW_ASSIGN_OR_RAISE(
          auto blockPayload, createBlockPayload(numRows, std::move(buffers), isValidityBuffer, reuseBuffers));
      return blockPayload;
    }

    MergeGuard mergeGuard(partitionInMerge_, partitionId);

    if (!hasMerged(partitionId)) {
      if (numRows < options_->compression_threshold) {
        // Save for merge.
        ARROW_ASSIGN_OR_RAISE(
            partitionMergePayload_[partitionId],
            createMergeBlockPayload(numRows, std::move(buffers), isValidityBuffer, reuseBuffers));
        return std::nullopt;
      }
      // If already reach compression threshold, return BlockPayload.
      ARROW_ASSIGN_OR_RAISE(
          auto blockPayload, createBlockPayload(numRows, std::move(buffers), isValidityBuffer, reuseBuffers));
      return blockPayload;
    }
    auto lastPayload = std::move(partitionMergePayload_[partitionId]);
    auto payload = std::make_unique<BlockPayload>(getPayloadType(), numRows, std::move(buffers), isValidityBuffer);
    ARROW_ASSIGN_OR_RAISE(
        auto merged, MergeBlockPayload::merge(std::move(lastPayload), std::move(payload), pool_, codec_));
    if (numRows + lastPayload->numRows() < options_->compression_threshold) {
      // Still not reach compression threshold, save for next merge.
      partitionMergePayload_[partitionId] = std::move(merged);
      return std::nullopt;
    }
    return merged->finish(true);
  }

  arrow::Result<std::optional<std::unique_ptr<Payload>>> finish(uint32_t partitionId, bool spill) {
    if (spill) {
      // If it's triggered by spill, we need to check whether the spill source is from compressing the merged buffers.
      if ((partitionInMerge_.has_value() && *partitionInMerge_ == partitionId) || !hasMerged(partitionId)) {
        return std::nullopt;
      }
      return partitionMergePayload_[partitionId]->finish(false);
    }
    if (!hasMerged(partitionId)) {
      return std::nullopt;
    }
    return std::move(partitionMergePayload_[partitionId]);
  }

 private:
  ShuffleWriterOptions* options_;
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;
  bool hasComplexType_;
  std::unordered_map<uint32_t, std::unique_ptr<MergeBlockPayload>> partitionMergePayload_;
  std::optional<uint32_t> partitionInMerge_;

  class MergeGuard {
   public:
    MergeGuard(std::optional<uint32_t>& partitionInMerge, uint32_t partitionId) : partitionInMerge_(partitionInMerge) {
      partitionInMerge_ = partitionId;
    }
    ~MergeGuard() {
      partitionInMerge_ = std::nullopt;
    }

   private:
    std::optional<uint32_t>& partitionInMerge_;
  };

  bool hasMerged(uint32_t partitionId) {
    return partitionMergePayload_.find(partitionId) != partitionMergePayload_.end() &&
        partitionMergePayload_[partitionId] != nullptr;
  }

  Payload::Type getPayloadType() {
    return codec_ == nullptr ? Payload::kUncompressed : Payload::kCompressed;
  }

  arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> copyBuffers(
      std::vector<std::shared_ptr<arrow::Buffer>> buffers) {
    // Copy.
    std::vector<std::shared_ptr<arrow::Buffer>> copies;
    for (auto& buffer : buffers) {
      if (!buffer) {
        copies.push_back(nullptr);
        continue;
      }
      ARROW_ASSIGN_OR_RAISE(auto copy, arrow::AllocateResizableBuffer(buffer->size(), pool_));
      memcpy(copy->mutable_data(), buffer->data(), buffer->size());
      buffer = nullptr;
      copies.push_back(std::move(copy));
    }
    return copies;
  }

  arrow::Result<std::unique_ptr<MergeBlockPayload>> createMergeBlockPayload(
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      const std::vector<bool>* isValidityBuffer,
      bool reuseBuffers) {
    if (reuseBuffers) {
      // This is the first payload, therefore unmergable, need copy.
      ARROW_ASSIGN_OR_RAISE(buffers, copyBuffers(std::move(buffers)));
    }
    return std::make_unique<MergeBlockPayload>(
        getPayloadType(), numRows, std::move(buffers), isValidityBuffer, pool_, codec_);
  }

  arrow::Result<std::unique_ptr<Payload>> createBlockPayload(
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      const std::vector<bool>* isValidityBuffer,
      bool reuseBuffers) {
    if (reuseBuffers && codec_ == nullptr) {
      // For uncompressed buffers, need to copy before caching.
      ARROW_ASSIGN_OR_RAISE(buffers, copyBuffers(std::move(buffers)));
    }
    ARROW_ASSIGN_OR_RAISE(
        auto payload,
        BlockPayload::fromBuffers(getPayloadType(), numRows, std::move(buffers), isValidityBuffer, pool_, codec_));
    return payload;
  }
};

class CacheEvictor : public LocalPartitionWriter::LocalEvictor {
 public:
  CacheEvictor(
      uint32_t numPartitions,
      ShuffleWriterOptions* options,
      const std::string& spillFile,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec)
      : LocalPartitionWriter::LocalEvictor(numPartitions, options, spillFile, pool, codec) {}

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<Payload> payload) override {
    if (partitionCachedPayload_.find(partitionId) == partitionCachedPayload_.end()) {
      partitionCachedPayload_[partitionId] = std::list<std::unique_ptr<Payload>>{};
    }
    partitionCachedPayload_[partitionId].push_back(std::move(payload));
    return arrow::Status::OK();
  }

  arrow::Status spill() override {
    ScopedTimer timer(evictTime_);

    ARROW_ASSIGN_OR_RAISE(auto os, arrow::io::FileOutputStream::Open(spillFile_, true));
    ARROW_ASSIGN_OR_RAISE(auto start, os->Tell());
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      if (partitionCachedPayload_.find(pid) != partitionCachedPayload_.end()) {
        auto payloads = std::move(partitionCachedPayload_[pid]);
        while (!payloads.empty()) {
          auto payload = std::move(payloads.front());
          payloads.pop_front();
          RETURN_NOT_OK(payload->serialize(os.get()));

          if (UNLIKELY(!diskSpill_)) {
            diskSpill_ = std::make_unique<DiskSpill>(Spill::SpillType::kBatchedSpill, numPartitions_, spillFile_);
          }
          ARROW_ASSIGN_OR_RAISE(auto end, os->Tell());
          DEBUG_OUT << "Spilled partition " << pid << " file start: " << start << ", file end: " << end
                    << ", file: " << spillFile_ << std::endl;
          diskSpill_->insertPayload(
              pid, payload->type(), payload->numRows(), payload->isValidityBuffer(), end - start, pool_, codec_);
          start = end;
        }
      }
    }
    RETURN_NOT_OK(os->Close());
    return arrow::Status::OK();
  }

  arrow::Result<std::unique_ptr<Spill>> finish() override {
    if (finished_) {
      return arrow::Status::Invalid("Calling finish() on a finished CacheEvictor.");
    }
    finished_ = true;

    if (diskSpill_) {
      return std::move(diskSpill_);
    }
    // No spill on disk. Delete the empty spill file.
    if (!spillFile_.empty() && std::filesystem::exists(spillFile_)) {
      std::filesystem::remove(spillFile_);
    }

    ARROW_ASSIGN_OR_RAISE(auto spill, createSpill());
    return std::move(spill);
  }

  Evict::type evictType() override {
    return Evict::kCache;
  }

 private:
  bool finished_{false};
  std::unique_ptr<DiskSpill> diskSpill_{nullptr};
  std::unordered_map<uint32_t, std::list<std::unique_ptr<Payload>>> partitionCachedPayload_;

  arrow::Result<std::unique_ptr<InMemorySpill>> createSpill() {
    if (partitionCachedPayload_.empty()) {
      return nullptr;
    }
    auto spill = std::make_unique<InMemorySpill>(
        Spill::SpillType::kBatchedInMemorySpill,
        numPartitions_,
        options_->buffer_size,
        options_->compression_threshold,
        pool_,
        codec_,
        std::move(partitionCachedPayload_));
    partitionCachedPayload_.clear();
    return spill;
  }
};

class SpillEvictor final : public LocalPartitionWriter::LocalEvictor {
 public:
  SpillEvictor(
      uint32_t numPartitions,
      ShuffleWriterOptions* options,
      const std::string& spillFile,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec)
      : LocalPartitionWriter::LocalEvictor(numPartitions, options, spillFile, pool, codec) {}

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<Payload> payload) override {
    ScopedTimer timer(evictTime_);
    if (!opened_) {
      opened_ = true;
      ARROW_ASSIGN_OR_RAISE(os_, arrow::io::FileOutputStream::Open(spillFile_, true));
      spill_ = std::make_unique<DiskSpill>(Spill::SpillType::kSequentialSpill, numPartitions_, spillFile_);
    }

    ARROW_ASSIGN_OR_RAISE(auto start, os_->Tell());
    RETURN_NOT_OK(payload->serialize(os_.get()));
    ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());
    DEBUG_OUT << "Spilled partition " << partitionId << " file start: " << start << ", file end: " << end
              << ", file: " << spillFile_ << std::endl;
    spill_->insertPayload(
        partitionId, payload->type(), payload->numRows(), payload->isValidityBuffer(), end - start, pool_, codec_);
    return arrow::Status::OK();
  }

  arrow::Result<std::unique_ptr<Spill>> finish() override {
    if (finished_) {
      return arrow::Status::Invalid("Calling finish() on a finished SpillEvictor.");
    }
    finished_ = true;

    if (!opened_) {
      return arrow::Status::Invalid("SpillEvictor has no data spilled.");
    }
    RETURN_NOT_OK(os_->Close());
    return std::move(spill_);
  }

  arrow::Status spill() override {
    return arrow::Status::OK();
  }

  Evict::type evictType() override {
    return Evict::kSpill;
  }

 private:
  bool opened_{false};
  bool finished_{false};
  std::unique_ptr<DiskSpill> spill_{nullptr};
  std::shared_ptr<arrow::io::FileOutputStream> os_;
};

LocalPartitionWriter::LocalPartitionWriter(
    uint32_t numPartitions,
    const std::string& dataFile,
    const std::vector<std::string>& localDirs,
    ShuffleWriterOptions* options)
    : PartitionWriter(numPartitions, options), dataFile_(dataFile), localDirs_(localDirs) {
  init();
}

std::string LocalPartitionWriter::nextSpilledFileDir() {
  auto spilledFileDir = getSpilledShuffleFileDir(localDirs_[dirSelection_], subDirSelection_[dirSelection_]);
  subDirSelection_[dirSelection_] = (subDirSelection_[dirSelection_] + 1) % options_->num_sub_dirs;
  dirSelection_ = (dirSelection_ + 1) % localDirs_.size();
  return spilledFileDir;
}

arrow::Status LocalPartitionWriter::openDataFile() {
  // open data file output stream
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  ARROW_ASSIGN_OR_RAISE(fout, arrow::io::FileOutputStream::Open(dataFile_));
  if (options_->buffered_write) {
    // Output stream buffer is neither partition buffer memory nor ipc memory.
    ARROW_ASSIGN_OR_RAISE(dataFileOs_, arrow::io::BufferedOutputStream::Create(16384, options_->memory_pool, fout));
  } else {
    dataFileOs_ = fout;
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::clearResource() {
  RETURN_NOT_OK(dataFileOs_->Close());
  // When buffered_write = true, dataFileOs_->Close doesn't release underlying buffer.
  dataFileOs_.reset();
  spills_.clear();
  return arrow::Status::OK();
}

void LocalPartitionWriter::init() {
  partitionLengths_.resize(numPartitions_, 0);
  rawPartitionLengths_.resize(numPartitions_, 0);
  fs_ = std::make_shared<arrow::fs::LocalFileSystem>();

  // Shuffle the configured local directories. This prevents each task from using the same directory for spilled
  // files.
  std::random_device rd;
  std::default_random_engine engine(rd());
  std::shuffle(localDirs_.begin(), localDirs_.end(), engine);
  subDirSelection_.assign(localDirs_.size(), 0);
}

arrow::Status LocalPartitionWriter::mergeSpills(uint32_t partitionId) {
  auto spillId = 0;
  auto spillIter = spills_.begin();
  while (spillIter != spills_.end()) {
    ARROW_ASSIGN_OR_RAISE(auto st, dataFileOs_->Tell());
    if (auto groupSpill = dynamic_cast<InMemorySpill*>(spillIter->get())) {
      auto groupPayloads =
          groupSpill->grouping(partitionId, codec_ ? Payload::Type::kCompressed : Payload::Type::kUncompressed);
      while (!groupPayloads.empty()) {
        auto payload = std::move(groupPayloads.front());
        groupPayloads.pop_front();
        // May trigger spill during compression.
        auto status = payload->serialize(dataFileOs_.get());
        if (!status.ok()) {
          std::cout << "stack 0: " << boost::stacktrace::stacktrace() << std::endl;
          return status;
        }
      }
    } else {
      // Read if partition exists in the spilled file and write to the final file.
      while (auto payload = (*spillIter)->nextPayload(partitionId)) {
        auto status = payload->serialize(dataFileOs_.get());
        if (!status.ok()) {
          std::cout << "stack 1: " << boost::stacktrace::stacktrace() << std::endl;
          return status;
        }
      }
    }
    ++spillIter;
    ARROW_ASSIGN_OR_RAISE(auto ed, dataFileOs_->Tell());
    std::cout << "Partition " << partitionId << " spilled from spillResult " << spillId++ << " of bytes " << ed - st
              << std::endl;
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stop(ShuffleWriterMetrics* metrics) {
  if (stopped_) {
    return arrow::Status::OK();
  }
  stopped_ = true;

  // Open final data file.
  // If options_.buffered_write is set, it will acquire 16KB memory that might trigger spill.
  RETURN_NOT_OK(openDataFile());

  auto writeTimer = Timer();
  writeTimer.start();

  // Cached payloads.
  if (evictor_) {
    ARROW_RETURN_IF(evictor_->evictType() != Evict::kCache, arrow::Status::Invalid("Unclosed evictor."));
    ARROW_ASSIGN_OR_RAISE(auto spill, evictor_->finish());
    if (spill) {
      spills_.push_back(std::move(spill));
    }
    evictTime_ += evictor_->getEvictTime();
    evictor_ = nullptr;
  }

  int64_t endInFinalFile = 0;
  DEBUG_OUT << "Total spills: " << spills_.size() << std::endl;
  // Iterator over pid.
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    // Record start offset.
    auto startInFinalFile = endInFinalFile;
    // Iterator over all spilled files.
    RETURN_NOT_OK(mergeSpills(pid));
    if (merger_) {
      ARROW_ASSIGN_OR_RAISE(auto merged, merger_->finish(pid, false));
      if (merged) {
        RETURN_NOT_OK((*merged)->serialize(dataFileOs_.get()));
      }
    }
    ARROW_ASSIGN_OR_RAISE(endInFinalFile, dataFileOs_->Tell());
    if (endInFinalFile != startInFinalFile && options_->write_eos) {
      // Write EOS if any payload written.
      int64_t bytes;
      RETURN_NOT_OK(writeEos(dataFileOs_.get(), &bytes));
      endInFinalFile += bytes;
    }
    partitionLengths_[pid] = endInFinalFile - startInFinalFile;
    DEBUG_OUT << "Partition " << pid << " partition length " << partitionLengths_[pid] << std::endl;
  }

  for (const auto& spill : spills_) {
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      if (spill->hasNextPayload(pid)) {
        return arrow::Status::Invalid("Merging from spill is not exhausted.");
      }
    }
  }

  writeTimer.stop();
  writeTime_ = writeTimer.realTimeUsed();
  ARROW_ASSIGN_OR_RAISE(totalBytesWritten_, dataFileOs_->Tell());

  // Close Final file, Clear buffered resources.
  RETURN_NOT_OK(clearResource());
  // Populate shuffle writer metrics.
  RETURN_NOT_OK(populateMetrics(metrics));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::requestEvict(Evict::type evictType) {
  if (evictor_ && evictor_->evictType() == evictType) {
    return arrow::Status::OK();
  }
  if (evictor_) {
    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(spills_.back(), evictor_->finish());
    evictTime_ += evictor_->getEvictTime();
    evictor_ = nullptr;
  }

  ARROW_ASSIGN_OR_RAISE(auto spilledFile, createTempShuffleFile(nextSpilledFileDir()));
  ARROW_ASSIGN_OR_RAISE(evictor_, createEvictor(evictType, spilledFile));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::evictMerged() {
  if (merger_) {
    RETURN_NOT_OK(requestEvict(Evict::kCache));
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      ARROW_ASSIGN_OR_RAISE(auto merged, merger_->finish(pid, true));
      if (merged.has_value()) {
        RETURN_NOT_OK(evictor_->evict(pid, std::move(*merged)));
      }
    }
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::spill() {
  RETURN_NOT_OK(evictMerged());
  if (evictor_) {
    RETURN_NOT_OK(evictor_->spill());
    ARROW_ASSIGN_OR_RAISE(auto spill, evictor_->finish());
    if (spill) {
      spills_.push_back(std::move(spill));
    }
    evictTime_ += evictor_->getEvictTime();
    evictor_ = nullptr;
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::evict(
    uint32_t partitionId,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    const std::vector<bool>* isValidityBuffer,
    bool reuseBuffers,
    Evict::type evictType,
    bool hasComplexType) {
  rawPartitionLengths_[partitionId] += getBufferSize(buffers);

  if (evictType == Evict::kSpill) {
    RETURN_NOT_OK(requestEvict(evictType));
    ARROW_ASSIGN_OR_RAISE(
        auto payload,
        BlockPayload::fromBuffers(
            Payload::kUncompressed, numRows, std::move(buffers), isValidityBuffer, payloadPool_.get(), nullptr));
    RETURN_NOT_OK(evictor_->evict(partitionId, std::move(payload)));
    return arrow::Status::OK();
  }

  if (!merger_) {
    merger_ =
        std::make_unique<PayloadMerger>(options_, payloadPool_.get(), codec_ ? codec_.get() : nullptr, hasComplexType);
  }
  ARROW_ASSIGN_OR_RAISE(
      auto merged, merger_->merge(partitionId, numRows, std::move(buffers), isValidityBuffer, reuseBuffers));
  if (merged.has_value()) {
    RETURN_NOT_OK(requestEvict(evictType));
    RETURN_NOT_OK(evictor_->evict(partitionId, std::move(*merged)));
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::populateMetrics(ShuffleWriterMetrics* metrics) {
  metrics->totalCompressTime += compressTime_;
  metrics->totalEvictTime += evictTime_;
  metrics->totalWriteTime += writeTime_;
  metrics->totalBytesEvicted += totalBytesEvicted_;
  metrics->totalBytesWritten += totalBytesWritten_;
  metrics->partitionLengths = std::move(partitionLengths_);
  metrics->rawPartitionLengths = std::move(rawPartitionLengths_);
  return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<LocalPartitionWriter::LocalEvictor>> LocalPartitionWriter::createEvictor(
    Evict::type evictType,
    const std::string& spillFile) {
  switch (evictType) {
    case Evict::kSpill:
      return std::make_unique<SpillEvictor>(numPartitions_, options_, spillFile, payloadPool_.get(), codec_.get());
    case Evict::kCache:
      return std::make_unique<CacheEvictor>(numPartitions_, options_, spillFile, payloadPool_.get(), codec_.get());
    default:
      return arrow::Status::Invalid("Cannot create Evictor from type .", std::to_string(evictType));
  }
}
} // namespace gluten
