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

#include "operators/shuffle/splitter.h"

#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>
#include <immintrin.h>
#include <x86intrin.h>

#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "compute/protobuf_utils.h"
#include "operators/shuffle/utils.h"
#include "utils/macros.h"

namespace gluten {
namespace shuffle {
using arrow::internal::checked_cast;

#ifndef SPLIT_BUFFER_SIZE
// by default, allocate 8M block, 2M page size
#define SPLIT_BUFFER_SIZE 16 * 1024 * 1024
#endif

template <typename T>
std::string __m128i_toString(const __m128i var) {
  std::stringstream sstr;
  T values[16 / sizeof(T)];
  std::memcpy(values, &var, sizeof(values)); // See discussion below
  if (sizeof(T) == 1) {
    for (unsigned int i = 0; i < sizeof(__m128i); i++) { // C++11: Range for
                                                         // also possible
      sstr << std::hex << (int)values[i] << " " << std::dec;
    }
  } else {
    for (unsigned int i = 0; i < sizeof(__m128i) / sizeof(T);
         i++) { // C++11: Range for also possible
      sstr << std::hex << values[i] << " " << std::dec;
    }
  }
  return sstr.str();
}

SplitOptions SplitOptions::Defaults() {
  return SplitOptions();
}

class Splitter::PartitionWriter {
 public:
  explicit PartitionWriter(Splitter* splitter, int32_t partition_id)
      : splitter_(splitter), partition_id_(partition_id) {}

  arrow::Status Spill() {
#ifndef SKIPWRITE
    RETURN_NOT_OK(EnsureOpened());
#endif
    RETURN_NOT_OK(
        WriteRecordBatchPayload(spilled_file_os_.get(), partition_id_));
    ClearCache();
    return arrow::Status::OK();
  }

  arrow::Status WriteCachedRecordBatchAndClose() {
    const auto& data_file_os = splitter_->data_file_os_;
    ARROW_ASSIGN_OR_RAISE(auto before_write, data_file_os->Tell());

    if (splitter_->options_.write_schema) {
      RETURN_NOT_OK(WriteSchemaPayload(data_file_os.get()));
    }

    if (spilled_file_opened_) {
      RETURN_NOT_OK(spilled_file_os_->Close());
      RETURN_NOT_OK(MergeSpilled());
    } else {
      if (splitter_->partition_cached_recordbatch_size_[partition_id_] == 0) {
        return arrow::Status::Invalid("Partition writer got empty partition");
      }
    }

    RETURN_NOT_OK(WriteRecordBatchPayload(data_file_os.get(), partition_id_));
    RETURN_NOT_OK(WriteEOS(data_file_os.get()));
    ClearCache();

    ARROW_ASSIGN_OR_RAISE(auto after_write, data_file_os->Tell());
    partition_length = after_write - before_write;

    return arrow::Status::OK();
  }

  // metrics
  int64_t bytes_spilled = 0;
  int64_t partition_length = 0;
  int64_t compress_time = 0;

 private:
  arrow::Status EnsureOpened() {
    if (!spilled_file_opened_) {
      ARROW_ASSIGN_OR_RAISE(
          spilled_file_,
          CreateTempShuffleFile(splitter_->NextSpilledFileDir()));
      ARROW_ASSIGN_OR_RAISE(
          spilled_file_os_,
          arrow::io::FileOutputStream::Open(spilled_file_, true));
      spilled_file_opened_ = true;
    }
    return arrow::Status::OK();
  }

  arrow::Status MergeSpilled() {
    ARROW_ASSIGN_OR_RAISE(
        auto spilled_file_is_,
        arrow::io::MemoryMappedFile::Open(
            spilled_file_, arrow::io::FileMode::READ));
    // copy spilled data blocks
    ARROW_ASSIGN_OR_RAISE(auto nbytes, spilled_file_is_->GetSize());
    ARROW_ASSIGN_OR_RAISE(auto buffer, spilled_file_is_->Read(nbytes));
    RETURN_NOT_OK(splitter_->data_file_os_->Write(buffer));

    // close spilled file streams and delete the file
    RETURN_NOT_OK(spilled_file_is_->Close());
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    RETURN_NOT_OK(fs->DeleteFile(spilled_file_));
    bytes_spilled += nbytes;
    return arrow::Status::OK();
  }

  arrow::Status WriteSchemaPayload(arrow::io::OutputStream* os) {
    ARROW_ASSIGN_OR_RAISE(auto payload, splitter_->GetSchemaPayload());
    int32_t metadata_length = 0; // unused
    RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
        *payload, splitter_->options_.ipc_write_options, os, &metadata_length));
    return arrow::Status::OK();
  }

  arrow::Status WriteRecordBatchPayload(
      arrow::io::OutputStream* os,
      int32_t partition_id) {
    int32_t metadata_length = 0; // unused
#ifndef SKIPWRITE
    for (auto& payload :
         splitter_->partition_cached_recordbatch_[partition_id_]) {
      RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(
          *payload,
          splitter_->options_.ipc_write_options,
          os,
          &metadata_length));
      payload = nullptr;
    }
#endif
    return arrow::Status::OK();
  }

  arrow::Status WriteEOS(arrow::io::OutputStream* os) {
    // write EOS
    constexpr int32_t kZeroLength = 0;
    RETURN_NOT_OK(os->Write(&kIpcContinuationToken, sizeof(int32_t)));
    RETURN_NOT_OK(os->Write(&kZeroLength, sizeof(int32_t)));
    return arrow::Status::OK();
  }

  void ClearCache() {
    splitter_->partition_cached_recordbatch_[partition_id_].clear();
    splitter_->partition_cached_recordbatch_size_[partition_id_] = 0;
  }

  Splitter* splitter_;
  int32_t partition_id_;
  std::string spilled_file_;
  std::shared_ptr<arrow::io::FileOutputStream> spilled_file_os_;

  bool spilled_file_opened_ = false;
};

// ----------------------------------------------------------------------
// Splitter

arrow::Result<std::shared_ptr<Splitter>> Splitter::Make(
    const std::string& short_name,
    std::shared_ptr<arrow::Schema> schema,
    int num_partitions,
    const uint8_t* expr_data,
    int expr_size,
    SplitOptions options) {
  if (short_name == "hash") {
    if (expr_data == nullptr || expr_size == 0) {
      return arrow::Status::Invalid("Invalid expression data or size.");
    }
    substrait::Rel subRel;
    ParseProtobuf(expr_data, expr_size, &subRel);
    return HashSplitter::Create(
        num_partitions, std::move(schema), subRel, std::move(options));
  } else if (short_name == "rr") {
    return RoundRobinSplitter::Create(
        num_partitions, std::move(schema), std::move(options));
  } else if (short_name == "range") {
    return FallbackRangeSplitter::Create(
        num_partitions, std::move(schema), std::move(options));
  } else if (short_name == "single") {
    return RoundRobinSplitter::Create(1, std::move(schema), std::move(options));
  }
  return arrow::Status::NotImplemented(
      "Partitioning " + short_name + " not supported yet.");
}

arrow::Result<std::shared_ptr<Splitter>> Splitter::Make(
    const std::string& short_name,
    std::shared_ptr<arrow::Schema> schema,
    int num_partitions,
    SplitOptions options) {
  return Make(
      short_name,
      std::move(schema),
      num_partitions,
      nullptr,
      0,
      std::move(options));
}

arrow::Status Splitter::Init() {
  support_avx512_ = __builtin_cpu_supports("avx512bw");
  // partition number should be less than 64k
  ARROW_CHECK_LE(num_partitions_, 64 * 1024);
  // split record batch size should be less than 32k
  ARROW_CHECK_LE(options_.buffer_size, 32 * 1024);

  const auto& fields = schema_->fields();
  ARROW_ASSIGN_OR_RAISE(column_type_id_, ToSplitterTypeId(schema_->fields()));

  partition_writer_.resize(num_partitions_);

  // pre-computed row count for each partition after the record batch split
  partition_id_cnt_.resize(num_partitions_);
  // pre-allocated buffer size for each partition, unit is row count
  partition_buffer_size_.resize(num_partitions_);

  // start index for each partition when new record batch starts to split
  partition_buffer_idx_base_.resize(num_partitions_);
  // the offset of each partition during record batch split
  partition_buffer_idx_offset_.resize(num_partitions_);

  partition_cached_recordbatch_.resize(num_partitions_);
  partition_cached_recordbatch_size_.resize(num_partitions_);
  partition_lengths_.resize(num_partitions_);
  raw_partition_lengths_.resize(num_partitions_);
  reducer_offset_offset_.resize(num_partitions_ + 1);

  std::vector<int32_t> binary_array_idx;

  for (int i = 0; i < column_type_id_.size(); ++i) {
    switch (column_type_id_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id:
        binary_array_idx.push_back(i);
        break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id:
        list_array_idx_.push_back(i);
        break;
      case arrow::NullType::type_id:
        break;
      default:
        array_idx_.push_back(i);
        break;
    }
  }
  fixed_width_col_cnt_ = array_idx_.size();

  array_idx_.insert(
      array_idx_.end(), binary_array_idx.begin(), binary_array_idx.end());

  uint32_t array_cnt = array_idx_.size();

  partition_validity_addrs_.resize(array_cnt);
  partition_fixed_width_value_addrs_.resize(fixed_width_col_cnt_);
  partition_buffers_.resize(array_cnt);
  binary_array_empirical_size_.resize(array_cnt - fixed_width_col_cnt_, 0);
  input_has_null_.resize(array_cnt, false);
  partition_binary_addrs_.resize(binary_array_idx.size());

  std::for_each(
      partition_validity_addrs_.begin(),
      partition_validity_addrs_.end(),
      [this](std::vector<uint8_t*>& v) { v.resize(num_partitions_, nullptr); });
  std::for_each(
      partition_fixed_width_value_addrs_.begin(),
      partition_fixed_width_value_addrs_.end(),
      [this](std::vector<uint8_t*>& v) { v.resize(num_partitions_, nullptr); });
  std::for_each(
      partition_buffers_.begin(),
      partition_buffers_.end(),
      [this](std::vector<arrow::BufferVector>& v) {
        v.resize(num_partitions_);
      });

  std::for_each(
      partition_binary_addrs_.begin(),
      partition_binary_addrs_.end(),
      [this](std::vector<BinaryBuff>& v) { v.resize(num_partitions_); });

  partition_list_builders_.resize(list_array_idx_.size());
  for (auto i = 0; i < list_array_idx_.size(); ++i) {
    partition_list_builders_[i].resize(num_partitions_);
  }

  ARROW_ASSIGN_OR_RAISE(configured_dirs_, GetConfiguredLocalDirs());
  sub_dir_selection_.assign(configured_dirs_.size(), 0);

  // Both data_file and shuffle_index_file should be set through jni.
  // For test purpose, Create a temporary subdirectory in the system temporary
  // dir with prefix "columnar-shuffle"
  if (options_.data_file.length() == 0) {
    ARROW_ASSIGN_OR_RAISE(
        options_.data_file, CreateTempShuffleFile(configured_dirs_[0]));
  }

  auto& ipc_write_options = options_.ipc_write_options;
  ipc_write_options.memory_pool = options_.memory_pool.get();
  ipc_write_options.use_threads = false;

  if (options_.compression_type == arrow::Compression::LZ4_FRAME) {
    ARROW_ASSIGN_OR_RAISE(
        ipc_write_options.codec,
        arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME));
  } else {
    ipc_write_options.codec = nullptr;
  }

  // initialize tiny batch write options
  tiny_bach_write_options_ = ipc_write_options;
  tiny_bach_write_options_.codec = nullptr;

  // Allocate first buffer for split reducer
  ARROW_ASSIGN_OR_RAISE(
      combine_buffer_,
      arrow::AllocateResizableBuffer(0, options_.memory_pool.get()));
  combine_buffer_->Resize(0, /*shrink_to_fit =*/false);

  return arrow::Status::OK();
}
arrow::Status Splitter::AllocateBufferFromPool(
    std::shared_ptr<arrow::Buffer>& buffer,
    uint32_t size) {
  // if size is already larger than buffer pool size, allocate it directly
  // make size 64byte aligned
  auto reminder = size & 0x3f;
  size += (64 - reminder) & ((reminder == 0) - 1);
  if (size > SPLIT_BUFFER_SIZE) {
    ARROW_ASSIGN_OR_RAISE(
        buffer,
        arrow::AllocateResizableBuffer(size, options_.memory_pool.get()));
    return arrow::Status::OK();
  } else if (combine_buffer_->capacity() - combine_buffer_->size() < size) {
    // memory pool is not enough
    ARROW_ASSIGN_OR_RAISE(
        combine_buffer_,
        arrow::AllocateResizableBuffer(
            SPLIT_BUFFER_SIZE, options_.memory_pool.get()));
    combine_buffer_->Resize(0, /*shrink_to_fit = */ false);
  }
  buffer =
      arrow::SliceMutableBuffer(combine_buffer_, combine_buffer_->size(), size);

  combine_buffer_->Resize(
      combine_buffer_->size() + size, /*shrink_to_fit = */ false);
  return arrow::Status::OK();
}

int64_t Splitter::CompressedSize(const arrow::RecordBatch& rb) {
  auto payload = std::make_shared<arrow::ipc::IpcPayload>();
  arrow::Status result;
  result = arrow::ipc::GetRecordBatchPayload(
      rb, options_.ipc_write_options, payload.get());
  if (result.ok()) {
    return payload->body_length;
  } else {
    result.UnknownError("Failed to get the compressed size.");
    return -1;
  }
}

arrow::Status Splitter::SetCompressType(
    arrow::Compression::type compressed_type) {
  if (compressed_type == arrow::Compression::LZ4_FRAME) {
    ARROW_ASSIGN_OR_RAISE(
        options_.ipc_write_options.codec,
        arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME));
  } else {
    options_.ipc_write_options.codec = nullptr;
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::Split(const arrow::RecordBatch& rb) {
  EVAL_START("split", options_.thread_id)
  RETURN_NOT_OK(ComputeAndCountPartitionId(rb));
  RETURN_NOT_OK(DoSplit(rb));
  EVAL_END("split", options_.thread_id, options_.task_attempt_id)
  return arrow::Status::OK();
}

arrow::Status Splitter::Stop() {
  EVAL_START("write", options_.thread_id)
  // open data file output stream
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  ARROW_ASSIGN_OR_RAISE(
      fout, arrow::io::FileOutputStream::Open(options_.data_file, true));
  if (options_.buffered_write) {
    ARROW_ASSIGN_OR_RAISE(
        data_file_os_,
        arrow::io::BufferedOutputStream::Create(
            16384, options_.memory_pool.get(), fout));
  } else {
    data_file_os_ = fout;
  }

  // stop PartitionWriter and collect metrics
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    RETURN_NOT_OK(CacheRecordBatch(pid, true));
    if (partition_cached_recordbatch_size_[pid] > 0) {
      if (partition_writer_[pid] == nullptr) {
        partition_writer_[pid] = std::make_shared<PartitionWriter>(this, pid);
      }
    }
    if (partition_writer_[pid] != nullptr) {
      const auto& writer = partition_writer_[pid];
      TIME_NANO_OR_RAISE(
          total_write_time_, writer->WriteCachedRecordBatchAndClose());
      partition_lengths_[pid] = writer->partition_length;
      total_bytes_written_ += writer->partition_length;
      total_bytes_spilled_ += writer->bytes_spilled;
      total_compress_time_ += writer->compress_time;
    } else {
      partition_lengths_[pid] = 0;
    }
  }
  this->combine_buffer_.reset();
  this->schema_payload_.reset();
  partition_buffers_.clear();

  // close data file output Stream
  RETURN_NOT_OK(data_file_os_->Close());

  EVAL_END("write", options_.thread_id, options_.task_attempt_id)

  return arrow::Status::OK();
}
int64_t batch_nbytes(const arrow::RecordBatch& batch) {
  int64_t accumulated = 0L;

  for (const auto& array : batch.columns()) {
    if (array == nullptr || array->data() == nullptr) {
      continue;
    }
    for (const auto& buf : array->data()->buffers) {
      if (buf == nullptr) {
        continue;
      }
      accumulated += buf->size();
    }
  }
  return accumulated;
}

int64_t batch_nbytes(std::shared_ptr<arrow::RecordBatch> batch) {
  if (batch == nullptr) {
    return 0;
  }
  return batch_nbytes(*batch);
}

arrow::Status Splitter::CacheRecordBatch(
    int32_t partition_id,
    bool reset_buffers) {
  static int printed = 0;

  if (partition_buffer_idx_base_[partition_id] > 0) {
    // already filled
    auto fixed_width_idx = 0;
    auto binary_idx = 0;
    auto list_idx = 0;
    auto num_fields = schema_->num_fields();
    auto num_rows = partition_buffer_idx_base_[partition_id];
    auto buffer_sizes = 0;
    int8_t sizeof_binary_offset = -1;
    std::vector<std::shared_ptr<arrow::Array>> arrays(num_fields);
    for (int i = 0; i < num_fields; ++i) {
      switch (column_type_id_[i]->id()) {
        case arrow::BinaryType::type_id:
        case arrow::StringType::type_id:
          sizeof_binary_offset = sizeof(arrow::BinaryType::offset_type);
        case arrow::LargeBinaryType::type_id:
        case arrow::LargeStringType::type_id: {
          if (sizeof_binary_offset == -1)
            sizeof_binary_offset = sizeof(arrow::LargeBinaryType::offset_type);

          auto buffers = partition_buffers_[fixed_width_col_cnt_ + binary_idx]
                                           [partition_id];
          // validity buffer
          if (buffers[0] != nullptr) {
            buffers[0] = arrow::SliceBuffer(
                buffers[0], 0, arrow::bit_util::BytesForBits(num_rows));
          }
          // offset buffer
          if (buffers[1] != nullptr) {
            buffers[1] = arrow::SliceBuffer(
                buffers[1], 0, (num_rows + 1) * sizeof_binary_offset);
          }
          // value buffer
          if (buffers[2] != nullptr) {
            ARROW_CHECK_NE(buffers[1], nullptr);
            buffers[2] = arrow::SliceBuffer(
                buffers[2],
                0,
                sizeof_binary_offset == 4
                    ? reinterpret_cast<const arrow::BinaryType::offset_type*>(
                          buffers[1]->data())[num_rows]
                    : reinterpret_cast<
                          const arrow::LargeBinaryType::offset_type*>(
                          buffers[1]->data())[num_rows]);
          }

          arrays[i] = arrow::MakeArray(arrow::ArrayData::Make(
              schema_->field(i)->type(),
              num_rows,
              {buffers[0], buffers[1], buffers[2]}));

          uint64_t dst_offset0 = sizeof_binary_offset == 4
              ? reinterpret_cast<const arrow::BinaryType::offset_type*>(
                    buffers[1]->data())[0]
              : reinterpret_cast<const arrow::LargeBinaryType::offset_type*>(
                    buffers[1]->data())[0];
          ARROW_CHECK_EQ(dst_offset0, 0);

          if (reset_buffers) {
            partition_validity_addrs_[fixed_width_col_cnt_ + binary_idx]
                                     [partition_id] = nullptr;
            partition_binary_addrs_[binary_idx][partition_id] = BinaryBuff();
            partition_buffers_[fixed_width_col_cnt_ + binary_idx][partition_id]
                .clear();
          } else {
            // reset the offset
            partition_binary_addrs_[binary_idx][partition_id].value_offset = 0;
          }
          binary_idx++;
          break;
        }
        case arrow::StructType::type_id:
        case arrow::MapType::type_id:
        case arrow::LargeListType::type_id:
        case arrow::ListType::type_id: {
          auto& builder = partition_list_builders_[list_idx][partition_id];
          if (reset_buffers) {
            RETURN_NOT_OK(builder->Finish(&arrays[i]));
            builder->Reset();
          } else {
            RETURN_NOT_OK(builder->Finish(&arrays[i]));
            builder->Reset();
            RETURN_NOT_OK(builder->Reserve(num_rows));
          }
          list_idx++;
          break;
        }
        case arrow::NullType::type_id: {
          arrays[i] = arrow::MakeArray(arrow::ArrayData::Make(
              arrow::null(), num_rows, {nullptr, nullptr}, num_rows));
          break;
        }
        default: {
          auto buffers = partition_buffers_[fixed_width_idx][partition_id];
          if (buffers[0] != nullptr) {
            buffers[0] = arrow::SliceBuffer(
                buffers[0], 0, arrow::bit_util::BytesForBits(num_rows));
          }
          if (buffers[1] != nullptr) {
            if (column_type_id_[i]->id() == arrow::BooleanType::type_id)
              buffers[1] = arrow::SliceBuffer(
                  buffers[1], 0, arrow::bit_util::BytesForBits(num_rows));
            else
              buffers[1] = arrow::SliceBuffer(
                  buffers[1],
                  0,
                  num_rows * (arrow::bit_width(column_type_id_[i]->id()) >> 3));
          }

          arrays[i] = arrow::MakeArray(arrow::ArrayData::Make(
              schema_->field(i)->type(), num_rows, {buffers[0], buffers[1]}));
          if (reset_buffers) {
            partition_validity_addrs_[fixed_width_idx][partition_id] = nullptr;
            partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] =
                nullptr;
            partition_buffers_[fixed_width_idx][partition_id].clear();
          }
          fixed_width_idx++;
          break;
        }
      }
    }
    auto batch = arrow::RecordBatch::Make(schema_, num_rows, std::move(arrays));
    int64_t raw_size = batch_nbytes(batch);

    raw_partition_lengths_[partition_id] += raw_size;
    auto payload = std::make_shared<arrow::ipc::IpcPayload>();
#ifndef SKIPCOMPRESS
    if (num_rows <= options_.batch_compress_threshold) {
      TIME_NANO_OR_RAISE(
          total_compress_time_,
          arrow::ipc::GetRecordBatchPayload(
              *batch, tiny_bach_write_options_, payload.get()));
    } else {
      TIME_NANO_OR_RAISE(
          total_compress_time_,
          arrow::ipc::GetRecordBatchPayload(
              *batch, options_.ipc_write_options, payload.get()));
    }
#else
    // for test reason
    TIME_NANO_OR_RAISE(
        total_compress_time_,
        arrow::ipc::GetRecordBatchPayload(
            *batch, tiny_bach_write_options_, payload.get()));
#endif

    partition_cached_recordbatch_size_[partition_id] += payload->body_length;
    partition_cached_recordbatch_[partition_id].push_back(std::move(payload));
    partition_buffer_idx_base_[partition_id] = 0;
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::AllocatePartitionBuffers(
    int32_t partition_id,
    int32_t new_size) {
  // try to allocate new
  auto num_fields = schema_->num_fields();
  auto fixed_width_idx = 0;
  auto binary_idx = 0;
  auto list_idx = 0;
  auto total_size = 0;
  int8_t sizeof_binary_offset = -1;

  std::vector<std::shared_ptr<arrow::ArrayBuilder>> new_list_builders;

  for (auto i = 0; i < num_fields; ++i) {
    switch (column_type_id_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
        sizeof_binary_offset = sizeof(arrow::StringType::offset_type);
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id: {
        if (sizeof_binary_offset == -1) {
          sizeof_binary_offset = sizeof(arrow::LargeStringType::offset_type);
        }

        std::shared_ptr<arrow::Buffer> offset_buffer;
        std::shared_ptr<arrow::Buffer> validity_buffer = nullptr;
        auto value_buf_size =
            binary_array_empirical_size_[binary_idx] * new_size + 1024;
        ARROW_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Buffer> value_buffer,
            arrow::AllocateResizableBuffer(
                value_buf_size, options_.memory_pool.get()));
        ARROW_RETURN_NOT_OK(AllocateBufferFromPool(
            offset_buffer, new_size * sizeof_binary_offset + 1));
        // set the first offset to 0
        uint8_t* offsetaddr = offset_buffer->mutable_data();
        memset(offsetaddr, 0, 8);

        partition_binary_addrs_[binary_idx][partition_id] = BinaryBuff(
            value_buffer->mutable_data(),
            offset_buffer->mutable_data(),
            value_buf_size);

        if (input_has_null_[fixed_width_col_cnt_ + binary_idx]) {
          ARROW_RETURN_NOT_OK(AllocateBufferFromPool(
              validity_buffer, arrow::bit_util::BytesForBits(new_size)));
          // initialize all true once allocated
          memset(
              validity_buffer->mutable_data(),
              0xff,
              validity_buffer->capacity());
          partition_validity_addrs_[fixed_width_col_cnt_ + binary_idx]
                                   [partition_id] =
                                       validity_buffer->mutable_data();
        } else {
          partition_validity_addrs_[fixed_width_col_cnt_ + binary_idx]
                                   [partition_id] = nullptr;
        }
        partition_buffers_[fixed_width_col_cnt_ + binary_idx][partition_id] = {
            std::move(validity_buffer),
            std::move(offset_buffer),
            std::move(value_buffer)};
        binary_idx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id: {
        std::unique_ptr<arrow::ArrayBuilder> array_builder;
        RETURN_NOT_OK(MakeBuilder(
            options_.memory_pool.get(), column_type_id_[i], &array_builder));
        assert(array_builder != nullptr);
        RETURN_NOT_OK(array_builder->Reserve(new_size));
        partition_list_builders_[list_idx][partition_id] =
            std::move(array_builder);
        list_idx++;
        break;
      }
      case arrow::NullType::type_id:
        break;
      default: {
        std::shared_ptr<arrow::Buffer> value_buffer;
        std::shared_ptr<arrow::Buffer> validity_buffer = nullptr;
        if (column_type_id_[i]->id() == arrow::BooleanType::type_id) {
          ARROW_RETURN_NOT_OK(AllocateBufferFromPool(
              value_buffer, arrow::bit_util::BytesForBits(new_size)));
        } else {
          ARROW_RETURN_NOT_OK(AllocateBufferFromPool(
              value_buffer,
              new_size * (arrow::bit_width(column_type_id_[i]->id()) >> 3)));
        }
        partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] =
            value_buffer->mutable_data();

        if (input_has_null_[fixed_width_idx]) {
          ARROW_RETURN_NOT_OK(AllocateBufferFromPool(
              validity_buffer, arrow::bit_util::BytesForBits(new_size)));
          // initialize all true once allocated
          memset(
              validity_buffer->mutable_data(),
              0xff,
              validity_buffer->capacity());
          partition_validity_addrs_[fixed_width_idx][partition_id] =
              validity_buffer->mutable_data();
        } else {
          partition_validity_addrs_[fixed_width_idx][partition_id] = nullptr;
        }
        partition_buffers_[fixed_width_idx][partition_id] = {
            std::move(validity_buffer), std::move(value_buffer)};
        fixed_width_idx++;
        break;
      }
    }
  }

  partition_buffer_size_[partition_id] = new_size;
  return arrow::Status::OK();
}

arrow::Status Splitter::AllocateNew(int32_t partition_id, int32_t new_size) {
  auto status = AllocatePartitionBuffers(partition_id, new_size);
  int32_t retry = 0;
  while (status.IsOutOfMemory() && retry < 3) {
    // retry allocate
    std::cout << status.ToString() << std::endl
              << std::to_string(++retry)
              << " retry to allocate new buffer for partition "
              << std::to_string(partition_id) << std::endl;
    int64_t spilled_size;
    ARROW_ASSIGN_OR_RAISE(
        auto partition_to_spill, SpillLargestPartition(&spilled_size));
    if (partition_to_spill == -1) {
      std::cout << "Failed to allocate new buffer for partition "
                << std::to_string(partition_id)
                << ". No partition buffer to spill." << std::endl;
      return status;
    }
    status = AllocatePartitionBuffers(partition_id, new_size);
  }
  if (status.IsOutOfMemory()) {
    std::cout << "Failed to allocate new buffer for partition "
              << std::to_string(partition_id) << ". Out of memory."
              << std::endl;
  }
  return status;
}

// call from memory management
arrow::Status Splitter::SpillFixedSize(int64_t size, int64_t* actual) {
  int64_t current_spilled = 0L;
  int32_t try_count = 0;
  while (current_spilled < size && try_count < 5) {
    try_count++;
    int64_t single_call_spilled;
    ARROW_ASSIGN_OR_RAISE(
        int32_t spilled_partition_id,
        SpillLargestPartition(&single_call_spilled))
    if (spilled_partition_id == -1) {
      break;
    }
    current_spilled += single_call_spilled;
  }
  *actual = current_spilled;
  return arrow::Status::OK();
}

arrow::Status Splitter::SpillPartition(int32_t partition_id) {
  if (partition_writer_[partition_id] == nullptr) {
    partition_writer_[partition_id] =
        std::make_shared<PartitionWriter>(this, partition_id);
  }
  TIME_NANO_OR_RAISE(
      total_spill_time_, partition_writer_[partition_id]->Spill());

  // reset validity buffer after spill
  std::for_each(
      partition_buffers_.begin(),
      partition_buffers_.end(),
      [partition_id](std::vector<arrow::BufferVector>& bufs) {
        if (bufs[partition_id][0] != nullptr) {
          // initialize all true once allocated
          auto addr = bufs[partition_id][0]->mutable_data();
          memset(addr, 0xff, bufs[partition_id][0]->capacity());
        }
      });

  return arrow::Status::OK();
}

arrow::Result<int32_t> Splitter::SpillLargestPartition(int64_t* size) {
  // spill the largest partition
  auto max_size = 0;
  int32_t partition_to_spill = -1;
  for (auto i = 0; i < num_partitions_; ++i) {
    if (partition_cached_recordbatch_size_[i] > max_size) {
      max_size = partition_cached_recordbatch_size_[i];
      partition_to_spill = i;
    }
  }
  if (partition_to_spill != -1) {
    RETURN_NOT_OK(SpillPartition(partition_to_spill));
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "Spilled partition " << std::to_string(partition_to_spill)
              << ", " << std::to_string(max_size) << " bytes released"
              << std::endl;
#endif
    *size = max_size;
  } else {
    *size = 0;
  }
  return partition_to_spill;
}
Splitter::row_offset_type Splitter::CalculateSplitBatchSize(
    const arrow::RecordBatch& rb) {
  uint32_t size_per_row = 0;
  auto num_rows = rb.num_rows();
  for (int i = fixed_width_col_cnt_; i < array_idx_.size(); ++i) {
    if (ARROW_PREDICT_FALSE(
            binary_array_empirical_size_[i - fixed_width_col_cnt_] == 0)) {
      auto arr = rb.column_data(array_idx_[i]);
      auto cid = rb.column(array_idx_[i])->type_id();
      ARROW_CHECK_EQ(arr->buffers.size(), 3);
      // offset array_data
      if (ARROW_PREDICT_TRUE(arr->buffers[1] != nullptr)) {
        auto offsetbuf = arr->buffers[1]->data();
        uint64_t length;
        switch (column_type_id_[array_idx_[i]]->id()) {
          case arrow::BinaryType::type_id:
          case arrow::StringType::type_id:
            length = reinterpret_cast<const arrow::StringType::offset_type*>(
                         offsetbuf)[num_rows] -
                reinterpret_cast<const arrow::StringType::offset_type*>(
                         offsetbuf)[0];
            break;
          case arrow::LargeBinaryType::type_id:
          case arrow::LargeStringType::type_id:
            length =
                reinterpret_cast<const arrow::LargeStringType::offset_type*>(
                    offsetbuf)[num_rows] -
                reinterpret_cast<const arrow::LargeStringType::offset_type*>(
                    offsetbuf)[0];
            break;
        }
        binary_array_empirical_size_[i - fixed_width_col_cnt_] =
            length % num_rows == 0 ? length / num_rows : length / num_rows + 1;
        // std::cout << "avg str length col = " << i - fixed_width_col_cnt_ << "
        // len = "
        // << binary_array_empirical_size_[i - fixed_width_col_cnt_] <<
        // std::endl;
      }
    }
  }
  size_per_row = std::accumulate(
      binary_array_empirical_size_.begin(),
      binary_array_empirical_size_.end(),
      0);

  for (auto col = 0; col < array_idx_.size(); ++col) {
    auto col_idx = array_idx_[col];
    if (col_idx < fixed_width_col_cnt_)
      size_per_row += arrow::bit_width(column_type_id_[col_idx]->id()) >> 3;
  }

  int64_t prealloc_row_cnt = options_.offheap_per_task > 0 && size_per_row > 0
      ? options_.offheap_per_task / size_per_row / num_partitions_ >> 2
      : options_.buffer_size;
  prealloc_row_cnt = std::min(prealloc_row_cnt, (int64_t)options_.buffer_size);

  return (row_offset_type)prealloc_row_cnt;
}

arrow::Status Splitter::DoSplit(const arrow::RecordBatch& rb) {
  // buffer is allocated less than 64K
  // ARROW_CHECK_LE(rb.num_rows(),64*1024);

  reducer_offsets_.resize(rb.num_rows());

  reducer_offset_offset_[0] = 0;
  for (auto pid = 1; pid <= num_partitions_; pid++) {
    reducer_offset_offset_[pid] =
        reducer_offset_offset_[pid - 1] + partition_id_cnt_[pid - 1];
  }
  for (auto row = 0; row < rb.num_rows(); row++) {
    auto pid = partition_id_[row];
    reducer_offsets_[reducer_offset_offset_[pid]] = row;
    _mm_prefetch(
        reducer_offsets_.data() + reducer_offset_offset_[pid] + 32,
        _MM_HINT_T0);
    reducer_offset_offset_[pid]++;
  }
  std::transform(
      reducer_offset_offset_.begin(),
      std::prev(reducer_offset_offset_.end()),
      partition_id_cnt_.begin(),
      reducer_offset_offset_.begin(),
      [](row_offset_type x, row_offset_type y) { return x - y; });
  // for the first input record batch, scan binary arrays and large binary
  // arrays to get their empirical sizes

  for (auto col = 0; col < array_idx_.size(); ++col) {
    auto col_idx = array_idx_[col];
    if (col_idx < fixed_width_col_cnt_)
      // check input_has_null_[col] is cheaper than GetNullCount()
      //  once input_has_null_ is set to true, we didn't reset it after spill
      if (input_has_null_[col] == false &&
          rb.column_data(col_idx)->GetNullCount() != 0) {
        input_has_null_[col] = true;
      }
  }

  // prepare partition buffers and spill if necessary
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    if (partition_id_cnt_[pid] > 0) {
      // make sure the size to be allocated is larger than the size to be filled
      if (partition_buffer_size_[pid] == 0) {
        // allocate buffer if it's not yet allocated
        auto new_size =
            std::max(CalculateSplitBatchSize(rb), partition_id_cnt_[pid]);
        RETURN_NOT_OK(AllocatePartitionBuffers(pid, new_size));
      } else if (
          partition_buffer_idx_base_[pid] + partition_id_cnt_[pid] >
          partition_buffer_size_[pid]) {
        auto new_size =
            std::max(CalculateSplitBatchSize(rb), partition_id_cnt_[pid]);
        // if the size to be filled + allready filled > the buffer size, need to
        // allocate new buffer
        if (options_.prefer_spill) {
          // if prefer_spill is set, spill current record batch, we may reuse
          // the buffers

          if (new_size > partition_buffer_size_[pid]) {
            // if the partition size after split is already larger than
            // allocated buffer size, need reallocate
            RETURN_NOT_OK(CacheRecordBatch(pid, /*reset_buffers = */ true));
            // splill immediately
            RETURN_NOT_OK(SpillPartition(pid));
            RETURN_NOT_OK(AllocatePartitionBuffers(pid, new_size));
          } else {
            // partition size after split is smaller than buffer size, no need
            // to reset buffer, reuse it.
            RETURN_NOT_OK(CacheRecordBatch(pid, /*reset_buffers = */ false));
            RETURN_NOT_OK(SpillPartition(pid));
          }
        } else {
          // if prefer_spill is disabled, cache the record batch
          RETURN_NOT_OK(CacheRecordBatch(pid, /*reset_buffers = */ true));
          // allocate partition buffer with retries
          RETURN_NOT_OK(AllocateNew(pid, new_size));
        }
      }
    }
  }
  // now start to split the record batch
  RETURN_NOT_OK(SplitFixedWidthValueBuffer(rb));
  RETURN_NOT_OK(SplitValidityBuffer(rb));
  RETURN_NOT_OK(SplitBinaryArray(rb));
  RETURN_NOT_OK(SplitListArray(rb));

  // update partition buffer base after split
  for (auto pid = 0; pid < num_partitions_; ++pid) {
    partition_buffer_idx_base_[pid] += partition_id_cnt_[pid];
  }

  return arrow::Status::OK();
}
template <typename T>
arrow::Status Splitter::SplitFixedType(
    const uint8_t* src_addr,
    const std::vector<uint8_t*>& dst_addrs) {
  std::transform(
      dst_addrs.begin(),
      dst_addrs.end(),
      partition_buffer_idx_base_.begin(),
      partition_buffer_idx_offset_.begin(),
      [](uint8_t* x, row_offset_type y) { return x + y * sizeof(T); });
  // assume batch size = 32k; reducer# = 4K; row/reducer = 8
  for (auto pid = 0; pid < num_partitions_; pid++) {
    auto dst_pid_base =
        reinterpret_cast<T*>(partition_buffer_idx_offset_[pid]); /*32k*/
    auto r = reducer_offset_offset_[pid]; /*8k*/
    auto size = reducer_offset_offset_[pid + 1];
    for (r; r < size; r++) {
      auto src_offset = reducer_offsets_[r]; /*16k*/
      *dst_pid_base = reinterpret_cast<const T*>(src_addr)[src_offset]; /*64k*/
      _mm_prefetch(src_addr + src_offset * sizeof(T) + 64, _MM_HINT_T2);
      dst_pid_base += 1;
    }
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::SplitFixedWidthValueBuffer(
    const arrow::RecordBatch& rb) {
  const auto num_rows = rb.num_rows();
  int64_t row;
  std::vector<row_offset_type> partition_buffer_idx_offset;

  for (auto col = 0; col < fixed_width_col_cnt_; ++col) {
    const auto& dst_addrs = partition_fixed_width_value_addrs_[col];
    auto col_idx = array_idx_[col];
    auto src_addr =
        const_cast<uint8_t*>(rb.column_data(col_idx)->buffers[1]->data());

    switch (arrow::bit_width(column_type_id_[col_idx]->id())) {
      case 8:
        SplitFixedType<uint8_t>(src_addr, dst_addrs);
        break;
      case 16:
        SplitFixedType<uint16_t>(src_addr, dst_addrs);
        break;
      case 32:
        SplitFixedType<uint32_t>(src_addr, dst_addrs);
        break;
      case 64:
#ifdef PROCESSAVX
        std::transform(
            dst_addrs.begin(),
            dst_addrs.end(),
            partition_buffer_idx_base_.begin(),
            partition_buffer_idx_offset_.begin(),
            [](uint8_t* x, row_offset_type y) {
              return x + y * sizeof(uint64_t);
            });
        for (auto pid = 0; pid < num_partitions_; pid++) {
          auto dst_pid_base = reinterpret_cast<uint64_t*>(
              partition_buffer_idx_offset_[pid]); /*32k*/
          auto r = reducer_offset_offset_[pid]; /*8k*/
          auto size = reducer_offset_offset_[pid + 1];
#if 1
          for (r; r < size && (((uint64_t)dst_pid_base & 0x1f) > 0); r++) {
            auto src_offset = reducer_offsets_[r]; /*16k*/
            *dst_pid_base =
                reinterpret_cast<uint64_t*>(src_addr)[src_offset]; /*64k*/
            _mm_prefetch(
                &(src_addr)[src_offset * sizeof(uint64_t) + 64], _MM_HINT_T2);
            dst_pid_base += 1;
          }
#if 0
          for (r; r+4<size; r+=4)                              
          {                                                                                    
            auto src_offset = reducer_offsets_[r];                                 /*16k*/ 
            __m128i src_ld = _mm_loadl_epi64((__m128i*)(&reducer_offsets_[r]));    
            __m128i src_offset_4x = _mm_cvtepu16_epi32(src_ld);
            
            __m256i src_4x = _mm256_i32gather_epi64((const long long int*)src_addr,src_offset_4x,8);
            //_mm256_store_si256((__m256i*)dst_pid_base,src_4x); 
            _mm_stream_si128((__m128i*)dst_pid_base,src_2x);
                                                         
            _mm_prefetch(&(src_addr)[(uint32_t)reducer_offsets_[r]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            _mm_prefetch(&(src_addr)[(uint32_t)reducer_offsets_[r+1]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            _mm_prefetch(&(src_addr)[(uint32_t)reducer_offsets_[r+2]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            _mm_prefetch(&(src_addr)[(uint32_t)reducer_offsets_[r+3]*sizeof(uint64_t)+64], _MM_HINT_T2);              
            dst_pid_base+=4;                                                                   
          }
#endif
          for (r; r + 2 < size; r += 2) {
            __m128i src_offset_2x =
                _mm_cvtsi32_si128(*((int32_t*)(reducer_offsets_.data() + r)));
            src_offset_2x = _mm_shufflelo_epi16(src_offset_2x, 0x98);

            __m128i src_2x = _mm_i32gather_epi64(
                (const long long int*)src_addr, src_offset_2x, 8);
            _mm_store_si128((__m128i*)dst_pid_base, src_2x);
            //_mm_stream_si128((__m128i*)dst_pid_base,src_2x);

            _mm_prefetch(
                &(src_addr)
                    [(uint32_t)reducer_offsets_[r] * sizeof(uint64_t) + 64],
                _MM_HINT_T2);
            _mm_prefetch(
                &(src_addr)
                    [(uint32_t)reducer_offsets_[r + 1] * sizeof(uint64_t) + 64],
                _MM_HINT_T2);
            dst_pid_base += 2;
          }
#endif
          for (r; r < size; r++) {
            auto src_offset = reducer_offsets_[r]; /*16k*/
            *dst_pid_base =
                reinterpret_cast<const uint64_t*>(src_addr)[src_offset]; /*64k*/
            _mm_prefetch(
                &(src_addr)[src_offset * sizeof(uint64_t) + 64], _MM_HINT_T2);
            dst_pid_base += 1;
          }
        }
        break;
#else
        SplitFixedType<uint64_t>(src_addr, dst_addrs);
#endif
        break;
      case 128: // arrow::Decimal128Type::type_id
        // too bad gcc generates movdqa even we use __m128i_u data type.
        // SplitFixedType<__m128i_u>(src_addr, dst_addrs);
        {
          std::transform(
              dst_addrs.begin(),
              dst_addrs.end(),
              partition_buffer_idx_base_.begin(),
              partition_buffer_idx_offset_.begin(),
              [](uint8_t* x, row_offset_type y) {
                return x + y * sizeof(__m128i_u);
              });
          // assume batch size = 32k; reducer# = 4K; row/reducer = 8
          for (auto pid = 0; pid < num_partitions_; pid++) {
            auto dst_pid_base = reinterpret_cast<__m128i_u*>(
                partition_buffer_idx_offset_[pid]); /*32k*/
            auto r = reducer_offset_offset_[pid]; /*8k*/
            auto size = reducer_offset_offset_[pid + 1];
            for (r; r < size; r++) {
              auto src_offset = reducer_offsets_[r]; /*16k*/
              __m128i value = _mm_loadu_si128(
                  reinterpret_cast<const __m128i_u*>(src_addr) + src_offset);
              _mm_storeu_si128(dst_pid_base, value);
              _mm_prefetch(
                  src_addr + src_offset * sizeof(__m128i_u) + 64, _MM_HINT_T2);
              dst_pid_base += 1;
            }
          }
        }
        break;
      case 1: // arrow::BooleanType::type_id:
        RETURN_NOT_OK(SplitBoolType(src_addr, dst_addrs));
        break;
      default:
        return arrow::Status::Invalid(
            "Column type " + schema_->field(col_idx)->type()->ToString() +
            " is not fixed width");
    }
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::SplitBoolType(
    const uint8_t* src_addr,
    const std::vector<uint8_t*>& dst_addrs) {
  // assume batch size = 32k; reducer# = 4K; row/reducer = 8
  for (auto pid = 0; pid < num_partitions_; pid++) {
    // set the last byte
    auto dstaddr = dst_addrs[pid];
    if (partition_id_cnt_[pid] > 0 && dstaddr != nullptr) {
      auto r = reducer_offset_offset_[pid]; /*8k*/
      auto size = reducer_offset_offset_[pid + 1];
      row_offset_type dst_offset = partition_buffer_idx_base_[pid];
      row_offset_type dst_offset_in_byte = (8 - (dst_offset & 0x7)) & 0x7;
      row_offset_type dst_idx_byte = dst_offset_in_byte;
      uint8_t dst = dstaddr[dst_offset >> 3];
      if (pid + 1 < num_partitions_) {
        _mm_prefetch(
            &dstaddr[partition_buffer_idx_base_[pid + 1] >> 3], _MM_HINT_T1);
      }
      for (r; r < size && dst_idx_byte > 0; r++, dst_idx_byte--) {
        auto src_offset = reducer_offsets_[r]; /*16k*/
        uint8_t src = src_addr[src_offset >> 3];
        src = src >> (src_offset & 7) |
            0xfe; // get the bit in bit 0, other bits set to 1
        src = __rolb(src, 8 - dst_idx_byte);
        dst = dst & src; // only take the useful bit.
      }
      dstaddr[dst_offset >> 3] = dst;
      if (r == size) {
        continue;
      }
      dst_offset += dst_offset_in_byte;
      // now dst_offset is 8 aligned
      for (r; r + 8 < size; r += 8) {
        uint8_t src = 0;
        auto src_offset = reducer_offsets_[r]; /*16k*/
        src = src_addr[src_offset >> 3];
        _mm_prefetch(&(src_addr)[(src_offset >> 3) + 64], _MM_HINT_T0);
        dst = src >> (src_offset & 7) |
            0xfe; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 1]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 1 |
            0xfd; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 2]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 2 |
            0xfb; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 3]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 3 |
            0xf7; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 4]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 4 |
            0xef; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 5]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 5 |
            0xdf; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 6]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 6 |
            0xbf; // get the bit in bit 0, other bits set to 1

        src_offset = reducer_offsets_[r + 7]; /*16k*/
        src = src_addr[src_offset >> 3];
        dst &= src >> (src_offset & 7) << 7 |
            0x7f; // get the bit in bit 0, other bits set to 1

        dstaddr[dst_offset >> 3] = dst;
        dst_offset += 8;
        //_mm_prefetch(dstaddr + (dst_offset >> 3) + 64, _MM_HINT_T0);
      }
      // last byte, set it to 0xff is ok
      dst = 0xff;
      dst_idx_byte = 0;
      for (r; r < size; r++, dst_idx_byte++) {
        auto src_offset = reducer_offsets_[r]; /*16k*/
        uint8_t src = src_addr[src_offset >> 3];
        src = src >> (src_offset & 7) |
            0xfe; // get the bit in bit 0, other bits set to 1
        src = __rolb(src, dst_idx_byte);
        dst = dst & src; // only take the useful bit.
      }
      dstaddr[dst_offset >> 3] = dst;
    }
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::SplitValidityBuffer(const arrow::RecordBatch& rb) {
  const auto num_rows = rb.num_rows();

  for (auto col = 0; col < array_idx_.size(); ++col) {
    auto col_idx = array_idx_[col];
    auto& dst_addrs = partition_validity_addrs_[col];
    if (rb.column_data(col_idx)->GetNullCount() > 0) {
      // there is Null count
      for (auto pid = 0; pid < num_partitions_; ++pid) {
        if (partition_id_cnt_[pid] > 0 && dst_addrs[pid] == nullptr) {
          // init bitmap if it's null, initialize the buffer as true
          auto new_size = std::max(
              partition_id_cnt_[pid], (row_offset_type)options_.buffer_size);
          std::shared_ptr<arrow::Buffer> validity_buffer;
          auto status = AllocateBufferFromPool(
              validity_buffer, arrow::bit_util::BytesForBits(new_size));
          ARROW_RETURN_NOT_OK(status);
          dst_addrs[pid] = const_cast<uint8_t*>(validity_buffer->data());
          memset(
              validity_buffer->mutable_data(),
              0xff,
              validity_buffer->capacity());
          partition_buffers_[col][pid][0] = std::move(validity_buffer);
        }
      }
      auto src_addr =
          const_cast<uint8_t*>(rb.column_data(col_idx)->buffers[0]->data());

      RETURN_NOT_OK(SplitBoolType(src_addr, dst_addrs));
    }
  }
  return arrow::Status::OK();
}

template <typename T>
arrow::Status Splitter::SplitBinaryType(
    const uint8_t* src_addr,
    const T* src_offset_addr,
    std::vector<BinaryBuff>& dst_addrs,
    const int binary_idx) {
  for (auto pid = 0; pid < num_partitions_; pid++) {
    auto dst_offset_base = reinterpret_cast<T*>(dst_addrs[pid].offsetptr) +
        partition_buffer_idx_base_[pid];
    if (pid + 1 < num_partitions_) {
      _mm_prefetch(
          reinterpret_cast<T*>(dst_addrs[pid + 1].offsetptr) +
              partition_buffer_idx_base_[pid + 1],
          _MM_HINT_T1);
      _mm_prefetch(
          dst_addrs[pid + 1].valueptr + dst_addrs[pid + 1].value_offset,
          _MM_HINT_T1);
    }
    auto value_offset = dst_addrs[pid].value_offset;
    auto dst_value_base = dst_addrs[pid].valueptr + value_offset;
    auto capacity = dst_addrs[pid].value_capacity;

    auto r = reducer_offset_offset_[pid]; /*128k*/
    auto size = reducer_offset_offset_[pid + 1] - r;

    for (register uint32_t x = 0; x < size; x++) {
      auto src_offset = reducer_offsets_[x + r]; /*128k*/
      auto strlength =
          src_offset_addr[src_offset + 1] - src_offset_addr[src_offset];
      value_offset = dst_offset_base[x + 1] = value_offset + strlength;
      if (ARROW_PREDICT_FALSE(value_offset >= capacity)) {
        // allocate value buffer again
        // enlarge the buffer by 1.5x
        capacity = capacity + std::max((capacity >> 1), (uint64_t)strlength);

        auto value_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            partition_buffers_[fixed_width_col_cnt_ + binary_idx][pid][2]);
        value_buffer->Reserve(capacity);

        dst_addrs[pid].valueptr = value_buffer->mutable_data();
        dst_addrs[pid].value_capacity = capacity;
        dst_value_base = dst_addrs[pid].valueptr + value_offset - strlength;
        std::cout << " value buffer resized colid = " << binary_idx
                  << " dst_start " << dst_offset_base[x] << " dst_end "
                  << dst_offset_base[x + 1] << " old size = " << capacity
                  << " new size = " << capacity
                  << " row = " << partition_buffer_idx_base_[pid]
                  << " strlen = " << strlength << std::endl;
      }
      auto value_src_ptr = src_addr + src_offset_addr[src_offset];
#ifdef __AVX512BW__
      if (ARROW_PREDICT_TRUE(support_avx512_)) {
        // write the variable value
        T k;
        for (k = 0; k + 32 < strlength; k += 32) {
          __m256i v = _mm256_loadu_si256((const __m256i*)(value_src_ptr + k));
          _mm256_storeu_si256((__m256i*)(dst_value_base + k), v);
        }
        auto mask = (1L << (strlength - k)) - 1;
        __m256i v = _mm256_maskz_loadu_epi8(mask, value_src_ptr + k);
        _mm256_mask_storeu_epi8(dst_value_base + k, mask, v);
      } else
#endif
      {
        memcpy(dst_value_base, value_src_ptr, strlength);
      }
      dst_value_base += strlength;
      _mm_prefetch(value_src_ptr + 64, _MM_HINT_T1);
      _mm_prefetch(src_offset_addr + src_offset + 64 / sizeof(T), _MM_HINT_T1);
    }
    dst_addrs[pid].value_offset = value_offset;
  }
  return arrow::Status::OK();
}

arrow::Status Splitter::SplitBinaryArray(const arrow::RecordBatch& rb) {
  const auto num_rows = rb.num_rows();
  int64_t row;
  for (auto col = fixed_width_col_cnt_; col < array_idx_.size(); ++col) {
    auto& dst_addrs = partition_binary_addrs_[col - fixed_width_col_cnt_];
    auto col_idx = array_idx_[col];
    auto arr_data = rb.column_data(col_idx);
    auto src_value_addr = arr_data->GetValuesSafe<uint8_t>(2);

    auto typeids = column_type_id_[col_idx]->id();
    if (typeids == arrow::BinaryType::type_id ||
        typeids == arrow::StringType::type_id) {
      auto src_offset_addr =
          arr_data->GetValuesSafe<arrow::BinaryType::offset_type>(1);
      SplitBinaryType<arrow::BinaryType::offset_type>(
          src_value_addr,
          src_offset_addr,
          dst_addrs,
          col - fixed_width_col_cnt_);
    } else {
      auto src_offset_addr =
          arr_data->GetValuesSafe<arrow::LargeBinaryType::offset_type>(1);
      SplitBinaryType<arrow::LargeBinaryType::offset_type>(
          src_value_addr,
          src_offset_addr,
          dst_addrs,
          col - fixed_width_col_cnt_);
    }
  }

  return arrow::Status::OK();
}

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::Decimal128Type)         \
  PROCESS(arrow::StringType)             \
  PROCESS(arrow::BinaryType)
arrow::Status Splitter::SplitListArray(const arrow::RecordBatch& rb) {
  for (int i = 0; i < list_array_idx_.size(); ++i) {
    auto src_arr = std::static_pointer_cast<arrow::ListArray>(
        rb.column(list_array_idx_[i]));
    auto status = AppendList(
        rb.column(list_array_idx_[i]),
        partition_list_builders_[i],
        rb.num_rows());
    if (!status.ok())
      return status;
  }
  return arrow::Status::OK();
}

#undef PROCESS_SUPPORTED_TYPES

arrow::Status Splitter::AppendList(
    const std::shared_ptr<arrow::Array>& src_arr,
    const std::vector<std::shared_ptr<arrow::ArrayBuilder>>& dst_builders,
    int64_t num_rows) {
  for (auto row = 0; row < num_rows; ++row) {
    RETURN_NOT_OK(dst_builders[partition_id_[row]]->AppendArraySlice(
        *(src_arr->data().get()), row, 1));
  }
  return arrow::Status::OK();
}

std::string Splitter::NextSpilledFileDir() {
  auto spilled_file_dir = GetSpilledShuffleFileDir(
      configured_dirs_[dir_selection_], sub_dir_selection_[dir_selection_]);
  sub_dir_selection_[dir_selection_] =
      (sub_dir_selection_[dir_selection_] + 1) % options_.num_sub_dirs;
  dir_selection_ = (dir_selection_ + 1) % configured_dirs_.size();
  return spilled_file_dir;
}

arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>>
Splitter::GetSchemaPayload() {
  if (schema_payload_ != nullptr) {
    return schema_payload_;
  }
  schema_payload_ = std::make_shared<arrow::ipc::IpcPayload>();
  arrow::ipc::DictionaryFieldMapper dict_file_mapper; // unused
  RETURN_NOT_OK(arrow::ipc::GetSchemaPayload(
      *schema_,
      options_.ipc_write_options,
      dict_file_mapper,
      schema_payload_.get()));
  return schema_payload_;
}

// ----------------------------------------------------------------------
// RoundRobinSplitter

arrow::Result<std::shared_ptr<RoundRobinSplitter>> RoundRobinSplitter::Create(
    int32_t num_partitions,
    std::shared_ptr<arrow::Schema> schema,
    SplitOptions options) {
  std::shared_ptr<RoundRobinSplitter> res(new RoundRobinSplitter(
      num_partitions, std::move(schema), std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status RoundRobinSplitter::ComputeAndCountPartitionId(
    const arrow::RecordBatch& rb) {
  std::fill(std::begin(partition_id_cnt_), std::end(partition_id_cnt_), 0);
  partition_id_.resize(rb.num_rows());
  for (auto& pid : partition_id_) {
    pid = pid_selection_;
    partition_id_cnt_[pid_selection_]++;
    pid_selection_ =
        (pid_selection_ + 1) == num_partitions_ ? 0 : (pid_selection_ + 1);
  }
  return arrow::Status::OK();
}

// ----------------------------------------------------------------------
// HashSplitter

arrow::Result<std::shared_ptr<HashSplitter>> HashSplitter::Create(
    int32_t num_partitions,
    std::shared_ptr<arrow::Schema> schema,
    const substrait::Rel& subRel,
    SplitOptions options) {
  std::shared_ptr<HashSplitter> res(
      new HashSplitter(num_partitions, std::move(schema), std::move(options)));
  RETURN_NOT_OK(res->Init());
  RETURN_NOT_OK(res->CreateGandivaExpr(subRel));
  RETURN_NOT_OK(res->CreateProjector());
  return res;
}

arrow::Status HashSplitter::CreateGandivaExpr(const substrait::Rel& subRel) {
  // Parse the ProjectRel to get hash expression.
  // Currently, only filed is supported.
  substrait::ProjectRel subProject;
  if (subRel.has_project()) {
    subProject = subRel.project();
  }
  for (auto& sexpr : subProject.expressions()) {
    switch (sexpr.rex_type_case()) {
      case substrait::Expression::RexTypeCase::kSelection: {
        auto sfield = sexpr.selection();
        switch (sfield.reference_type_case()) {
          case substrait::Expression::FieldReference::ReferenceTypeCase::
              kDirectReference: {
            auto sref = sfield.direct_reference();
            switch (sref.reference_type_case()) {
              case substrait::Expression::ReferenceSegment::ReferenceTypeCase::
                  kStructField: {
                hashIndices_.push_back(sref.struct_field().field());
                break;
              }
              default:
                return arrow::Status::Invalid("Unrecognized reference.");
            }
            break;
          }
          default:
            return arrow::Status::Invalid("Unrecognized expression.");
        }
        break;
      }
      default:
        return arrow::Status::Invalid(
            "Only Fields are supported as hash keys.");
    }
  }
  for (auto idx : hashIndices_) {
    const auto& field = schema_->field(idx);
    auto node = gandiva::TreeExprBuilder::MakeField(field);
    exprVector_.push_back(gandiva::TreeExprBuilder::MakeExpression(
        std::move(node), arrow::field("res_" + field->name(), field->type())));
  }
  return arrow::Status::OK();
}

arrow::Status HashSplitter::CreateProjector() {
  // same seed as spark's
  auto hash = gandiva::TreeExprBuilder::MakeLiteral((int32_t)42);
  for (const auto& expr : exprVector_) {
    switch (expr->root()->return_type()->id()) {
      case arrow::NullType::type_id:
        break;
      case arrow::BooleanType::type_id:
      case arrow::Int8Type::type_id:
      case arrow::Int16Type::type_id:
      case arrow::Int32Type::type_id:
      case arrow::FloatType::type_id:
      case arrow::Date32Type::type_id:
        hash = gandiva::TreeExprBuilder::MakeFunction(
            "hash32_spark", {expr->root(), hash}, arrow::int32());
        break;
      case arrow::Int64Type::type_id:
      case arrow::DoubleType::type_id:
        hash = gandiva::TreeExprBuilder::MakeFunction(
            "hash64_spark", {expr->root(), hash}, arrow::int32());
        break;
      case arrow::StringType::type_id:
        hash = gandiva::TreeExprBuilder::MakeFunction(
            "hashbuf_spark", {expr->root(), hash}, arrow::int32());
        break;
      default:
        hash = gandiva::TreeExprBuilder::MakeFunction(
            "hash32", {expr->root(), hash}, arrow::int32());
        /*return arrow::Status::NotImplemented("HashSplitter::CreateProjector
           doesn't support type ", expr->result()->type()->ToString());*/
    }
  }
  auto hash_expr = gandiva::TreeExprBuilder::MakeExpression(
      hash, arrow::field("pid", arrow::int32()));
  return gandiva::Projector::Make(schema_, {hash_expr}, &projector_);
}

arrow::Status HashSplitter::ComputeAndCountPartitionId(
    const arrow::RecordBatch& rb) {
  auto num_rows = rb.num_rows();
  partition_id_.resize(num_rows);
  std::fill(std::begin(partition_id_cnt_), std::end(partition_id_cnt_), 0);

  arrow::ArrayVector outputs;
  TIME_NANO_OR_RAISE(
      total_compute_pid_time_,
      projector_->Evaluate(rb, options_.memory_pool.get(), &outputs));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid(
        "Projector result should have one field, actual is ",
        std::to_string(outputs.size()));
  }
  auto pid_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  if (pid_arr == nullptr) {
    return arrow::Status::Invalid("failed to cast outputs.at(0)");
  }
  for (auto i = 0; i < num_rows; ++i) {
    // positive mod
    auto pid = pid_arr->Value(i) % num_partitions_;
    // force to generate ASM
    __asm__(
        "lea (%[num_partitions],%[pid],1),%[tmp]\n"
        "test %[pid],%[pid]\n"
        "cmovs %[tmp],%[pid]\n"
        : [ pid ] "+r"(pid)
        : [ num_partitions ] "r"(num_partitions_), [ tmp ] "r"(0));
    partition_id_[i] = pid;
    partition_id_cnt_[pid]++;
  }
  return arrow::Status::OK();
}

// ----------------------------------------------------------------------
// FallBackRangeSplitter

arrow::Result<std::shared_ptr<FallbackRangeSplitter>>
FallbackRangeSplitter::Create(
    int32_t num_partitions,
    std::shared_ptr<arrow::Schema> schema,
    SplitOptions options) {
  auto res = std::shared_ptr<FallbackRangeSplitter>(new FallbackRangeSplitter(
      num_partitions, std::move(schema), std::move(options)));
  RETURN_NOT_OK(res->Init());
  return res;
}

arrow::Status FallbackRangeSplitter::Init() {
  input_schema_ = std::move(schema_);
  ARROW_ASSIGN_OR_RAISE(schema_, input_schema_->RemoveField(0))
  return Splitter::Init();
}

arrow::Status FallbackRangeSplitter::Split(const arrow::RecordBatch& rb) {
  EVAL_START("split", options_.thread_id)
  RETURN_NOT_OK(ComputeAndCountPartitionId(rb));
  ARROW_ASSIGN_OR_RAISE(auto remove_pid, rb.RemoveColumn(0));
  RETURN_NOT_OK(DoSplit(*remove_pid));
  EVAL_END("split", options_.thread_id, options_.task_attempt_id)
  return arrow::Status::OK();
}

arrow::Status FallbackRangeSplitter::ComputeAndCountPartitionId(
    const arrow::RecordBatch& rb) {
  if (rb.column(0)->type_id() != arrow::Type::INT32) {
    return arrow::Status::Invalid(
        "RecordBatch field 0 should be ",
        arrow::int32()->ToString(),
        ", actual is ",
        rb.column(0)->type()->ToString());
  }

  auto pid_arr =
      reinterpret_cast<const int32_t*>(rb.column_data(0)->buffers[1]->data());
  auto num_rows = rb.num_rows();
  partition_id_.resize(num_rows);
  std::fill(std::begin(partition_id_cnt_), std::end(partition_id_cnt_), 0);
  for (auto i = 0; i < num_rows; ++i) {
    auto pid = pid_arr[i];
    if (pid >= num_partitions_) {
      return arrow::Status::Invalid(
          "Partition id ",
          std::to_string(pid),
          " is equal or greater than ",
          std::to_string(num_partitions_));
    }
    partition_id_[i] = pid;
    partition_id_cnt_[pid]++;
  }
  return arrow::Status::OK();
}

} // namespace shuffle
} // namespace gluten
