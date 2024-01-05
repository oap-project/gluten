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
#include "PartitionWriter.h"
#include <filesystem>
#include <format>
#include <memory>
#include <ostream>
#include <vector>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/sortBlock.h>
#include <Shuffle/CachedShuffleWriter.h>
#include <Storages/IO/CompressedWriteBuffer.h>
#include <Storages/IO/NativeWriter.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/CHUtil.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include "DataTypes/DataTypesNumber.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

using namespace DB;
namespace local_engine
{


void Partition::addBlock(DB::Block block)
{
    /// Do not insert empty blocks, otherwise will cause the shuffle read terminate early.
    if (!block.rows())
        return;

    cached_bytes += block.bytes();
    blocks.emplace_back(std::move(block));
}

size_t Partition::spill(NativeWriter & writer)
{
    size_t written_bytes = 0;
    for (auto & block : blocks)
    {
        written_bytes += writer.write(block);

        /// Clear each block once it is serialized to reduce peak memory
        DB::Block().swap(block);
    }

    blocks.clear();
    cached_bytes = 0;
    return written_bytes;
}

IPartitionWriter::IPartitionWriter(CachedShuffleWriter * shuffle_writer_)
    : shuffle_writer(shuffle_writer_), options(&shuffle_writer->options)
{
}

void IPartitionWriter::write(PartitionInfo & info, DB::Block & block)
{
    /// PartitionWriter::write is always the top frame which occupies evicting_or_writing
    if (evicting_or_writing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "IPartitionWriter::write is invoked with evicting_or_writing being occupied");

    evicting_or_writing = true;
    SCOPE_EXIT({ evicting_or_writing = false; });

    unsafeWrite(info, block);
}

void IPartitionWriter::stop()
{
    if (evicting_or_writing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "IPartitionWriter::stop is invoked with evicting_or_writing being occupied");

    evicting_or_writing = true;
    SCOPE_EXIT({ evicting_or_writing = false; });
    return unsafeStop();
}

size_t IPartitionWriter::evictPartitions(bool for_memory_spill, bool flush_block_buffer)
{
    if (evicting_or_writing)
        return 0;

    evicting_or_writing = true;
    SCOPE_EXIT({ evicting_or_writing = false; });
    return unsafeEvictPartitions(for_memory_spill, flush_block_buffer);
}

IHashBasedPartitionWriter::IHashBasedPartitionWriter(CachedShuffleWriter * shuffle_writer_)
    : IPartitionWriter(shuffle_writer_)
    , partition_block_buffer(options->partition_num)
    , partition_buffer(options->partition_num)
    , last_partition_id(options->partition_num - 1)
{
    for (size_t partition_id = 0; partition_id < options->partition_num; ++partition_id)
    {
        partition_block_buffer[partition_id] = std::make_shared<ColumnsBuffer>(options->split_size);
        partition_buffer[partition_id] = std::make_shared<Partition>();
    }
}

size_t IHashBasedPartitionWriter::bytes() const
{
    size_t bytes = 0;

    for (const auto & buffer : partition_block_buffer)
        bytes += buffer->bytes();

    for (const auto & buffer : partition_buffer)
        bytes += buffer->bytes();

    return bytes;
}

void IHashBasedPartitionWriter::unsafeWrite(PartitionInfo & info, DB::Block & block)
{
    size_t current_cached_bytes = bytes();
    for (size_t partition_id = 0; partition_id < info.partition_num; ++partition_id)
    {
        size_t from = info.partition_start_points[partition_id];
        size_t length = info.partition_start_points[partition_id + 1] - from;

        /// Make sure buffer size is no greater than split_size
        auto & block_buffer = partition_block_buffer[partition_id];
        auto & buffer = partition_buffer[partition_id];
        if (block_buffer->size() && block_buffer->size() + length > shuffle_writer->options.split_size)
            buffer->addBlock(block_buffer->releaseColumns());

        current_cached_bytes -= block_buffer->bytes();
        for (size_t col_i = 0; col_i < block.columns(); ++col_i)
            block_buffer->appendSelective(col_i, block, info.partition_selector, from, length);
        current_cached_bytes += block_buffer->bytes();

        /// Only works for celeborn partitiion writer
        if (supportsEvictSinglePartition() && options->spill_threshold > 0 && current_cached_bytes >= options->spill_threshold)
        {
            /// If flush_block_buffer_before_evict is disabled, evict partitions from (last_partition_id+1)%partition_num to partition_id directly without flush,
            /// Otherwise flush partition block buffer if it's size is no less than average rows, then evict partitions as above.
            if (!options->flush_block_buffer_before_evict)
            {
                for (size_t i = (last_partition_id + 1) % options->partition_num; i != (partition_id + 1) % options->partition_num;
                     i = (i + 1) % options->partition_num)
                    unsafeEvictSinglePartition(false, false, i);
            }
            else
            {
                /// Calculate average rows of each partition block buffer
                size_t avg_size = 0;
                size_t cnt = 0;
                for (size_t i = (last_partition_id + 1) % options->partition_num; i != (partition_id + 1) % options->partition_num;
                     i = (i + 1) % options->partition_num)
                {
                    avg_size += partition_block_buffer[i]->size();
                    ++cnt;
                }
                avg_size /= cnt;


                for (size_t i = (last_partition_id + 1) % options->partition_num; i != (partition_id + 1) % options->partition_num;
                     i = (i + 1) % options->partition_num)
                {
                    bool flush_block_buffer = partition_block_buffer[i]->size() >= avg_size;
                    current_cached_bytes -= flush_block_buffer ? partition_block_buffer[i]->bytes() + partition_buffer[i]->bytes()
                                                               : partition_buffer[i]->bytes();
                    unsafeEvictSinglePartition(false, flush_block_buffer, i);
                }
            }

            last_partition_id = partition_id;
        }
    }

    /// Only works for local partition writer
    if (!supportsEvictSinglePartition() && options->spill_threshold && current_cached_bytes >= options->spill_threshold)
        unsafeEvictPartitions(false, options->flush_block_buffer_before_evict);
}

size_t LocalHashBasedPartitionWriter::unsafeEvictPartitions(bool for_memory_spill, bool flush_block_buffer)
{
    size_t res = 0;
    size_t spilled_bytes = 0;

    auto spill_to_file = [this, for_memory_spill, flush_block_buffer, &res, &spilled_bytes]()
    {
        auto file = getNextSpillFile();
        WriteBufferFromFile output(file, shuffle_writer->options.io_buffer_size);
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
        CompressedWriteBuffer compressed_output(output, codec, shuffle_writer->options.io_buffer_size);
        NativeWriter writer(compressed_output, shuffle_writer->output_header);

        SpillInfo info;
        info.spilled_file = file;

        Stopwatch serialization_time_watch;
        for (size_t partition_id = 0; partition_id < partition_buffer.size(); ++partition_id)
        {
            auto & buffer = partition_buffer[partition_id];

            if (flush_block_buffer)
            {
                auto & block_buffer = partition_block_buffer[partition_id];
                if (!block_buffer->empty())
                    buffer->addBlock(block_buffer->releaseColumns());
            }

            if (buffer->empty())
                continue;

            PartitionSpillInfo partition_spill_info;
            partition_spill_info.start = output.count();
            spilled_bytes += buffer->bytes();

            size_t written_bytes = buffer->spill(writer);
            res += written_bytes;

            compressed_output.sync();
            partition_spill_info.length = output.count() - partition_spill_info.start;
            shuffle_writer->split_result.raw_partition_lengths[partition_id] += written_bytes;
            partition_spill_info.partition_id = partition_id;
            info.partition_spill_infos.emplace_back(partition_spill_info);
        }

        spill_infos.emplace_back(info);
        shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
        shuffle_writer->split_result.total_write_time += compressed_output.getWriteTime();
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
    };

    Stopwatch spill_time_watch;
    if (for_memory_spill && options->throw_if_memory_exceed)
    {
        // escape memory track from current thread status; add untracked memory limit for create thread object, avoid trigger memory spill again
        IgnoreMemoryTracker ignore(2 * 1024 * 1024);
        ThreadFromGlobalPool thread(spill_to_file);
        thread.join();
    }
    else
    {
        spill_to_file();
    }
    shuffle_writer->split_result.total_spill_time += spill_time_watch.elapsedNanoseconds();
    shuffle_writer->split_result.total_bytes_spilled += spilled_bytes;
    return res;
}

void LocalHashBasedPartitionWriter::unsafeStop()
{
    WriteBufferFromFile output(options->data_file, options->io_buffer_size);
    auto offsets = mergeSpills(output);
    shuffle_writer->split_result.partition_lengths = offsets;
}

String LocalHashBasedPartitionWriter::getNextSpillFile()
{
    auto file_name = std::to_string(options->shuffle_id) + "_" + std::to_string(options->map_id) + "_" + std::to_string(spill_infos.size());
    std::hash<std::string> hasher;
    auto hash = hasher(file_name);
    auto dir_id = hash % options->local_dirs_list.size();
    auto sub_dir_id = (hash / options->local_dirs_list.size()) % options->num_sub_dirs;

    std::string dir = std::filesystem::path(options->local_dirs_list[dir_id]) / std::format("{:02x}", sub_dir_id);
    if (!std::filesystem::exists(dir))
        std::filesystem::create_directories(dir);
    return std::filesystem::path(dir) / file_name;
}

std::vector<UInt64> LocalHashBasedPartitionWriter::mergeSpills(WriteBuffer & data_file)
{
    auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
    CompressedWriteBuffer compressed_output(data_file, codec, shuffle_writer->options.io_buffer_size);
    NativeWriter writer(compressed_output, shuffle_writer->output_header);

    std::vector<UInt64> partition_length(shuffle_writer->options.partition_num, 0);

    std::vector<ReadBufferPtr> spill_inputs;
    spill_inputs.reserve(spill_infos.size());
    for (const auto & spill : spill_infos)
    {
        // only use readBig
        spill_inputs.emplace_back(std::make_shared<ReadBufferFromFile>(spill.spilled_file, 0));
    }

    Stopwatch write_time_watch;
    Stopwatch io_time_watch;
    Stopwatch serialization_time_watch;
    size_t merge_io_time = 0;
    String buffer;
    for (size_t partition_id = 0; partition_id < partition_block_buffer.size(); ++partition_id)
    {
        auto size_before = data_file.count();

        io_time_watch.restart();
        for (size_t i = 0; i < spill_infos.size(); ++i)
        {
            size_t size = spill_infos[i].partition_spill_infos[partition_id].length;
            buffer.reserve(size);
            auto count = spill_inputs[i]->readBig(buffer.data(), size);
            data_file.write(buffer.data(), count);
        }
        merge_io_time += io_time_watch.elapsedNanoseconds();

        serialization_time_watch.restart();
        if (!partition_block_buffer[partition_id]->empty())
        {
            Block block = partition_block_buffer[partition_id]->releaseColumns();
            partition_buffer[partition_id]->addBlock(std::move(block));
        }
        size_t raw_size = partition_buffer[partition_id]->spill(writer);

        compressed_output.sync();
        partition_length[partition_id] = data_file.count() - size_before;
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
        shuffle_writer->split_result.total_bytes_written += partition_length[partition_id];
        shuffle_writer->split_result.raw_partition_lengths[partition_id] += raw_size;
    }

    shuffle_writer->split_result.total_write_time += write_time_watch.elapsedNanoseconds();
    shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
    shuffle_writer->split_result.total_io_time += compressed_output.getWriteTime();
    shuffle_writer->split_result.total_serialize_time = shuffle_writer->split_result.total_serialize_time - shuffle_writer->split_result.total_io_time - shuffle_writer->split_result.total_compress_time;
    shuffle_writer->split_result.total_io_time += merge_io_time;

    for (const auto & spill : spill_infos)
    {
        std::filesystem::remove(spill.spilled_file);
    }

    return partition_length;
}

size_t CelebornHashBasedPartitionWriter::unsafeEvictPartitions(bool for_memory_spill, bool flush_block_buffer)
{
    size_t res = 0;
    for (size_t partition_id = 0; partition_id < options->partition_num; ++partition_id)
        res += unsafeEvictSinglePartition(for_memory_spill, flush_block_buffer, partition_id);
    return res;
}

void CelebornHashBasedPartitionWriter::unsafeStop()
{
    unsafeEvictPartitions(false, true);
    for (const auto & length : shuffle_writer->split_result.partition_lengths)
    {
        shuffle_writer->split_result.total_bytes_written += length;
    }
}

size_t CelebornHashBasedPartitionWriter::unsafeEvictSinglePartition(bool for_memory_spill, bool flush_block_buffer, size_t partition_id)
{
    size_t res = 0;
    size_t spilled_bytes = 0;
    auto spill_to_celeborn = [this, for_memory_spill, flush_block_buffer, partition_id, &res, &spilled_bytes]()
    {
        Stopwatch serialization_time_watch;
        auto & buffer = partition_buffer[partition_id];

        if (flush_block_buffer)
        {
            auto & block_buffer = partition_block_buffer[partition_id];
            if (!block_buffer->empty())
            {
                buffer->addBlock(block_buffer->releaseColumns());
            }
        }

        /// Skip empty buffer
        if (buffer->empty())
            return;

        WriteBufferFromOwnString output;
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
        CompressedWriteBuffer compressed_output(output, codec, shuffle_writer->options.io_buffer_size);
        NativeWriter writer(compressed_output, shuffle_writer->output_header);

        spilled_bytes += buffer->bytes();
        size_t written_bytes = buffer->spill(writer);
        res += written_bytes;
        compressed_output.sync();

        Stopwatch push_time_watch;
        celeborn_client->pushPartitionData(partition_id, output.str().data(), output.str().size());

        shuffle_writer->split_result.partition_lengths[partition_id] += output.str().size();
        shuffle_writer->split_result.raw_partition_lengths[partition_id] += written_bytes;
        shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
        shuffle_writer->split_result.total_write_time += compressed_output.getWriteTime();
        shuffle_writer->split_result.total_write_time += push_time_watch.elapsedNanoseconds();
        shuffle_writer->split_result.total_io_time += push_time_watch.elapsedNanoseconds();
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
    };

    Stopwatch spill_time_watch;
    if (for_memory_spill && options->throw_if_memory_exceed)
    {
        // escape memory track from current thread status; add untracked memory limit for create thread object, avoid trigger memory spill again
        IgnoreMemoryTracker ignore(2 * 1024 * 1024);
        ThreadFromGlobalPool thread(spill_to_celeborn);
        thread.join();
    }
    else
    {
        spill_to_celeborn();
    }

    shuffle_writer->split_result.total_spill_time += spill_time_watch.elapsedNanoseconds();
    shuffle_writer->split_result.total_bytes_spilled += spilled_bytes;
    return res;
}

void ISortBasedPartitionWriter::unsafeWrite(PartitionInfo & info, DB::Block & block)
{
    size_t rows = block.rows();
    if (rows == 0)
        return;

    /// Make sure sorted_block_buffer do not exceed split_size
    if (sorted_block_buffer && sorted_block_buffer->size() + rows > sorted_block_buffer->reserveSize())
        flushSortedBlockBuffer();

    /// Create sorted_block_buffer if not exists
    if (!sorted_block_buffer) [[unlikely]]
    {
        size_t bytes_per_row = block.bytes() / rows;
        size_t reserve_size = options->spill_threshold / bytes_per_row;
        sorted_block_buffer = std::make_shared<ColumnsBuffer>(reserve_size);
        std::cout << "create sorted buffer with reserve size:" << reserve_size << std::endl;
    }

    /// Insert column partition for later sort
    ColumnWithTypeAndName col_partition;
    col_partition.name = "_partition_" + std::to_string(reinterpret_cast<uintptr_t>(this));
    col_partition.type = std::make_shared<DataTypeUInt64>();
    auto column_partition = ColumnVector<UInt64>::create();
    column_partition->getData().swap(info.partition_ids);
    col_partition.column = column_partition->getPtr();
    block.insert(std::move(col_partition));

    /// Merge current block into sorted_buffer
    sorted_block_buffer->append(block, 0, rows);
    std::cout << "append block with rows:" << rows << " sorted_buffer rows:" << sorted_block_buffer->size() << std::endl;
}

size_t ISortBasedPartitionWriter::flushSortedBlockBuffer()
{
    Stopwatch watch;

    if (!sorted_block_buffer || sorted_block_buffer->empty())
        return 0;

    size_t res = 0;

    /// Sort block released from sorted_buffer by partition column
    Block sorted_block = sorted_block_buffer->releaseColumns();
    {
        const auto & col_partition = sorted_block.getByPosition(sorted_block.columns() - 1);
        SortDescription sort_description;
        sort_description.emplace_back(col_partition.name, 1, 1);
        sortBlock(sorted_block, sort_description);
    }

    /// Remove the last partition column
    const auto col_partition = sorted_block.getByPosition(sorted_block.columns() - 1);
    sorted_block.erase(sorted_block.columns() - 1);

    /// Evict sorted_block by partitions
    const auto & partition_data = checkAndGetColumn<ColumnUInt64>(col_partition.column.get())->getData();
    size_t start = 0;
    size_t end = 0;
    for (size_t row_i = 1; row_i < partition_data.size(); ++row_i)
    {
        if (partition_data[row_i] == partition_data[start])
            continue;

        end = row_i;
        res += unsafeEvictSinglePartitionFromBlock(partition_data[start], sorted_block, start, end - start);

        start = row_i;
    }

    end = partition_data.size();
    res += unsafeEvictSinglePartitionFromBlock(partition_data[start], sorted_block, start, end - start);
    shuffle_writer->split_result.total_bytes_spilled += sorted_block.bytes();
    std::cout << "split bytes:" << sorted_block.bytes() << " rows:" << sorted_block.rows() << " in " << watch.elapsedMilliseconds() << " ms"
              << std::endl;
    return res;
}

void ISortBasedPartitionWriter::unsafeStop()
{
    flushSortedBlockBuffer();
    for (const auto & length : shuffle_writer->split_result.partition_lengths)
    {
        shuffle_writer->split_result.total_bytes_written += length;
    }
}

size_t ISortBasedPartitionWriter::unsafeEvictPartitions(bool /*for_memory_spill*/, bool flush_block_buffer)
{
    if (!flush_block_buffer)
        return 0;

    return flushSortedBlockBuffer();
}

size_t CelebornSortedBasedPartitionWriter::unsafeEvictSinglePartitionFromBlock(
    size_t partition_id, const DB::Block & block, size_t start, size_t length)
{
    size_t res = 0;
    auto spill_to_celeborn = [this, partition_id, start, length, &block, &res]()
    {
        Stopwatch serialization_time_watch;

        if (length == 0)
            return;

        WriteBufferFromOwnString output;
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
        CompressedWriteBuffer compressed_output(output, codec, shuffle_writer->options.io_buffer_size);
        NativeWriter writer(compressed_output, shuffle_writer->output_header);

        size_t written_bytes = writer.write(block, start, length);
        res += written_bytes;
        compressed_output.sync();

        Stopwatch push_time_watch;
        celeborn_client->pushPartitionData(partition_id, output.str().data(), output.str().size());

        shuffle_writer->split_result.partition_lengths[partition_id] += output.str().size();
        shuffle_writer->split_result.raw_partition_lengths[partition_id] += written_bytes;
        shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
        shuffle_writer->split_result.total_write_time += compressed_output.getWriteTime();
        shuffle_writer->split_result.total_write_time += push_time_watch.elapsedNanoseconds();
        shuffle_writer->split_result.total_io_time += push_time_watch.elapsedNanoseconds();
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
    };

    Stopwatch spill_time_watch;
    spill_to_celeborn();
    shuffle_writer->split_result.total_spill_time += spill_time_watch.elapsedNanoseconds();
    return res;
}

}
