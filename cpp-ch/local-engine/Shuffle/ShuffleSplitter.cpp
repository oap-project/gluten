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
#include "ShuffleSplitter.h"
#include <filesystem>
#include <format>
#include <memory>
#include <string>
#include <fcntl.h>
#include <Compression/CompressionFactory.h>
#include <Storages/IO/AggregateSerializationUtils.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parser/SerializedPlanParser.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Poco/StringTokenizer.h>
#include <Common/Stopwatch.h>
#include <Common/DebugUtils.h>

namespace local_engine
{

void ShuffleSplitter::split(DB::Block & block)
{
    if (block.rows() == 0)
    {
        return;
    }
    initOutputIfNeeded(block);
    computeAndCountPartitionId(block);
    Stopwatch split_time_watch;
    block = convertAggregateStateInBlock(block);
    split_result.total_split_time += split_time_watch.elapsedNanoseconds();
    splitBlockByPartition(block);
}

SplitResult ShuffleSplitter::stop()
{
    // spill all buffers
    Stopwatch watch;
    for (size_t i = 0; i < options.partition_num; i++)
    {
        spillPartition(i);
        partition_outputs[i]->flush();
        partition_write_buffers[i]->sync();
    }
    for (auto * item : compressed_buffers)
    {
        if (item)
        {
            split_result.total_compress_time += item->getCompressTime();
            split_result.total_io_time += item->getWriteTime();
        }
    }
    split_result.total_serialize_time = split_result.total_spill_time - split_result.total_compress_time - split_result.total_io_time;
    partition_outputs.clear();
    partition_cached_write_buffers.clear();
    partition_write_buffers.clear();
    mergePartitionFiles();
    split_result.total_write_time += watch.elapsedNanoseconds();
    stopped = true;
    return split_result;
}

void ShuffleSplitter::initOutputIfNeeded(Block & block)
{
    if (output_header.columns() == 0) [[unlikely]]
    {
        output_header = block.cloneEmpty();
        if (output_columns_indicies.empty())
        {
            output_header = block.cloneEmpty();
            for (size_t i = 0; i < block.columns(); ++i)
            {
                output_columns_indicies.push_back(i);
            }
        }
        else
        {
            ColumnsWithTypeAndName cols;
            for (const auto & index : output_columns_indicies)
            {
                cols.push_back(block.getByPosition(index));
            }
            output_header = DB::Block(cols);
        }
    }
}

void ShuffleSplitter::splitBlockByPartition(DB::Block & block)
{
    Stopwatch split_time_watch;
    DB::Block out_block;
    for (size_t col = 0; col < output_header.columns(); ++col)
    {
        out_block.insert(block.getByPosition(output_columns_indicies[col]));
    }
    for (size_t col = 0; col < output_header.columns(); ++col)
    {
        for (size_t j = 0; j < partition_info.partition_num; ++j)
        {
            size_t from = partition_info.partition_start_points[j];
            size_t length = partition_info.partition_start_points[j + 1] - from;
            if (length == 0)
                continue; // no data for this partition continue;
            partition_buffer[j]->appendSelective(col, out_block, partition_info.partition_selector, from, length);
        }
    }
    split_result.total_split_time += split_time_watch.elapsedNanoseconds();

    for (size_t i = 0; i < options.partition_num; ++i)
    {
        auto & buffer = partition_buffer[i];
        if (buffer->size() >= options.split_size)
        {
            spillPartition(i);
        }
    }
}

ShuffleSplitter::ShuffleSplitter(const SplitOptions & options_) : options(options_)
{
    init();
}

void ShuffleSplitter::init()
{
    partition_buffer.resize(options.partition_num);
    partition_outputs.resize(options.partition_num);
    partition_write_buffers.resize(options.partition_num);
    partition_cached_write_buffers.resize(options.partition_num);
    split_result.partition_lengths.resize(options.partition_num);
    split_result.raw_partition_lengths.resize(options.partition_num);
    for (size_t partition_i = 0; partition_i < options.partition_num; ++partition_i)
    {
        partition_buffer[partition_i] = std::make_shared<ColumnsBuffer>(options.split_size);
        split_result.partition_lengths[partition_i] = 0;
        split_result.raw_partition_lengths[partition_i] = 0;
    }
}

void ShuffleSplitter::spillPartition(size_t partition_id)
{
    Stopwatch watch;
    if (!partition_outputs[partition_id])
    {
        partition_write_buffers[partition_id] = getPartitionWriteBuffer(partition_id);
        partition_outputs[partition_id]
            = std::make_unique<NativeWriter>(*partition_write_buffers[partition_id], output_header);
    }
    DB::Block result = partition_buffer[partition_id]->releaseColumns();
    if (result.rows() > 0)
    {
        partition_outputs[partition_id]->write(result);
    }
    split_result.total_spill_time += watch.elapsedNanoseconds();
    split_result.total_bytes_spilled += result.bytes();
}

void ShuffleSplitter::mergePartitionFiles()
{
    Stopwatch merge_io_time;
    DB::WriteBufferFromFile data_write_buffer = DB::WriteBufferFromFile(options.data_file);
    std::string buffer;
    size_t buffer_size = options.io_buffer_size;
    buffer.reserve(buffer_size);
    for (size_t i = 0; i < options.partition_num; ++i)
    {
        auto file = getPartitionTempFile(i);
        DB::ReadBufferFromFile reader = DB::ReadBufferFromFile(file, options.io_buffer_size);
        while (reader.next())
        {
            auto bytes = reader.readBig(buffer.data(), buffer_size);
            data_write_buffer.write(buffer.data(), bytes);
            split_result.partition_lengths[i] += bytes;
            split_result.total_bytes_written += bytes;
        }
        reader.close();
        std::filesystem::remove(file);
    }
    split_result.total_io_time += merge_io_time.elapsedNanoseconds();
    data_write_buffer.close();
}


ShuffleSplitterPtr ShuffleSplitter::create(const std::string & short_name, const SplitOptions & options_)
{
    if (short_name == "rr")
        return RoundRobinSplitter::create(options_);
    else if (short_name == "hash")
        return HashSplitter::create(options_);
    else if (short_name == "single")
    {
        SplitOptions options = options_;
        options.partition_num = 1;
        return RoundRobinSplitter::create(options);
    }
    else if (short_name == "range")
        return RangeSplitter::create(options_);
    else
        throw std::runtime_error("unsupported splitter " + short_name);
}

std::string ShuffleSplitter::getPartitionTempFile(size_t partition_id)
{
    auto file_name = std::to_string(options.shuffle_id) + "_" + std::to_string(options.map_id) + "_" + std::to_string(partition_id);
    std::hash<std::string> hasher;
    auto hash = hasher(file_name);
    auto dir_id = hash % options.local_dirs_list.size();
    auto sub_dir_id = (hash / options.local_dirs_list.size()) % options.num_sub_dirs;

    std::string dir = std::filesystem::path(options.local_dirs_list[dir_id]) / std::format("{:02x}", sub_dir_id);
    if (!std::filesystem::exists(dir))
        std::filesystem::create_directories(dir);
    return std::filesystem::path(dir) / file_name;
}

std::unique_ptr<DB::WriteBuffer> ShuffleSplitter::getPartitionWriteBuffer(size_t partition_id)
{
    auto file = getPartitionTempFile(partition_id);
    if (partition_cached_write_buffers[partition_id] == nullptr)
        partition_cached_write_buffers[partition_id]
            = std::make_unique<DB::WriteBufferFromFile>(file, options.io_buffer_size, O_CREAT | O_WRONLY | O_APPEND);
    if (!options.compress_method.empty()
        && std::find(compress_methods.begin(), compress_methods.end(), options.compress_method) != compress_methods.end())
    {
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(options.compress_method), {});
        auto compressed = std::make_unique<CompressedWriteBuffer>(*partition_cached_write_buffers[partition_id], codec);
        compressed_buffers.emplace_back(compressed.get());
        return compressed;
    }
    else
    {
        return std::move(partition_cached_write_buffers[partition_id]);
    }
}

void ShuffleSplitter::writeIndexFile()
{
    auto index_file = options.data_file + ".index";
    auto writer = std::make_unique<DB::WriteBufferFromFile>(index_file, options.io_buffer_size, O_CREAT | O_WRONLY | O_TRUNC);
    for (auto len : split_result.partition_lengths)
    {
        DB::writeIntText(len, *writer);
        DB::writeChar('\n', *writer);
    }
}

void ColumnsBuffer::add(DB::Block & block, int start, int end)
{
    if (!header)
        header = block.cloneEmpty();

    if (accumulated_columns.empty())
    {
        accumulated_columns.reserve(block.columns());
        for (size_t i = 0; i < block.columns(); i++)
        {
            auto column = block.getColumns()[i]->cloneEmpty();
            column->reserve(prefer_buffer_size);
            accumulated_columns.emplace_back(std::move(column));
        }
    }

    assert(!accumulated_columns.empty());
    for (size_t i = 0; i < block.columns(); ++i)
    {
        if (!accumulated_columns[i]->onlyNull())
        {
            accumulated_columns[i]->insertRangeFrom(*block.getByPosition(i).column, start, end - start);
        }
        else
        {
            accumulated_columns[i]->insertMany(DB::Field(), end - start);
        }
    }
}

void ColumnsBuffer::appendSelective(
    size_t column_idx, const DB::Block & source, const DB::IColumn::Selector & selector, size_t from, size_t length)
{
    if (!header)
        header = source.cloneEmpty();

    if (accumulated_columns.empty())
    {
        accumulated_columns.reserve(source.columns());
        for (size_t i = 0; i < source.columns(); i++)
        {
            auto column = source.getColumns()[i]->convertToFullIfNeeded()->cloneEmpty();
            column->reserve(prefer_buffer_size);
            accumulated_columns.emplace_back(std::move(column));
        }
    }

    if (!accumulated_columns[column_idx]->onlyNull())
    {
        accumulated_columns[column_idx]->insertRangeSelective(
            *source.getByPosition(column_idx).column->convertToFullIfNeeded(), selector, from, length);
    }
    else
    {
        accumulated_columns[column_idx]->insertMany(DB::Field(), length);
    }
}

size_t ColumnsBuffer::size() const
{
    return accumulated_columns.empty() ? 0 : accumulated_columns[0]->size();
}

bool ColumnsBuffer::empty() const
{
    return accumulated_columns.empty() ? true : accumulated_columns[0]->empty();
}

DB::Block ColumnsBuffer::releaseColumns()
{
    DB::Columns columns(std::make_move_iterator(accumulated_columns.begin()), std::make_move_iterator(accumulated_columns.end()));
    accumulated_columns.clear();

    if (columns.empty())
        return header.cloneEmpty();
    else
        return header.cloneWithColumns(columns);
}

DB::Block ColumnsBuffer::getHeader()
{
    return header;
}

ColumnsBuffer::ColumnsBuffer(size_t prefer_buffer_size_) : prefer_buffer_size(prefer_buffer_size_)
{
}

RoundRobinSplitter::RoundRobinSplitter(const SplitOptions & options_) : ShuffleSplitter(options_)
{
    Poco::StringTokenizer output_column_tokenizer(options_.out_exprs, ",");
    for (auto iter = output_column_tokenizer.begin(); iter != output_column_tokenizer.end(); ++iter)
    {
        output_columns_indicies.push_back(std::stoi(*iter));
    }
    selector_builder = std::make_unique<RoundRobinSelectorBuilder>(options.partition_num);
}

void RoundRobinSplitter::computeAndCountPartitionId(DB::Block & block)
{
    Stopwatch watch;
    partition_info = selector_builder->build(block);
    split_result.total_compute_pid_time += watch.elapsedNanoseconds();
}

ShuffleSplitterPtr RoundRobinSplitter::create(const SplitOptions & options_)
{
    return std::make_unique<RoundRobinSplitter>(options_);
}

HashSplitter::HashSplitter(SplitOptions options_) : ShuffleSplitter(options_)
{
    Poco::StringTokenizer exprs_list(options_.hash_exprs, ",");
    std::vector<size_t> hash_fields;
    for (auto iter = exprs_list.begin(); iter != exprs_list.end(); ++iter)
    {
        hash_fields.push_back(std::stoi(*iter));
    }

    Poco::StringTokenizer output_column_tokenizer(options_.out_exprs, ",");
    for (auto iter = output_column_tokenizer.begin(); iter != output_column_tokenizer.end(); ++iter)
    {
        output_columns_indicies.push_back(std::stoi(*iter));
    }

    selector_builder = std::make_unique<HashSelectorBuilder>(options.partition_num, hash_fields, options_.hash_algorithm);
}

std::unique_ptr<ShuffleSplitter> HashSplitter::create(const SplitOptions & options_)
{
    return std::make_unique<HashSplitter>(options_);
}

void HashSplitter::computeAndCountPartitionId(DB::Block & block)
{
    Stopwatch watch;
    partition_info = selector_builder->build(block);
    split_result.total_compute_pid_time += watch.elapsedNanoseconds();
}

ShuffleSplitterPtr RangeSplitter::create(const SplitOptions & options_)
{
    return std::make_unique<RangeSplitter>(options_);
}

RangeSplitter::RangeSplitter(const SplitOptions & options_) : ShuffleSplitter(options_)
{
    Poco::StringTokenizer output_column_tokenizer(options_.out_exprs, ",");
    for (auto iter = output_column_tokenizer.begin(); iter != output_column_tokenizer.end(); ++iter)
    {
        output_columns_indicies.push_back(std::stoi(*iter));
    }
    selector_builder = std::make_unique<RangeSelectorBuilder>(options.hash_exprs, options.partition_num);
}

void RangeSplitter::computeAndCountPartitionId(DB::Block & block)
{
    Stopwatch watch;
    partition_info = selector_builder->build(block);
    split_result.total_compute_pid_time += watch.elapsedNanoseconds();
}
}
