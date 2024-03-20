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
#include "ShuffleReader.h"
#include <Compression/CompressedReadBuffer.h>
#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <jni/jni_common.h>
#include <Common/JNIUtils.h>

using namespace DB;

namespace local_engine
{

void configureCompressedReadBuffer(DB::CompressedReadBuffer & compressedReadBuffer)
{
    compressedReadBuffer.disableChecksumming();
}
ShuffleReader::ShuffleReader(std::unique_ptr<ReadBuffer> in_, bool compressed, Int64 max_shuffle_read_rows_, Int64 max_shuffle_read_bytes_)
    : in(std::move(in_)), max_shuffle_read_rows(max_shuffle_read_rows_), max_shuffle_read_bytes(max_shuffle_read_bytes_)
{
    if (compressed)
    {
        compressed_in = std::make_unique<CompressedReadBuffer>(*in);
        configureCompressedReadBuffer(static_cast<DB::CompressedReadBuffer &>(*compressed_in));
        input_stream = std::make_unique<NativeReader>(*compressed_in, max_shuffle_read_rows_, max_shuffle_read_bytes_);
    }
    else
    {
        input_stream = std::make_unique<NativeReader>(*in);
    }
}
Block * ShuffleReader::read()
{
    auto block = input_stream->read();
    setCurrentBlock(block);
    if (unlikely(header.columns() == 0))
        header = currentBlock().cloneEmpty();
    return &currentBlock();
}

ShuffleReader::~ShuffleReader()
{
    in.reset();
    compressed_in.reset();
    input_stream.reset();
}

jclass ShuffleReader::input_stream_class = nullptr;
jmethodID ShuffleReader::input_stream_read = nullptr;

bool ReadBufferFromJavaInputStream::nextImpl()
{
    int count = readFromJava();
    if (count > 0)
        working_buffer.resize(count);
    return count > 0;
}

int ReadBufferFromJavaInputStream::readFromJava() const
{
    GET_JNIENV(env)
    jint count = safeCallIntMethod(
        env, java_in, ShuffleReader::input_stream_read, reinterpret_cast<jlong>(internal_buffer.begin()), internal_buffer.size());
    CLEAN_JNIENV
    return count;
}

ReadBufferFromJavaInputStream::ReadBufferFromJavaInputStream(jobject input_stream) : java_in(input_stream)
{
}

ReadBufferFromJavaInputStream::~ReadBufferFromJavaInputStream()
{
    GET_JNIENV(env)
    env->DeleteGlobalRef(java_in);
    CLEAN_JNIENV
}

bool ReadBufferFromByteArray::nextImpl()
{
    if (read_pos >= array_size)
        return false;

    GET_JNIENV(env)
    const size_t read_size = std::min(internal_buffer.size(), array_size - read_pos);
    env->GetByteArrayRegion(array, read_pos, read_size, reinterpret_cast<jbyte *>(internal_buffer.begin()));
    working_buffer.resize(read_size);
    read_pos += read_size;
    CLEAN_JNIENV
    return true;
}
}
