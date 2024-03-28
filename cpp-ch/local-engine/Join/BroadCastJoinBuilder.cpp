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
#include "BroadCastJoinBuilder.h"
#include <Compression/CompressedReadBuffer.h>
#include <Interpreters/TableJoin.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Parser/JoinRelParser.h>
#include <Parser/TypeParser.h>
#include <Shuffle/ShuffleReader.h>
#include <jni/SharedPointerWrapper.h>
#include <jni/jni_common.h>
#include <Poco/StringTokenizer.h>
#include <Common/CurrentThread.h>
#include <Common/JNIUtils.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
namespace BroadCastJoinBuilder
{
static jclass Java_CHBroadcastBuildSideCache = nullptr;
static jmethodID Java_get = nullptr;
jlong callJavaGet(const std::string & id)
{
    GET_JNIENV(env)
    const jstring s = charTojstring(env, id.c_str());
    const auto result = safeCallStaticLongMethod(env, Java_CHBroadcastBuildSideCache, Java_get, s);
    CLEAN_JNIENV

    return result;
}

void cleanBuildHashTable(const std::string & hash_table_id, jlong instance)
{
    /// Thread status holds raw pointer on query context, thus it always must be destroyed
    /// It always called by no thread_status. We need create first.
    /// Otherwise global tracker will not free bhj memory.
    DB::ThreadStatus thread_status;
    SharedPointerWrapper<StorageJoinFromReadBuffer>::dispose(instance);
    LOG_DEBUG(&Poco::Logger::get("BroadCastJoinBuilder"), "Broadcast hash table {} is cleaned", hash_table_id);
}

std::shared_ptr<StorageJoinFromReadBuffer> getJoin(const std::string & key)
{
    const jlong result = callJavaGet(key);

    if (unlikely(result == 0))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "broadcast table {} not found in cache.", key);

    auto wrapper = SharedPointerWrapper<StorageJoinFromReadBuffer>::sharedPtr(result);
    if (unlikely(!wrapper))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "broadcast table {} not found, cache value is invalidated.", key);

    return wrapper;
}

std::shared_ptr<StorageJoinFromReadBuffer> buildJoin(
    const std::string & key,
    DB::ReadBuffer & input,
    jlong row_count,
    const std::string & join_keys,
    substrait::JoinRel_JoinType join_type,
    const std::string & named_struct)
{
    auto join_key_list = Poco::StringTokenizer(join_keys, ",");
    Names key_names;
    for (const auto & key_name : join_key_list)
        key_names.emplace_back(key_name);
    DB::JoinKind kind;
    DB::JoinStrictness strictness;

    std::tie(kind, strictness) = getJoinKindAndStrictness(join_type);

    substrait::NamedStruct substrait_struct;
    substrait_struct.ParseFromString(named_struct);
    Block header = TypeParser::buildBlockFromNamedStruct(substrait_struct);
    ColumnsDescription columns_description(header.getNamesAndTypesList());

    return make_shared<StorageJoinFromReadBuffer>(
        input,
        row_count,
        key_names,
        true,
        std::make_shared<DB::TableJoin>(SizeLimits(), true, kind, strictness, key_names),
        columns_description,
        ConstraintsDescription(),
        key,
        true);
}

void init(JNIEnv * env)
{
    /**
     * Scala object will be compiled into two classes, one is with '$' suffix which is normal class,
     * and one is utility class which only has static method.
     *
     * Here, we use utility class.
     */

    const char * classSig = "Lorg/apache/gluten/execution/CHBroadcastBuildSideCache;";
    Java_CHBroadcastBuildSideCache = CreateGlobalClassReference(env, classSig);
    Java_get = GetStaticMethodID(env, Java_CHBroadcastBuildSideCache, "get", "(Ljava/lang/String;)J");
}

void destroy(JNIEnv * env)
{
    env->DeleteGlobalRef(Java_CHBroadcastBuildSideCache);
}

}
}
