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
#include <numeric>
#include <string>
#include <jni.h>

#include <Builder/SerializedPlanBuilder.h>
#include <Compression/CompressedReadBuffer.h>
#include <DataTypes/DataTypeNullable.h>
#include <Join/BroadCastJoinBuilder.h>
#include <Operator/BlockCoalesceOperator.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/MergeTreeRelParser.h>
#include <Parser/RelParser.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/SparkRowToCHColumn.h>
#include <Shuffle/CachedShuffleWriter.h>
#include <Shuffle/NativeSplitter.h>
#include <Shuffle/NativeWriterInMemory.h>
#include <Shuffle/PartitionWriter.h>
#include <Shuffle/ShuffleReader.h>
#include <Shuffle/ShuffleSplitter.h>
#include <Shuffle/ShuffleWriter.h>
#include <Shuffle/ShuffleWriterBase.h>
#include <Shuffle/WriteBufferFromJavaOutputStream.h>
#include <Storages/Mergetree/SparkMergeTreeWriter.h>
#include <Storages/Output/BlockStripeSplitter.h>
#include <Storages/Output/FileWriterWrappers.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <jni/ReservationListenerWrapper.h>
#include <jni/SharedPointerWrapper.h>
#include <jni/jni_common.h>
#include <jni/jni_error.h>
#include <Poco/Logger.h>
#include <Poco/StringTokenizer.h>
#include <Common/CHUtil.h>
#include <Common/CurrentThread.h>
#include <Common/ExceptionUtils.h>
#include <Common/JNIUtils.h>
#include <Common/QueryContext.h>


#ifdef __cplusplus
namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}
}
static DB::ColumnWithTypeAndName getColumnFromColumnVector(JNIEnv * /*env*/, jobject /*obj*/, jlong block_address, jint column_position)
{
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    return block->getByPosition(column_position);
}

static std::string jstring2string(JNIEnv * env, jstring jStr)
{
    if (!jStr)
        return "";

    jclass string_class = env->GetObjectClass(jStr);
    jmethodID get_bytes = env->GetMethodID(string_class, "getBytes", "(Ljava/lang/String;)[B");
    jbyteArray string_jbytes
        = static_cast<jbyteArray>(local_engine::safeCallObjectMethod(env, jStr, get_bytes, env->NewStringUTF("UTF-8")));

    size_t length = static_cast<size_t>(env->GetArrayLength(string_jbytes));
    jbyte * p_bytes = env->GetByteArrayElements(string_jbytes, nullptr);

    std::string ret = std::string(reinterpret_cast<char *>(p_bytes), length);
    env->ReleaseByteArrayElements(string_jbytes, p_bytes, JNI_ABORT);

    env->DeleteLocalRef(string_jbytes);
    env->DeleteLocalRef(string_class);
    return ret;
}

static jstring stringTojstring(JNIEnv * env, const char * pat)
{
    jclass strClass = (env)->FindClass("java/lang/String");
    jmethodID ctorID = (env)->GetMethodID(strClass, "<init>", "([BLjava/lang/String;)V");
    jbyteArray bytes = (env)->NewByteArray(strlen(pat));
    (env)->SetByteArrayRegion(bytes, 0, strlen(pat), reinterpret_cast<const jbyte *>(pat));
    jstring encoding = (env)->NewStringUTF("UTF-8");
    return static_cast<jstring>((env)->NewObject(strClass, ctorID, bytes, encoding));
}

extern "C" {
#endif


namespace dbms
{
class LocalExecutor;
}

static jclass spark_row_info_class;
static jmethodID spark_row_info_constructor;

static jclass block_stripes_class;
static jmethodID block_stripes_constructor;

static jclass split_result_class;
static jmethodID split_result_constructor;

static jclass native_metrics_class;
static jmethodID native_metrics_constructor;

JNIEXPORT jint JNI_OnLoad(JavaVM * vm, void * /*reserved*/)
{
    JNIEnv * env;
    if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8) != JNI_OK)
        return JNI_ERR;

    local_engine::JniErrorsGlobalState::instance().initialize(env);

    spark_row_info_class = local_engine::CreateGlobalClassReference(env, "Lio/glutenproject/row/SparkRowInfo;");
    spark_row_info_constructor = local_engine::GetMethodID(env, spark_row_info_class, "<init>", "([J[JJJJ)V");

    block_stripes_class = local_engine::CreateGlobalClassReference(env, "Lorg/apache/spark/sql/execution/datasources/BlockStripes;");
    block_stripes_constructor = local_engine::GetMethodID(env, block_stripes_class, "<init>", "(J[J[II)V");

    split_result_class = local_engine::CreateGlobalClassReference(env, "Lio/glutenproject/vectorized/CHSplitResult;");
    split_result_constructor = local_engine::GetMethodID(env, split_result_class, "<init>", "(JJJJJJ[J[JJJJ)V");

    local_engine::ShuffleReader::input_stream_class
        = local_engine::CreateGlobalClassReference(env, "Lio/glutenproject/vectorized/ShuffleInputStream;");
    local_engine::NativeSplitter::iterator_class
        = local_engine::CreateGlobalClassReference(env, "Lio/glutenproject/vectorized/IteratorWrapper;");
    local_engine::WriteBufferFromJavaOutputStream::output_stream_class
        = local_engine::CreateGlobalClassReference(env, "Ljava/io/OutputStream;");
    local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class
        = local_engine::CreateGlobalClassReference(env, "Lio/glutenproject/execution/ColumnarNativeIterator;");
    local_engine::SourceFromJavaIter::serialized_record_batch_iterator_hasNext
        = local_engine::GetMethodID(env, local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class, "hasNext", "()Z");
    local_engine::SourceFromJavaIter::serialized_record_batch_iterator_next
        = local_engine::GetMethodID(env, local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class, "next", "()[B");

    local_engine::ShuffleReader::input_stream_read
        = local_engine::GetMethodID(env, local_engine::ShuffleReader::input_stream_class, "read", "(JJ)J");

    local_engine::NativeSplitter::iterator_has_next
        = local_engine::GetMethodID(env, local_engine::NativeSplitter::iterator_class, "hasNext", "()Z");
    local_engine::NativeSplitter::iterator_next
        = local_engine::GetMethodID(env, local_engine::NativeSplitter::iterator_class, "next", "()J");

    local_engine::WriteBufferFromJavaOutputStream::output_stream_write
        = local_engine::GetMethodID(env, local_engine::WriteBufferFromJavaOutputStream::output_stream_class, "write", "([BII)V");
    local_engine::WriteBufferFromJavaOutputStream::output_stream_flush
        = local_engine::GetMethodID(env, local_engine::WriteBufferFromJavaOutputStream::output_stream_class, "flush", "()V");


    local_engine::SparkRowToCHColumn::spark_row_interator_class
        = local_engine::CreateGlobalClassReference(env, "Lio/glutenproject/execution/SparkRowIterator;");
    local_engine::SparkRowToCHColumn::spark_row_interator_hasNext
        = local_engine::GetMethodID(env, local_engine::SparkRowToCHColumn::spark_row_interator_class, "hasNext", "()Z");
    local_engine::SparkRowToCHColumn::spark_row_interator_next
        = local_engine::GetMethodID(env, local_engine::SparkRowToCHColumn::spark_row_interator_class, "next", "()[B");
    local_engine::SparkRowToCHColumn::spark_row_iterator_nextBatch = local_engine::GetMethodID(
        env, local_engine::SparkRowToCHColumn::spark_row_interator_class, "nextBatch", "()Ljava/nio/ByteBuffer;");

    local_engine::ReservationListenerWrapper::reservation_listener_class
        = local_engine::CreateGlobalClassReference(env, "Lio/glutenproject/memory/alloc/CHReservationListener;");
    local_engine::ReservationListenerWrapper::reservation_listener_reserve
        = local_engine::GetMethodID(env, local_engine::ReservationListenerWrapper::reservation_listener_class, "reserve", "(J)J");
    local_engine::ReservationListenerWrapper::reservation_listener_reserve_or_throw
        = local_engine::GetMethodID(env, local_engine::ReservationListenerWrapper::reservation_listener_class, "reserveOrThrow", "(J)V");
    local_engine::ReservationListenerWrapper::reservation_listener_unreserve
        = local_engine::GetMethodID(env, local_engine::ReservationListenerWrapper::reservation_listener_class, "unreserve", "(J)J");

    native_metrics_class = local_engine::CreateGlobalClassReference(env, "Lio/glutenproject/metrics/NativeMetrics;");
    native_metrics_constructor = local_engine::GetMethodID(env, native_metrics_class, "<init>", "(Ljava/lang/String;)V");

    local_engine::BroadCastJoinBuilder::init(env);

    local_engine::JNIUtils::vm = vm;
    return JNI_VERSION_1_8;
}

JNIEXPORT void JNI_OnUnload(JavaVM * vm, void * /*reserved*/)
{
    local_engine::BackendFinalizerUtil::finalizeGlobally();

    JNIEnv * env;
    vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);

    local_engine::JniErrorsGlobalState::instance().destroy(env);
    local_engine::BroadCastJoinBuilder::destroy(env);

    env->DeleteGlobalRef(spark_row_info_class);
    env->DeleteGlobalRef(block_stripes_class);
    env->DeleteGlobalRef(split_result_class);
    env->DeleteGlobalRef(local_engine::ShuffleReader::input_stream_class);
    env->DeleteGlobalRef(local_engine::NativeSplitter::iterator_class);
    env->DeleteGlobalRef(local_engine::WriteBufferFromJavaOutputStream::output_stream_class);
    env->DeleteGlobalRef(local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class);
    env->DeleteGlobalRef(local_engine::SparkRowToCHColumn::spark_row_interator_class);
    env->DeleteGlobalRef(local_engine::ReservationListenerWrapper::reservation_listener_class);
    env->DeleteGlobalRef(native_metrics_class);
}

JNIEXPORT void Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative(JNIEnv * env, jobject, jbyteArray conf_plan)
{
    LOCAL_ENGINE_JNI_METHOD_START
    jsize plan_buf_size = env->GetArrayLength(conf_plan);
    jbyte * plan_buf_addr = env->GetByteArrayElements(conf_plan, nullptr);
    std::string plan_str;
    plan_str.assign(reinterpret_cast<const char *>(plan_buf_addr), plan_buf_size);
    local_engine::BackendInitializerUtil::init(&plan_str);
    env->ReleaseByteArrayElements(conf_plan, plan_buf_addr, JNI_ABORT);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeFinalizeNative(JNIEnv * env)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::BackendFinalizerUtil::finalizeSessionally();
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithIterator(
    JNIEnv * env,
    jobject /*obj*/,
    jlong allocator_id,
    jbyteArray plan,
    jobjectArray split_infos,
    jobjectArray iter_arr,
    jbyteArray conf_plan,
    jboolean materialize_input)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto query_context = local_engine::getAllocator(allocator_id)->query_context;

    // by task update new configs ( in case of dynamic config update )
    jsize plan_buf_size = env->GetArrayLength(conf_plan);
    jbyte * plan_buf_addr = env->GetByteArrayElements(conf_plan, nullptr);
    std::string plan_str;
    plan_str.assign(reinterpret_cast<const char *>(plan_buf_addr), plan_buf_size);
    local_engine::BackendInitializerUtil::updateConfig(query_context, &plan_str);

    local_engine::SerializedPlanParser parser(query_context);
    jsize iter_num = env->GetArrayLength(iter_arr);
    for (jsize i = 0; i < iter_num; i++)
    {
        jobject iter = env->GetObjectArrayElement(iter_arr, i);
        iter = env->NewGlobalRef(iter);
        parser.addInputIter(iter, materialize_input);
    }

    for (jsize i = 0, split_info_arr_size = env->GetArrayLength(split_infos); i < split_info_arr_size; i++) {
        jbyteArray split_info = static_cast<jbyteArray>(env->GetObjectArrayElement(split_infos, i));
        jsize split_info_size = env->GetArrayLength(split_info);
        jbyte * split_info_addr = env->GetByteArrayElements(split_info, nullptr);
        std::string split_info_str;
        split_info_str.assign(reinterpret_cast<const char *>(split_info_addr), split_info_size);
        parser.addSplitInfo(split_info_str);
    }

    jsize plan_size = env->GetArrayLength(plan);
    jbyte * plan_address = env->GetByteArrayElements(plan, nullptr);
    std::string plan_string;
    plan_string.assign(reinterpret_cast<const char *>(plan_address), plan_size);
    auto query_plan = parser.parse(plan_string);
    local_engine::LocalExecutor * executor = new local_engine::LocalExecutor(parser.query_context, query_context);
    executor->setMetric(parser.getMetric());
    executor->setExtraPlanHolder(parser.extra_plan_holder);
    executor->execute(std::move(query_plan));
    env->ReleaseByteArrayElements(plan, plan_address, JNI_ABORT);
    env->ReleaseByteArrayElements(conf_plan, plan_buf_addr, JNI_ABORT);
    return reinterpret_cast<jlong>(executor);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jboolean Java_io_glutenproject_row_RowIterator_nativeHasNext(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    return executor->hasNext();
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jobject Java_io_glutenproject_row_RowIterator_nativeNext(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    local_engine::SparkRowInfoPtr spark_row_info = executor->next();

    auto * offsets_arr = env->NewLongArray(spark_row_info->getNumRows());
    const auto * offsets_src = reinterpret_cast<const jlong *>(spark_row_info->getOffsets().data());
    env->SetLongArrayRegion(offsets_arr, 0, spark_row_info->getNumRows(), offsets_src);
    auto * lengths_arr = env->NewLongArray(spark_row_info->getNumRows());
    const auto * lengths_src = reinterpret_cast<const jlong *>(spark_row_info->getLengths().data());
    env->SetLongArrayRegion(lengths_arr, 0, spark_row_info->getNumRows(), lengths_src);
    int64_t address = reinterpret_cast<int64_t>(spark_row_info->getBufferAddress());
    int64_t column_number = reinterpret_cast<int64_t>(spark_row_info->getNumCols());
    int64_t total_size = reinterpret_cast<int64_t>(spark_row_info->getTotalBytes());

    jobject spark_row_info_object
        = env->NewObject(spark_row_info_class, spark_row_info_constructor, offsets_arr, lengths_arr, address, column_number, total_size);
    return spark_row_info_object;
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT void Java_io_glutenproject_row_RowIterator_nativeClose(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    delete executor;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

// Columnar Iterator
JNIEXPORT jboolean Java_io_glutenproject_vectorized_BatchIterator_nativeHasNext(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    return executor->hasNext();
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_BatchIterator_nativeCHNext(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    DB::Block * column_batch = executor->nextColumnar();
    // LOG_DEBUG(&Poco::Logger::get("jni"), "row size of the column batch: {}", column_batch->rows());
    return reinterpret_cast<Int64>(column_batch);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_io_glutenproject_vectorized_BatchIterator_nativeClose(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    delete executor;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jobject Java_io_glutenproject_vectorized_BatchIterator_nativeFetchMetrics(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    /// Collect metrics only if optimizations are disabled, otherwise coredump would happen.
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    auto metric = executor->getMetric();
    String metrics_json = metric ? local_engine::RelMetricSerializer::serializeRelMetric(metric) : "";
    LOG_DEBUG(&Poco::Logger::get("jni"), "{}", metrics_json);
    jobject native_metrics = env->NewObject(native_metrics_class, native_metrics_constructor, stringTojstring(env, metrics_json.c_str()));
    return native_metrics;
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT void
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetJavaTmpDir(JNIEnv * /*env*/, jobject /*obj*/, jstring /*dir*/)
{
}

JNIEXPORT void
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetBatchSize(JNIEnv * /*env*/, jobject /*obj*/, jint /*batch_size*/)
{
}

JNIEXPORT void Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMetricsTime(
    JNIEnv * /*env*/, jobject /*obj*/, jboolean /*setMetricsTime*/)
{
}

JNIEXPORT jboolean
Java_io_glutenproject_vectorized_CHColumnVector_nativeHasNull(JNIEnv * env, jobject obj, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    if (!col.column->isNullable())
    {
        return false;
    }
    else
    {
        const auto * nullable = checkAndGetColumn<DB::ColumnNullable>(*col.column);
        size_t num_nulls = std::accumulate(nullable->getNullMapData().begin(), nullable->getNullMapData().end(), 0);
        return num_nulls < block->rows();
    }
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jint
Java_io_glutenproject_vectorized_CHColumnVector_nativeNumNulls(JNIEnv * env, jobject obj, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    if (!col.column->isNullable())
    {
        return 0;
    }
    else
    {
        const auto * nullable = checkAndGetColumn<DB::ColumnNullable>(*col.column);
        return std::accumulate(nullable->getNullMapData().begin(), nullable->getNullMapData().end(), 0);
    }
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jboolean Java_io_glutenproject_vectorized_CHColumnVector_nativeIsNullAt(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    return col.column->isNullAt(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jboolean Java_io_glutenproject_vectorized_CHColumnVector_nativeGetBoolean(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return nested_col->getBool(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jbyte Java_io_glutenproject_vectorized_CHColumnVector_nativeGetByte(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return reinterpret_cast<const jbyte *>(nested_col->getDataAt(row_id).data)[0];
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}

JNIEXPORT jshort Java_io_glutenproject_vectorized_CHColumnVector_nativeGetShort(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return reinterpret_cast<const jshort *>(nested_col->getDataAt(row_id).data)[0];
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jint Java_io_glutenproject_vectorized_CHColumnVector_nativeGetInt(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    if (col.type->getTypeId() == DB::TypeIndex::Date)
        return nested_col->getUInt(row_id);
    else
        return nested_col->getInt(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_CHColumnVector_nativeGetLong(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return nested_col->getInt(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jfloat Java_io_glutenproject_vectorized_CHColumnVector_nativeGetFloat(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return nested_col->getFloat32(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, 0.0)
}

JNIEXPORT jdouble Java_io_glutenproject_vectorized_CHColumnVector_nativeGetDouble(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return nested_col->getFloat64(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, 0.0)
}

JNIEXPORT jstring Java_io_glutenproject_vectorized_CHColumnVector_nativeGetString(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    const auto * string_col = checkAndGetColumn<DB::ColumnString>(nested_col.get());
    auto result = string_col->getDataAt(row_id);
    return local_engine::charTojstring(env, result.toString().c_str());
    LOCAL_ENGINE_JNI_METHOD_END(env, local_engine::charTojstring(env, ""))
}

// native block
JNIEXPORT void Java_io_glutenproject_vectorized_CHNativeBlock_nativeClose(JNIEnv * /*env*/, jobject /*obj*/, jlong /*block_address*/)
{
}

JNIEXPORT jint Java_io_glutenproject_vectorized_CHNativeBlock_nativeNumRows(JNIEnv * env, jobject /*obj*/, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    return block->rows();
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jint Java_io_glutenproject_vectorized_CHNativeBlock_nativeNumColumns(JNIEnv * env, jobject /*obj*/, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    return block->columns();
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jbyteArray
Java_io_glutenproject_vectorized_CHNativeBlock_nativeColumnType(JNIEnv * env, jobject /*obj*/, jlong block_address, jint position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    const auto & col = block->getByPosition(position);
    std::string substrait_type;
    dbms::SerializedPlanBuilder::buildType(col.type, substrait_type);
    return local_engine::stringTojbyteArray(env, substrait_type);
    LOCAL_ENGINE_JNI_METHOD_END(env, local_engine::stringTojbyteArray(env, ""))
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_CHNativeBlock_nativeTotalBytes(JNIEnv * env, jobject /*obj*/, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    return block->bytes();
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_CHStreamReader_createNativeShuffleReader(
    JNIEnv * env, jclass /*clazz*/, jobject input_stream, jboolean compressed, jlong max_shuffle_read_rows, jlong max_shuffle_read_bytes)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * input = env->NewGlobalRef(input_stream);
    auto read_buffer = std::make_unique<local_engine::ReadBufferFromJavaInputStream>(input);
    auto * shuffle_reader
        = new local_engine::ShuffleReader(std::move(read_buffer), compressed, max_shuffle_read_rows, max_shuffle_read_bytes);
    return reinterpret_cast<jlong>(shuffle_reader);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_CHStreamReader_nativeNext(JNIEnv * env, jobject /*obj*/, jlong shuffle_reader)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleReader * reader = reinterpret_cast<local_engine::ShuffleReader *>(shuffle_reader);
    DB::Block * block = reader->read();
    return reinterpret_cast<jlong>(block);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_io_glutenproject_vectorized_CHStreamReader_nativeClose(JNIEnv * env, jobject /*obj*/, jlong shuffle_reader)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleReader * reader = reinterpret_cast<local_engine::ShuffleReader *>(shuffle_reader);
    delete reader;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_CHCoalesceOperator_createNativeOperator(JNIEnv * env, jobject /*obj*/, jint buf_size)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::BlockCoalesceOperator * instance = new local_engine::BlockCoalesceOperator(buf_size);
    return reinterpret_cast<jlong>(instance);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_io_glutenproject_vectorized_CHCoalesceOperator_nativeMergeBlock(
    JNIEnv * env, jobject /*obj*/, jlong instance_address, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::BlockCoalesceOperator * instance = reinterpret_cast<local_engine::BlockCoalesceOperator *>(instance_address);
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    auto new_block = DB::Block(*block);
    instance->mergeBlock(new_block);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jboolean Java_io_glutenproject_vectorized_CHCoalesceOperator_nativeIsFull(JNIEnv * env, jobject /*obj*/, jlong instance_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::BlockCoalesceOperator * instance = reinterpret_cast<local_engine::BlockCoalesceOperator *>(instance_address);
    bool full = instance->isFull();
    return full ? JNI_TRUE : JNI_FALSE;
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_CHCoalesceOperator_nativeRelease(JNIEnv * env, jobject /*obj*/, jlong instance_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::BlockCoalesceOperator * instance = reinterpret_cast<local_engine::BlockCoalesceOperator *>(instance_address);
    auto * block = instance->releaseBlock();
    Int64 address = reinterpret_cast<jlong>(block);
    return address;
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_io_glutenproject_vectorized_CHCoalesceOperator_nativeClose(JNIEnv * env, jobject /*obj*/, jlong instance_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::BlockCoalesceOperator * instance = reinterpret_cast<local_engine::BlockCoalesceOperator *>(instance_address);
    delete instance;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

// Splitter Jni Wrapper
JNIEXPORT jlong Java_io_glutenproject_vectorized_CHShuffleSplitterJniWrapper_nativeMake(
    JNIEnv * env,
    jobject,
    jstring short_name,
    jint num_partitions,
    jbyteArray expr_list,
    jbyteArray out_expr_list,
    jint shuffle_id,
    jlong map_id,
    jint split_size,
    jstring codec,
    jstring data_file,
    jstring local_dirs,
    jint num_sub_dirs,
    jboolean prefer_spill,
    jlong spill_threshold,
    jstring hash_algorithm,
    jboolean throw_if_memory_exceed,
    jboolean flush_block_buffer_before_evict)
{
    LOCAL_ENGINE_JNI_METHOD_START
    std::string hash_exprs;
    std::string out_exprs;
    if (expr_list != nullptr)
    {
        int len = env->GetArrayLength(expr_list);
        auto * str = reinterpret_cast<jbyte *>(new char[len]);
        memset(str, 0, len);
        env->GetByteArrayRegion(expr_list, 0, len, str);
        hash_exprs = std::string(str, str + len);
        delete[] str;
    }

    if (out_expr_list != nullptr)
    {
        int len = env->GetArrayLength(out_expr_list);
        auto * str = reinterpret_cast<jbyte *>(new char[len]);
        memset(str, 0, len);
        env->GetByteArrayRegion(out_expr_list, 0, len, str);
        out_exprs = std::string(str, str + len);
        delete[] str;
    }

    Poco::StringTokenizer local_dirs_tokenizer(jstring2string(env, local_dirs), ",");
    std::vector<std::string> local_dirs_list;
    local_dirs_list.insert(local_dirs_list.end(), local_dirs_tokenizer.begin(), local_dirs_tokenizer.end());

    local_engine::SplitOptions options{
        .split_size = static_cast<size_t>(split_size),
        .io_buffer_size = DB::DBMS_DEFAULT_BUFFER_SIZE,
        .data_file = jstring2string(env, data_file),
        .local_dirs_list = std::move(local_dirs_list),
        .num_sub_dirs = num_sub_dirs,
        .shuffle_id = shuffle_id,
        .map_id = static_cast<int>(map_id),
        .partition_num = static_cast<size_t>(num_partitions),
        .hash_exprs = hash_exprs,
        .out_exprs = out_exprs,
        .compress_method = jstring2string(env, codec),
        .spill_threshold = static_cast<size_t>(spill_threshold),
        .hash_algorithm = jstring2string(env, hash_algorithm),
        .throw_if_memory_exceed = static_cast<bool>(throw_if_memory_exceed),
        .flush_block_buffer_before_evict = static_cast<bool>(flush_block_buffer_before_evict)};
    auto name = jstring2string(env, short_name);
    local_engine::SplitterHolder * splitter;
    if (prefer_spill)
        splitter = new local_engine::SplitterHolder{.splitter = local_engine::ShuffleSplitter::create(name, options)};
    else
        splitter = new local_engine::SplitterHolder{.splitter = std::make_unique<local_engine::CachedShuffleWriter>(name, options)};
    return reinterpret_cast<jlong>(splitter);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_CHShuffleSplitterJniWrapper_nativeMakeForRSS(
    JNIEnv * env,
    jobject,
    jstring short_name,
    jint num_partitions,
    jbyteArray expr_list,
    jbyteArray out_expr_list,
    jint shuffle_id,
    jlong map_id,
    jint split_size,
    jstring codec,
    jlong spill_threshold,
    jstring hash_algorithm,
    jobject pusher,
    jboolean throw_if_memory_exceed,
    jboolean flush_block_buffer_before_evict)
{
    LOCAL_ENGINE_JNI_METHOD_START
    std::string hash_exprs;
    std::string out_exprs;
    if (expr_list != nullptr)
    {
        int len = env->GetArrayLength(expr_list);
        auto * str = reinterpret_cast<jbyte *>(new char[len]);
        env->GetByteArrayRegion(expr_list, 0, len, str);
        hash_exprs = std::string(str, str + len);
        delete[] str;
    }

    if (out_expr_list != nullptr)
    {
        int len = env->GetArrayLength(out_expr_list);
        auto * str = reinterpret_cast<jbyte *>(new char[len]);
        env->GetByteArrayRegion(out_expr_list, 0, len, str);
        out_exprs = std::string(str, str + len);
        delete[] str;
    }

    local_engine::SplitOptions options{
        .split_size = static_cast<size_t>(split_size),
        .io_buffer_size = DB::DBMS_DEFAULT_BUFFER_SIZE,
        .shuffle_id = shuffle_id,
        .map_id = static_cast<int>(map_id),
        .partition_num = static_cast<size_t>(num_partitions),
        .hash_exprs = hash_exprs,
        .out_exprs = out_exprs,
        .compress_method = jstring2string(env, codec),
        .spill_threshold = static_cast<size_t>(spill_threshold),
        .hash_algorithm = jstring2string(env, hash_algorithm),
        .throw_if_memory_exceed = static_cast<bool>(throw_if_memory_exceed),
        .flush_block_buffer_before_evict = static_cast<bool>(flush_block_buffer_before_evict)};
    auto name = jstring2string(env, short_name);
    local_engine::SplitterHolder * splitter;
    splitter = new local_engine::SplitterHolder{.splitter = std::make_unique<local_engine::CachedShuffleWriter>(name, options, pusher)};
    return reinterpret_cast<jlong>(splitter);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_io_glutenproject_vectorized_CHShuffleSplitterJniWrapper_split(JNIEnv * env, jobject, jlong splitterId, jlong block)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::SplitterHolder * splitter = reinterpret_cast<local_engine::SplitterHolder *>(splitterId);
    DB::Block * data = reinterpret_cast<DB::Block *>(block);
    splitter->splitter->split(*data);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_CHShuffleSplitterJniWrapper_evict(JNIEnv * env, jobject, jlong splitterId)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::SplitterHolder * splitter = reinterpret_cast<local_engine::SplitterHolder *>(splitterId);
    auto size = splitter->splitter->evictPartitions();
    return size;
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}

JNIEXPORT jobject Java_io_glutenproject_vectorized_CHShuffleSplitterJniWrapper_stop(JNIEnv * env, jobject, jlong splitterId)
{
    LOCAL_ENGINE_JNI_METHOD_START

    local_engine::SplitterHolder * splitter = reinterpret_cast<local_engine::SplitterHolder *>(splitterId);
    auto result = splitter->splitter->stop();

    const auto & partition_lengths = result.partition_lengths;
    auto * partition_length_arr = env->NewLongArray(partition_lengths.size());
    const auto * src = reinterpret_cast<const jlong *>(partition_lengths.data());
    env->SetLongArrayRegion(partition_length_arr, 0, partition_lengths.size(), src);

    const auto & raw_partition_lengths = result.raw_partition_lengths;
    auto * raw_partition_length_arr = env->NewLongArray(raw_partition_lengths.size());
    const auto * raw_src = reinterpret_cast<const jlong *>(raw_partition_lengths.data());
    env->SetLongArrayRegion(raw_partition_length_arr, 0, raw_partition_lengths.size(), raw_src);

    jobject split_result = env->NewObject(
        split_result_class,
        split_result_constructor,
        result.total_compute_pid_time,
        result.total_write_time,
        result.total_spill_time,
        result.total_compress_time,
        result.total_bytes_written,
        result.total_bytes_spilled,
        partition_length_arr,
        raw_partition_length_arr,
        result.total_split_time,
        result.total_io_time,
        result.total_serialize_time);

    return split_result;
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT void Java_io_glutenproject_vectorized_CHShuffleSplitterJniWrapper_close(JNIEnv * env, jobject, jlong splitterId)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::SplitterHolder * splitter = reinterpret_cast<local_engine::SplitterHolder *>(splitterId);
    delete splitter;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

// CHBlockConverterJniWrapper
JNIEXPORT jobject
Java_io_glutenproject_vectorized_CHBlockConverterJniWrapper_convertColumnarToRow(JNIEnv * env, jclass, jlong block_address, jintArray masks)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::CHColumnToSparkRow converter;

    std::unique_ptr<local_engine::SparkRowInfo> spark_row_info = nullptr;
    local_engine::MaskVector mask = nullptr;
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    if (masks != nullptr)
    {
        jint size = env->GetArrayLength(masks);
        jboolean is_cp = JNI_FALSE;
        jint * values = env->GetIntArrayElements(masks, &is_cp);
        mask = std::make_unique<std::vector<size_t>>();
        for (int j = 0; j < size; j++)
            mask->push_back(values[j]);
        env->ReleaseIntArrayElements(masks, values, JNI_ABORT);
    }
    spark_row_info = converter.convertCHColumnToSparkRow(*block, mask);

    auto * offsets_arr = env->NewLongArray(spark_row_info->getNumRows());
    const auto * offsets_src = reinterpret_cast<const jlong *>(spark_row_info->getOffsets().data());
    env->SetLongArrayRegion(offsets_arr, 0, spark_row_info->getNumRows(), offsets_src);
    auto * lengths_arr = env->NewLongArray(spark_row_info->getNumRows());
    const auto * lengths_src = reinterpret_cast<const jlong *>(spark_row_info->getLengths().data());
    env->SetLongArrayRegion(lengths_arr, 0, spark_row_info->getNumRows(), lengths_src);
    int64_t address = reinterpret_cast<int64_t>(spark_row_info->getBufferAddress());
    int64_t column_number = reinterpret_cast<int64_t>(spark_row_info->getNumCols());
    int64_t total_size = reinterpret_cast<int64_t>(spark_row_info->getTotalBytes());

    jobject spark_row_info_object
        = env->NewObject(spark_row_info_class, spark_row_info_constructor, offsets_arr, lengths_arr, address, column_number, total_size);
    return spark_row_info_object;
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT void Java_io_glutenproject_vectorized_CHBlockConverterJniWrapper_freeMemory(JNIEnv * env, jclass, jlong address, jlong size)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::CHColumnToSparkRow converter;
    converter.freeMem(reinterpret_cast<char *>(address), size);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_CHBlockConverterJniWrapper_convertSparkRowsToCHColumn(
    JNIEnv * env, jclass, jobject java_iter, jobjectArray names, jobjectArray types)
{
    LOCAL_ENGINE_JNI_METHOD_START
    using namespace std;

    int num_columns = env->GetArrayLength(names);
    vector<string> c_names;
    vector<string> c_types;
    c_names.reserve(num_columns);
    for (int i = 0; i < num_columns; i++)
    {
        auto * name = static_cast<jstring>(env->GetObjectArrayElement(names, i));
        c_names.emplace_back(jstring2string(env, name));

        auto * type = static_cast<jbyteArray>(env->GetObjectArrayElement(types, i));
        auto type_length = env->GetArrayLength(type);
        jbyte * type_ptr = env->GetByteArrayElements(type, nullptr);
        string str_type(reinterpret_cast<const char *>(type_ptr), type_length);
        c_types.emplace_back(std::move(str_type));

        env->ReleaseByteArrayElements(type, type_ptr, JNI_ABORT);
        env->DeleteLocalRef(name);
        env->DeleteLocalRef(type);
    }
    local_engine::SparkRowToCHColumn converter;
    auto * block = converter.convertSparkRowItrToCHColumn(java_iter, c_names, c_types);
    return reinterpret_cast<jlong>(block);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_io_glutenproject_vectorized_CHBlockConverterJniWrapper_freeBlock(JNIEnv * env, jclass, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::SparkRowToCHColumn converter;
    converter.freeBlock(reinterpret_cast<DB::Block *>(block_address));
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_CHBlockWriterJniWrapper_nativeCreateInstance(JNIEnv * env, jobject)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = new local_engine::NativeWriterInMemory();
    return reinterpret_cast<jlong>(writer);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void
Java_io_glutenproject_vectorized_CHBlockWriterJniWrapper_nativeWrite(JNIEnv * env, jobject, jlong instance, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::NativeWriterInMemory *>(instance);
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    writer->write(*block);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jint Java_io_glutenproject_vectorized_CHBlockWriterJniWrapper_nativeResultSize(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::NativeWriterInMemory *>(instance);
    return static_cast<jint>(writer->collect().size());
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void
Java_io_glutenproject_vectorized_CHBlockWriterJniWrapper_nativeCollect(JNIEnv * env, jobject, jlong instance, jbyteArray result)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::NativeWriterInMemory *>(instance);
    auto data = writer->collect();
    env->SetByteArrayRegion(result, 0, data.size(), reinterpret_cast<const jbyte *>(data.data()));
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_io_glutenproject_vectorized_CHBlockWriterJniWrapper_nativeClose(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::NativeWriterInMemory *>(instance);
    delete writer;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_nativeInitFileWriterWrapper(
    JNIEnv * env, jobject, jstring file_uri_, jobjectArray names_, jstring format_hint_)
{
    LOCAL_ENGINE_JNI_METHOD_START
    int num_columns = env->GetArrayLength(names_);
    std::vector<std::string> names;
    names.reserve(num_columns);
    for (int i = 0; i < num_columns; i++)
    {
        auto * name = static_cast<jstring>(env->GetObjectArrayElement(names_, i));
        names.emplace_back(jstring2string(env, name));
        env->DeleteLocalRef(name);
    }
    auto file_uri = jstring2string(env, file_uri_);
    auto format_hint = jstring2string(env, format_hint_);
    // for HiveFileFormat, the file url may not end with .parquet, so we pass in the format as a hint
    auto * writer = local_engine::createFileWriterWrapper(file_uri, names, format_hint);
    return reinterpret_cast<jlong>(writer);
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}

JNIEXPORT jlong Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_nativeInitMergeTreeWriterWrapper(
    JNIEnv * env, jobject, jbyteArray plan_, jbyteArray split_info_, jstring uuid_, jstring task_id_, jstring partition_dir_, jstring bucket_dir_)
{
    LOCAL_ENGINE_JNI_METHOD_START
    const auto uuid_str = jstring2string(env, uuid_);
    const auto task_id = jstring2string(env, task_id_);
    const auto partition_dir = jstring2string(env, partition_dir_);
    const auto bucket_dir = jstring2string(env, bucket_dir_);

    jsize plan_buf_size = env->GetArrayLength(plan_);
    jbyte * plan_buf_addr = env->GetByteArrayElements(plan_, nullptr);
    std::string plan_str;
    plan_str.assign(reinterpret_cast<const char *>(plan_buf_addr), plan_buf_size);

    jsize split_info_size = env->GetArrayLength(split_info_);
    jbyte * split_info_addr = env->GetByteArrayElements(split_info_, nullptr);
    std::string split_info_str;
    split_info_str.assign(reinterpret_cast<const char *>(split_info_addr), split_info_size);

    auto plan_ptr = std::make_unique<substrait::Plan>();
    /// https://stackoverflow.com/questions/52028583/getting-error-parsing-protobuf-data
    /// Parsing may fail when the number of recursive layers is large.
    /// Here, set a limit large enough to avoid this problem.
    /// Once this problem occurs, it is difficult to troubleshoot, because the pb of c++ will not provide any valid information
    google::protobuf::io::CodedInputStream coded_in(
        reinterpret_cast<const uint8_t *>(plan_str.data()), static_cast<int>(plan_str.size()));
    coded_in.SetRecursionLimit(100000);

    auto ok = plan_ptr->ParseFromCodedStream(&coded_in);
    if (!ok)
        throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse substrait::Plan from string failed");

    substrait::ReadRel::ExtensionTable extension_table =
        local_engine::SerializedPlanParser::parseExtensionTable(split_info_str);

    auto storage = local_engine::MergeTreeRelParser::parseStorage(
        extension_table, local_engine::SerializedPlanParser::global_context);
    auto uuid = uuid_str + "_" + task_id;
    auto * writer = new local_engine::SparkMergeTreeWriter(
        *storage, storage->getInMemoryMetadataPtr(), local_engine::SerializedPlanParser::global_context, uuid, partition_dir, bucket_dir);

    env->ReleaseByteArrayElements(plan_, plan_buf_addr, JNI_ABORT);
    env->ReleaseByteArrayElements(split_info_, split_info_addr, JNI_ABORT);
    return reinterpret_cast<jlong>(writer);
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}

JNIEXPORT void
Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_write(JNIEnv * env, jobject, jlong instanceId, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START

    auto * writer = reinterpret_cast<local_engine::NormalFileWriter *>(instanceId);
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    writer->consume(*block);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_close(JNIEnv * env, jobject, jlong instanceId)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::NormalFileWriter *>(instanceId);
    writer->close();
    delete writer;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void
Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_writeToMergeTree(JNIEnv * env, jobject, jlong instanceId, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::SparkMergeTreeWriter *>(instanceId);
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    writer->write(*block);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jstring Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_closeMergeTreeWriter(JNIEnv * env, jobject, jlong instanceId)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::SparkMergeTreeWriter *>(instanceId);
    writer->finalize();
    auto part_infos = writer->getAllPartInfo();
    auto json_info = local_engine::SparkMergeTreeWriter::partInfosToJson(part_infos);
    delete writer;
    return stringTojstring(env, json_info.c_str());
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT jobject Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_splitBlockByPartitionAndBucket(
    JNIEnv * env, jclass, jlong blockAddress, jintArray partitionColIndice, jboolean hasBucket, jboolean reserve_partition_columns)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * block = reinterpret_cast<DB::Block *>(blockAddress);
    int * pIndice = env->GetIntArrayElements(partitionColIndice, nullptr);
    int size = env->GetArrayLength(partitionColIndice);

    std::vector<size_t> partition_col_indice_vec;
    for (int i = 0; i < size; ++i)
        partition_col_indice_vec.push_back(pIndice[i]);

    env->ReleaseIntArrayElements(partitionColIndice, pIndice, JNI_ABORT);
    local_engine::BlockStripes bs = local_engine::BlockStripeSplitter::split(*block, partition_col_indice_vec, hasBucket, reserve_partition_columns);


    auto * addresses = env->NewLongArray(bs.block_addresses.size());
    env->SetLongArrayRegion(addresses, 0, bs.block_addresses.size(), bs.block_addresses.data());
    auto * indices = env->NewIntArray(bs.heading_row_indice.size());
    env->SetIntArrayRegion(indices, 0, bs.heading_row_indice.size(), bs.heading_row_indice.data());

    jobject block_stripes = env->NewObject(
        block_stripes_class, block_stripes_constructor, bs.origin_block_address, addresses, indices, bs.origin_block_num_columns);
    return block_stripes;

    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_StorageJoinBuilder_nativeBuild(
    JNIEnv * env, jclass, jstring key, jbyteArray in, jlong row_count_, jstring join_key_, jint join_type_, jbyteArray named_struct)
{
    LOCAL_ENGINE_JNI_METHOD_START
    const auto hash_table_id = jstring2string(env, key);
    const auto join_key = jstring2string(env, join_key_);
    const jsize struct_size = env->GetArrayLength(named_struct);
    jbyte * struct_address = env->GetByteArrayElements(named_struct, nullptr);
    std::string struct_string;
    struct_string.assign(reinterpret_cast<const char *>(struct_address), struct_size);
    const auto join_type = static_cast<substrait::JoinRel_JoinType>(join_type_);
    const jsize length = env->GetArrayLength(in);
    local_engine::ReadBufferFromByteArray read_buffer_from_java_array(in, length);
    DB::CompressedReadBuffer input(read_buffer_from_java_array);
    local_engine::configureCompressedReadBuffer(input);
    const auto * obj
        = make_wrapper(local_engine::BroadCastJoinBuilder::buildJoin(hash_table_id, input, row_count_, join_key, join_type, struct_string));
    env->ReleaseByteArrayElements(named_struct, struct_address, JNI_ABORT);
    return obj->instance();
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_StorageJoinBuilder_nativeCloneBuildHashTable(JNIEnv * env, jclass, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * cloned
        = local_engine::make_wrapper(local_engine::SharedPointerWrapper<local_engine::StorageJoinFromReadBuffer>::sharedPtr(instance));
    return cloned->instance();
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}

JNIEXPORT void
Java_io_glutenproject_vectorized_StorageJoinBuilder_nativeCleanBuildHashTable(JNIEnv * env, jclass, jstring hash_table_id_, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto hash_table_id = jstring2string(env, hash_table_id_);
    local_engine::BroadCastJoinBuilder::cleanBuildHashTable(hash_table_id, instance);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

// BlockSplitIterator
JNIEXPORT jlong Java_io_glutenproject_vectorized_BlockSplitIterator_nativeCreate(
    JNIEnv * env,
    jobject,
    jobject in,
    jstring name,
    jstring expr,
    jstring schema,
    jint partition_num,
    jint buffer_size,
    jstring hash_algorithm)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::NativeSplitter::Options options;
    options.partition_num = partition_num;
    options.buffer_size = buffer_size;
    auto hash_algorithm_str = jstring2string(env, hash_algorithm);
    options.hash_algorithm.swap(hash_algorithm_str);
    auto expr_str = jstring2string(env, expr);
    std::string schema_str;
    if (schema)
        schema_str = jstring2string(env, schema);
    options.exprs_buffer.swap(expr_str);
    options.schema_buffer.swap(schema_str);
    local_engine::NativeSplitter::Holder * splitter = new local_engine::NativeSplitter::Holder{
        .splitter = local_engine::NativeSplitter::create(jstring2string(env, name), options, in)};
    return reinterpret_cast<jlong>(splitter);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_io_glutenproject_vectorized_BlockSplitIterator_nativeClose(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::NativeSplitter::Holder * splitter = reinterpret_cast<local_engine::NativeSplitter::Holder *>(instance);
    delete splitter;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jboolean Java_io_glutenproject_vectorized_BlockSplitIterator_nativeHasNext(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::NativeSplitter::Holder * splitter = reinterpret_cast<local_engine::NativeSplitter::Holder *>(instance);
    return splitter->splitter->hasNext();
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_BlockSplitIterator_nativeNext(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::NativeSplitter::Holder * splitter = reinterpret_cast<local_engine::NativeSplitter::Holder *>(instance);
    return reinterpret_cast<jlong>(splitter->splitter->next());
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jint Java_io_glutenproject_vectorized_BlockSplitIterator_nativeNextPartitionId(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::NativeSplitter::Holder * splitter = reinterpret_cast<local_engine::NativeSplitter::Holder *>(instance);
    return reinterpret_cast<jint>(splitter->splitter->nextPartitionId());
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_BlockOutputStream_nativeCreate(
    JNIEnv * env, jobject, jobject output_stream, jbyteArray buffer, jstring codec, jboolean compressed, jint customize_buffer_size)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleWriter * writer
        = new local_engine::ShuffleWriter(output_stream, buffer, jstring2string(env, codec), compressed, customize_buffer_size);
    return reinterpret_cast<jlong>(writer);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_io_glutenproject_vectorized_BlockOutputStream_nativeClose(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleWriter * writer = reinterpret_cast<local_engine::ShuffleWriter *>(instance);
    writer->flush();
    delete writer;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_io_glutenproject_vectorized_BlockOutputStream_nativeWrite(JNIEnv * env, jobject, jlong instance, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleWriter * writer = reinterpret_cast<local_engine::ShuffleWriter *>(instance);
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    writer->write(*block);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_io_glutenproject_vectorized_BlockOutputStream_nativeFlush(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleWriter * writer = reinterpret_cast<local_engine::ShuffleWriter *>(instance);
    writer->flush();
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong
Java_io_glutenproject_vectorized_SimpleExpressionEval_createNativeInstance(JNIEnv * env, jclass, jobject input, jbyteArray plan)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto context = DB::Context::createCopy(local_engine::SerializedPlanParser::global_context);
    local_engine::SerializedPlanParser parser(context);
    jobject iter = env->NewGlobalRef(input);
    parser.addInputIter(iter, false);
    jsize plan_size = env->GetArrayLength(plan);
    jbyte * plan_address = env->GetByteArrayElements(plan, nullptr);
    std::string plan_string;
    plan_string.assign(reinterpret_cast<const char *>(plan_address), plan_size);
    auto query_plan = parser.parse(plan_string);
    local_engine::LocalExecutor * executor = new local_engine::LocalExecutor(parser.query_context, context);
    executor->execute(std::move(query_plan));
    env->ReleaseByteArrayElements(plan, plan_address, JNI_ABORT);
    return reinterpret_cast<jlong>(executor);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_io_glutenproject_vectorized_SimpleExpressionEval_nativeClose(JNIEnv * env, jclass, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(instance);
    delete executor;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jboolean Java_io_glutenproject_vectorized_SimpleExpressionEval_nativeHasNext(JNIEnv * env, jclass, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(instance);
    return executor->hasNext();
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jlong Java_io_glutenproject_vectorized_SimpleExpressionEval_nativeNext(JNIEnv * env, jclass, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(instance);
    return reinterpret_cast<jlong>(executor->nextColumnar());
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_io_glutenproject_memory_alloc_CHNativeMemoryAllocator_getDefaultAllocator(JNIEnv *, jclass)
{
    return -1;
}

JNIEXPORT jlong Java_io_glutenproject_memory_alloc_CHNativeMemoryAllocator_createListenableAllocator(JNIEnv * env, jclass, jobject listener)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto listener_wrapper = std::make_shared<local_engine::ReservationListenerWrapper>(env->NewGlobalRef(listener));
    return local_engine::initializeQuery(listener_wrapper);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_io_glutenproject_memory_alloc_CHNativeMemoryAllocator_releaseAllocator(JNIEnv * env, jclass, jlong allocator_id)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::releaseAllocator(allocator_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_io_glutenproject_memory_alloc_CHNativeMemoryAllocator_bytesAllocated(JNIEnv * env, jclass, jlong allocator_id)
{
    LOCAL_ENGINE_JNI_METHOD_START
    return local_engine::allocatorMemoryUsage(allocator_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

#ifdef __cplusplus
}

#endif
