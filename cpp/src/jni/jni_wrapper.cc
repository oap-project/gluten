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

#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/ipc/api.h>
#include <arrow/record_batch.h>
#include <arrow/util/compression.h>
#include <jni.h>
#include <malloc.h>

#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "compute/protobuf_utils.h"
#include "jni/concurrent_map.h"
#include "jni/exec_backend.h"
#include "jni/jni_common.h"
#include "jni/jni_errors.h"
#include "operators/c2r/columnar_to_row_base.h"
#include "operators/shuffle/splitter.h"
#include "utils/exception.h"
#include "utils/metrics.h"

namespace types {
class ExpressionList;
} // namespace types

static jclass serializable_obj_builder_class;
static jmethodID serializable_obj_builder_constructor;

jclass java_reservation_listener_class;

jmethodID reserve_memory_method;
jmethodID unreserve_memory_method;

static jclass byte_array_class;

static jclass split_result_class;
static jmethodID split_result_constructor;

static jclass serialized_arrow_array_iterator_class;
static jclass metrics_builder_class;
static jmethodID metrics_builder_constructor;

static jmethodID serialized_arrow_array_iterator_hasNext;
static jmethodID serialized_arrow_array_iterator_next;

static jclass native_columnar_to_row_info_class;
static jmethodID native_columnar_to_row_info_constructor;

jlong default_memory_allocator_id = -1L;

using arrow::jni::ConcurrentMap;

static arrow::jni::ConcurrentMap<
    std::shared_ptr<gluten::columnartorow::ColumnarToRowConverterBase>>
    columnar_to_row_converter_holder_;

using gluten::ArrowArrayResultIterator;
static arrow::jni::ConcurrentMap<std::shared_ptr<ArrowArrayResultIterator>>
    array_iterator_holder_;

using gluten::shuffle::SplitOptions;
using gluten::shuffle::Splitter;
static arrow::jni::ConcurrentMap<std::shared_ptr<Splitter>>
    shuffle_splitter_holder_;

std::shared_ptr<ArrowArrayResultIterator> GetArrayIterator(
    JNIEnv* env,
    jlong id) {
  auto handler = array_iterator_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    throw gluten::GlutenException(error_message);
  }
  return handler;
}

class JavaArrowArrayIterator {
 public:
  explicit JavaArrowArrayIterator(
      JavaVM* vm,
      jobject java_serialized_arrow_array_iterator)
      : vm_(vm),
        java_serialized_arrow_array_iterator_(
            java_serialized_arrow_array_iterator) {}

  // singleton, avoid stack instantiation
  JavaArrowArrayIterator(const JavaArrowArrayIterator& itr) = delete;
  JavaArrowArrayIterator(JavaArrowArrayIterator&& itr) = delete;

  virtual ~JavaArrowArrayIterator() {
    JNIEnv* env;
    int getEnvStat = vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
    if (getEnvStat == JNI_EDETACHED) {
      // Reattach current thread to JVM
      getEnvStat =
          vm_->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);
      if (getEnvStat != JNI_OK) {
        std::cout << "Failed to deconstruct due to thread not being reattached."
                  << std::endl;
        return;
      }
    }
    if (getEnvStat != JNI_OK) {
      std::cout << "Failed to deconstruct due to thread not being attached."
                << std::endl;
      return;
    }
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "DELETING ITERATOR REF "
              << reinterpret_cast<long>(java_serialized_arrow_array_iterator_)
              << "..." << std::endl;
#endif
    env->DeleteGlobalRef(java_serialized_arrow_array_iterator_);
    vm_->DetachCurrentThread();
  }

  arrow::Result<std::shared_ptr<ArrowArray>> Next() {
    JNIEnv* env;
    int getEnvStat = vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
    if (getEnvStat == JNI_EDETACHED) {
#ifdef GLUTEN_PRINT_DEBUG
      std::cout << "JNIEnv was not attached to current thread." << std::endl;
#endif
      if (vm_->AttachCurrentThreadAsDaemon(
              reinterpret_cast<void**>(&env), NULL) != 0) {
        return arrow::Status::Invalid("Failed to attach thread.");
      }
#ifdef GLUTEN_PRINT_DEBUG
      std::cout << "Succeeded attaching current thread." << std::endl;
#endif
    } else if (getEnvStat != JNI_OK) {
      return arrow::Status::Invalid(
          "JNIEnv was not attached to current thread");
    }
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "PICKING ITERATOR REF "
              << reinterpret_cast<long>(java_serialized_arrow_array_iterator_)
              << "..." << std::endl;
#endif
    if (!env->CallBooleanMethod(
            java_serialized_arrow_array_iterator_,
            serialized_arrow_array_iterator_hasNext)) {
      CheckException(env);
      return nullptr; // stream ended
    }
    CheckException(env);
    ArrowArray c_array{};
    env->CallObjectMethod(
        java_serialized_arrow_array_iterator_,
        serialized_arrow_array_iterator_next,
        reinterpret_cast<jlong>(&c_array));
    CheckException(env);
    auto array = std::make_shared<ArrowArray>(c_array);
    return array;
  }

 private:
  JavaVM* vm_;
  jobject java_serialized_arrow_array_iterator_;
};

// TODO: make sure deleted
class JavaArrowArrayIteratorWrapper {
 public:
  explicit JavaArrowArrayIteratorWrapper(
      std::shared_ptr<JavaArrowArrayIterator> delegated)
      : delegated_(std::move(delegated)) {}

  arrow::Result<std::shared_ptr<ArrowArray>> Next() {
    return delegated_->Next();
  }

 private:
  std::shared_ptr<JavaArrowArrayIterator> delegated_;
};

// See Java class
// org/apache/arrow/dataset/jni/NativeSerializedRecordBatchIterator
//
std::shared_ptr<JavaArrowArrayIterator> MakeJavaArrowArrayIterator(
    JavaVM* vm,
    jobject java_serialized_arrow_array_iterator) {
#ifdef GLUTEN_PRINT_DEBUG
  std::cout << "CREATING ITERATOR REF "
            << reinterpret_cast<long>(java_serialized_arrow_array_iterator)
            << "..." << std::endl;
#endif
  std::shared_ptr<JavaArrowArrayIterator> itr =
      std::make_shared<JavaArrowArrayIterator>(
          vm, java_serialized_arrow_array_iterator);
  return itr;
}

jmethodID GetMethodIDOrError(
    JNIEnv* env,
    jclass this_class,
    const char* name,
    const char* sig) {
  jmethodID ret = GetMethodID(env, this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
        " within signature" + std::string(sig);
    throw gluten::GlutenException(error_message);
  }
  return ret;
}

jclass CreateGlobalClassReferenceOrError(JNIEnv* env, const char* class_name) {
  jclass global_class = CreateGlobalClassReference(env, class_name);
  if (global_class == nullptr) {
    std::string error_message =
        "Unable to CreateGlobalClassReferenceOrError for" +
        std::string(class_name);
    throw gluten::GlutenException(error_message);
  }
  return global_class;
}

using FileSystem = arrow::fs::FileSystem;

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  gluten::GetJniErrorsState()->Initialize(env);

  serializable_obj_builder_class = CreateGlobalClassReferenceOrError(
      env, "Lio/glutenproject/vectorized/NativeSerializableObject;");
  serializable_obj_builder_constructor = GetMethodIDOrError(
      env, serializable_obj_builder_class, "<init>", "([J[I)V");

  byte_array_class = CreateGlobalClassReferenceOrError(env, "[B");
  split_result_class = CreateGlobalClassReferenceOrError(
      env, "Lio/glutenproject/vectorized/SplitResult;");
  split_result_constructor =
      GetMethodIDOrError(env, split_result_class, "<init>", "(JJJJJJ[J[J)V");

  metrics_builder_class = CreateGlobalClassReferenceOrError(
      env, "Lio/glutenproject/vectorized/Metrics;");
  metrics_builder_constructor = GetMethodIDOrError(
      env, metrics_builder_class, "<init>", "([J[J[J[J[J[J[J[J[J[J[J[J[J[J)V");

  serialized_arrow_array_iterator_class = CreateGlobalClassReferenceOrError(
      env, "Lio/glutenproject/vectorized/ArrowInIterator;");
  serialized_arrow_array_iterator_hasNext = GetMethodIDOrError(
      env, serialized_arrow_array_iterator_class, "hasNext", "()Z");
  serialized_arrow_array_iterator_next = GetMethodIDOrError(
      env, serialized_arrow_array_iterator_class, "next", "(J)V");

  native_columnar_to_row_info_class = CreateGlobalClassReferenceOrError(
      env, "Lio/glutenproject/vectorized/NativeColumnarToRowInfo;");
  native_columnar_to_row_info_constructor = GetMethodIDOrError(
      env, native_columnar_to_row_info_class, "<init>", "(J[J[JJ)V");

  java_reservation_listener_class = CreateGlobalClassReference(
      env,
      "Lio/glutenproject/memory/"
      "ReservationListener;");
  reserve_memory_method = GetMethodIDOrError(
      env, java_reservation_listener_class, "reserve", "(J)V");
  unreserve_memory_method = GetMethodIDOrError(
      env, java_reservation_listener_class, "unreserve", "(J)V");

  default_memory_allocator_id =
      reinterpret_cast<jlong>(gluten::memory::DefaultMemoryAllocator().get());

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  env->DeleteGlobalRef(serializable_obj_builder_class);
  env->DeleteGlobalRef(split_result_class);
  env->DeleteGlobalRef(serialized_arrow_array_iterator_class);
  env->DeleteGlobalRef(native_columnar_to_row_info_class);

  env->DeleteGlobalRef(byte_array_class);

  array_iterator_holder_.Clear();
  columnar_to_row_converter_holder_.Clear();
  shuffle_splitter_holder_.Clear();
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetJavaTmpDir(
    JNIEnv* env,
    jobject obj,
    jstring pathObj) {
  JNI_METHOD_START
  jboolean ifCopy;
  auto path = env->GetStringUTFChars(pathObj, &ifCopy);
  setenv("NATIVESQL_TMP_DIR", path, 1);
  env->ReleaseStringUTFChars(pathObj, path);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetBatchSize(
    JNIEnv* env,
    jobject obj,
    jint batch_size) {
  setenv("NATIVESQL_BATCH_SIZE", std::to_string(batch_size).c_str(), 1);
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMetricsTime(
    JNIEnv* env,
    jobject obj,
    jboolean is_enable) {
  setenv("NATIVESQL_METRICS_TIME", (is_enable ? "true" : "false"), 1);
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithIterator(
    JNIEnv* env,
    jobject obj,
    jlong allocator_id,
    jbyteArray plan_arr,
    jobjectArray iter_arr) {
  JNI_METHOD_START
  arrow::Status msg;
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    std::string error_message = "Unable to get JavaVM instance";
    gluten::JniThrow(error_message);
  }

  auto plan_data =
      reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(plan_arr, 0));
  auto plan_size = env->GetArrayLength(plan_arr);

  auto* allocator =
      reinterpret_cast<gluten::memory::MemoryAllocator*>(allocator_id);
  if (allocator == nullptr) {
    gluten::JniThrow("Memory pool does not exist or has been closed");
  }

  auto backend = gluten::CreateBackend();
  if (!backend->ParsePlan(plan_data, plan_size)) {
    gluten::JniThrow("Failed to parse plan.");
  }

  // Handle the Java iters
  jsize iters_len = env->GetArrayLength(iter_arr);
  std::vector<std::shared_ptr<ArrowArrayResultIterator>> input_iters;
  if (iters_len > 0) {
    for (int idx = 0; idx < iters_len; idx++) {
      jobject iter = env->GetObjectArrayElement(iter_arr, idx);
      // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
      // TODO Release this in JNI Unload or dependent object's destructor
      jobject ref_iter = env->NewGlobalRef(iter);
      auto array_iter = MakeJavaArrowArrayIterator(vm, ref_iter);
      input_iters.push_back(
          std::make_shared<ArrowArrayResultIterator>(std::move(array_iter)));
    }
  }

  std::shared_ptr<ArrowArrayResultIterator> res_iter;
  if (input_iters.empty()) {
    res_iter = backend->GetResultIterator(allocator);
  } else {
    res_iter = backend->GetResultIterator(allocator, input_iters);
  }
  return array_iterator_holder_.Insert(std::move(res_iter));
  JNI_METHOD_END(-1)
}

JNIEXPORT jboolean JNICALL
Java_io_glutenproject_vectorized_ArrowOutIterator_nativeHasNext(
    JNIEnv* env,
    jobject obj,
    jlong id) {
  JNI_METHOD_START
  auto iter = GetArrayIterator(env, id);
  if (iter == nullptr) {
    std::string error_message = "faked to get batch iterator";
    gluten::JniThrow(error_message);
  }
  return iter->HasNext();
  JNI_METHOD_END(false)
}

JNIEXPORT jboolean JNICALL
Java_io_glutenproject_vectorized_ArrowOutIterator_nativeNext(
    JNIEnv* env,
    jobject obj,
    jlong id,
    jlong c_array) {
  JNI_METHOD_START
  auto iter = GetArrayIterator(env, id);
  if (!iter->HasNext()) {
    return false;
  }
  ArrowArrayMove(
      std::move(iter->Next().get()),
      reinterpret_cast<struct ArrowArray*>(c_array));
  return true;
  JNI_METHOD_END(false)
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_ArrowOutIterator_nativeFetchMetrics(
    JNIEnv* env,
    jobject obj,
    jlong id) {
  JNI_METHOD_START
  auto iter = GetArrayIterator(env, id);
  std::shared_ptr<Metrics> metrics = iter->GetMetrics();

  int numMetrics = 0;
  if (metrics) {
    numMetrics = metrics->numMetrics;
  }
  auto inputRows = env->NewLongArray(numMetrics);
  auto inputVectors = env->NewLongArray(numMetrics);
  auto inputBytes = env->NewLongArray(numMetrics);
  auto rawInputRows = env->NewLongArray(numMetrics);
  auto outputRows = env->NewLongArray(numMetrics);
  auto outputVectors = env->NewLongArray(numMetrics);
  auto outputBytes = env->NewLongArray(numMetrics);
  auto count = env->NewLongArray(numMetrics);
  auto wallNanos = env->NewLongArray(numMetrics);
  auto cpuNanos = env->NewLongArray(numMetrics);
  auto blockedWallNanos = env->NewLongArray(numMetrics);
  auto peakMemoryBytes = env->NewLongArray(numMetrics);
  auto numMemoryAllocations = env->NewLongArray(numMetrics);

  if (metrics) {
    env->SetLongArrayRegion(inputRows, 0, numMetrics, metrics->inputRows);
    env->SetLongArrayRegion(inputVectors, 0, numMetrics, metrics->inputVectors);
    env->SetLongArrayRegion(inputBytes, 0, numMetrics, metrics->inputBytes);
    env->SetLongArrayRegion(rawInputRows, 0, numMetrics, metrics->rawInputRows);
    env->SetLongArrayRegion(outputRows, 0, numMetrics, metrics->outputRows);
    env->SetLongArrayRegion(
        outputVectors, 0, numMetrics, metrics->outputVectors);
    env->SetLongArrayRegion(outputBytes, 0, numMetrics, metrics->outputBytes);
    env->SetLongArrayRegion(count, 0, numMetrics, metrics->count);
    env->SetLongArrayRegion(wallNanos, 0, numMetrics, metrics->wallNanos);
    env->SetLongArrayRegion(cpuNanos, 0, numMetrics, metrics->cpuNanos);
    env->SetLongArrayRegion(
        blockedWallNanos, 0, numMetrics, metrics->blockedWallNanos);
    env->SetLongArrayRegion(
        peakMemoryBytes, 0, numMetrics, metrics->peakMemoryBytes);
    env->SetLongArrayRegion(
        numMemoryAllocations, 0, numMetrics, metrics->numMemoryAllocations);
  }

  return env->NewObject(
      metrics_builder_class,
      metrics_builder_constructor,
      inputRows,
      inputVectors,
      inputBytes,
      rawInputRows,
      outputRows,
      outputVectors,
      outputBytes,
      count,
      wallNanos,
      cpuNanos,
      blockedWallNanos,
      peakMemoryBytes,
      numMemoryAllocations);
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ArrowOutIterator_nativeClose(
    JNIEnv* env,
    jobject this_obj,
    jlong id) {
  JNI_METHOD_START
#ifdef GLUTEN_PRINT_DEBUG
  auto it = array_iterator_holder_.Lookup(id);
  if (it.use_count() > 2) {
    std::cout << "ArrowArrayResultIterator Id " << id << " use count is "
              << it.use_count() << std::endl;
  }
  std::cout << "BatchIterator nativeClose." << std::endl;
#endif
  array_iterator_holder_.Erase(id);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeConvertColumnarToRow(
    JNIEnv* env,
    jobject,
    jlong c_schema,
    jlong c_array,
    jlong allocator_id,
    jboolean wsChild) {
  JNI_METHOD_START
  std::shared_ptr<arrow::RecordBatch> rb =
      gluten::JniGetOrThrow(arrow::ImportRecordBatch(
          reinterpret_cast<struct ArrowArray*>(c_array),
          reinterpret_cast<struct ArrowSchema*>(c_schema)));
  int64_t num_rows = rb->num_rows();
  // convert the record batch to spark unsafe row.
  auto* allocator =
      reinterpret_cast<gluten::memory::MemoryAllocator*>(allocator_id);
  if (allocator == nullptr) {
    gluten::JniThrow("Memory pool does not exist or has been closed");
  }
  auto backend = gluten::CreateBackend();
  std::shared_ptr<gluten::columnartorow::ColumnarToRowConverterBase>
      columnar_to_row_converter =
          backend->getColumnarConverter(allocator, rb, wsChild);
  gluten::JniAssertOkOrThrow(
      columnar_to_row_converter->Init(),
      "Native convert columnar to row: Init "
      "ColumnarToRowConverter failed");
  gluten::JniAssertOkOrThrow(
      columnar_to_row_converter->Write(),
      "Native convert columnar to row: ColumnarToRowConverter write failed");

  const auto& offsets = columnar_to_row_converter->GetOffsets();
  const auto& lengths = columnar_to_row_converter->GetLengths();
  int64_t instanceID =
      columnar_to_row_converter_holder_.Insert(columnar_to_row_converter);

  auto offsets_arr = env->NewLongArray(num_rows);
  auto offsets_src = reinterpret_cast<const jlong*>(offsets.data());
  env->SetLongArrayRegion(offsets_arr, 0, num_rows, offsets_src);
  auto lengths_arr = env->NewLongArray(num_rows);
  auto lengths_src = reinterpret_cast<const jlong*>(lengths.data());
  env->SetLongArrayRegion(lengths_arr, 0, num_rows, lengths_src);
  long address =
      reinterpret_cast<long>(columnar_to_row_converter->GetBufferAddress());

  jobject native_columnar_to_row_info = env->NewObject(
      native_columnar_to_row_info_class,
      native_columnar_to_row_info_constructor,
      instanceID,
      offsets_arr,
      lengths_arr,
      address);
  return native_columnar_to_row_info;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeClose(
    JNIEnv* env,
    jobject,
    jlong instance_id) {
  JNI_METHOD_START
  columnar_to_row_converter_holder_.Erase(instance_id);
  JNI_METHOD_END()
}

// Shuffle
JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ShuffleSplitterJniWrapper_nativeMake(
    JNIEnv* env,
    jobject,
    jstring partitioning_name_jstr,
    jint num_partitions,
    jlong c_schema,
    jbyteArray expr_arr,
    jlong offheap_per_task,
    jint buffer_size,
    jstring compression_type_jstr,
    jint batch_compress_threshold,
    jstring data_file_jstr,
    jint num_sub_dirs,
    jstring local_dirs_jstr,
    jboolean prefer_spill,
    jlong allocator_id,
    jboolean write_schema) {
  JNI_METHOD_START
  if (partitioning_name_jstr == NULL) {
    gluten::JniThrow(std::string("Short partitioning name can't be null"));
    return 0;
  }
  if (data_file_jstr == NULL) {
    gluten::JniThrow(std::string("Shuffle DataFile can't be null"));
  }
  if (local_dirs_jstr == NULL) {
    gluten::JniThrow(std::string("Shuffle DataFile can't be null"));
  }

  auto partitioning_name_c =
      env->GetStringUTFChars(partitioning_name_jstr, JNI_FALSE);
  auto partitioning_name = std::string(partitioning_name_c);
  env->ReleaseStringUTFChars(partitioning_name_jstr, partitioning_name_c);

  auto splitOptions = SplitOptions::Defaults();
  splitOptions.write_schema = write_schema;
  splitOptions.prefer_spill = prefer_spill;
  splitOptions.buffered_write = true;
  if (buffer_size > 0) {
    splitOptions.buffer_size = buffer_size;
  }
  splitOptions.offheap_per_task = offheap_per_task;

  if (num_sub_dirs > 0) {
    splitOptions.num_sub_dirs = num_sub_dirs;
  }

  if (compression_type_jstr != NULL) {
    auto compression_type_result =
        GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      splitOptions.compression_type = compression_type_result.MoveValueUnsafe();
    }
  }

  auto data_file_c = env->GetStringUTFChars(data_file_jstr, JNI_FALSE);
  splitOptions.data_file = std::string(data_file_c);
  env->ReleaseStringUTFChars(data_file_jstr, data_file_c);

  auto* allocator =
      reinterpret_cast<gluten::memory::MemoryAllocator*>(allocator_id);
  if (allocator == nullptr) {
    gluten::JniThrow("Memory pool does not exist or has been closed");
  }
  splitOptions.memory_pool =
      gluten::memory::AsWrappedArrowMemoryPool(allocator);

  auto local_dirs = env->GetStringUTFChars(local_dirs_jstr, JNI_FALSE);
  setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs, 1);
  env->ReleaseStringUTFChars(local_dirs_jstr, local_dirs);

  std::shared_ptr<arrow::Schema> schema = gluten::JniGetOrThrow(
      arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(c_schema)));

  jclass cls = env->FindClass("java/lang/Thread");
  jmethodID mid =
      env->GetStaticMethodID(cls, "currentThread", "()Ljava/lang/Thread;");
  jobject thread = env->CallStaticObjectMethod(cls, mid);
  if (thread == NULL) {
    std::cout << "Thread.currentThread() return NULL" << std::endl;
  } else {
    jmethodID mid_getid = GetMethodIDOrError(env, cls, "getId", "()J");
    jlong sid = env->CallLongMethod(thread, mid_getid);
    splitOptions.thread_id = (int64_t)sid;
  }

  jclass tc_cls = env->FindClass("org/apache/spark/TaskContext");
  jmethodID get_tc_mid =
      env->GetStaticMethodID(tc_cls, "get", "()Lorg/apache/spark/TaskContext;");
  jobject tc_obj = env->CallStaticObjectMethod(tc_cls, get_tc_mid);
  if (tc_obj == NULL) {
    std::cout << "TaskContext.get() return NULL" << std::endl;
  } else {
    jmethodID get_tsk_attmpt_mid =
        GetMethodIDOrError(env, tc_cls, "taskAttemptId", "()J");
    jlong attmpt_id = env->CallLongMethod(tc_obj, get_tsk_attmpt_mid);
    splitOptions.task_attempt_id = (int64_t)attmpt_id;
  }
  splitOptions.batch_compress_threshold = batch_compress_threshold;

  // Get the hash expressions.
  const uint8_t* expr_data = nullptr;
  int expr_size = 0;
  if (expr_arr != NULL) {
    expr_data = reinterpret_cast<const uint8_t*>(
        env->GetByteArrayElements(expr_arr, 0));
    expr_size = env->GetArrayLength(expr_arr);
  }

  auto splitter = gluten::JniGetOrThrow(
      Splitter::Make(
          partitioning_name,
          std::move(schema),
          num_partitions,
          expr_data,
          expr_size,
          std::move(splitOptions)),
      "Failed create native shuffle splitter");

  return shuffle_splitter_holder_.Insert(std::shared_ptr<Splitter>(splitter));

  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ShuffleSplitterJniWrapper_setCompressType(
    JNIEnv* env,
    jobject,
    jlong splitter_id,
    jstring compression_type_jstr) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message =
        "Invalid splitter id " + std::to_string(splitter_id);
    gluten::JniThrow(error_message);
  }

  if (compression_type_jstr != NULL) {
    auto compression_type_result =
        GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      gluten::JniAssertOkOrThrow(
          splitter->SetCompressType(compression_type_result.MoveValueUnsafe()));
    }
  }
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ShuffleSplitterJniWrapper_split(
    JNIEnv* env,
    jobject,
    jlong splitter_id,
    jint num_rows,
    jlong c_array,
    jboolean first_record_batch) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message =
        "Invalid splitter id " + std::to_string(splitter_id);
    gluten::JniThrow(error_message);
  }
  std::shared_ptr<arrow::RecordBatch> in =
      gluten::JniGetOrThrow(arrow::ImportRecordBatch(
          reinterpret_cast<struct ArrowArray*>(c_array),
          splitter->input_schema()));

  if (first_record_batch) {
    return splitter->CompressedSize(*in);
  }
  gluten::JniAssertOkOrThrow(
      splitter->Split(*in), "Native split: splitter split failed");
  return -1L;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_ShuffleSplitterJniWrapper_stop(
    JNIEnv* env,
    jobject,
    jlong splitter_id) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message =
        "Invalid splitter id " + std::to_string(splitter_id);
    gluten::JniThrow(error_message);
  }

  gluten::JniAssertOkOrThrow(
      splitter->Stop(), "Native split: splitter stop failed");

  const auto& partition_lengths = splitter->PartitionLengths();
  auto partition_length_arr = env->NewLongArray(partition_lengths.size());
  auto src = reinterpret_cast<const jlong*>(partition_lengths.data());
  env->SetLongArrayRegion(
      partition_length_arr, 0, partition_lengths.size(), src);

  const auto& raw_partition_lengths = splitter->RawPartitionLengths();
  auto raw_partition_length_arr =
      env->NewLongArray(raw_partition_lengths.size());
  auto raw_src = reinterpret_cast<const jlong*>(raw_partition_lengths.data());
  env->SetLongArrayRegion(
      raw_partition_length_arr, 0, raw_partition_lengths.size(), raw_src);

  jobject split_result = env->NewObject(
      split_result_class,
      split_result_constructor,
      splitter->TotalComputePidTime(),
      splitter->TotalWriteTime(),
      splitter->TotalSpillTime(),
      splitter->TotalCompressTime(),
      splitter->TotalBytesWritten(),
      splitter->TotalBytesSpilled(),
      partition_length_arr,
      raw_partition_length_arr);

  return split_result;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ShuffleSplitterJniWrapper_close(
    JNIEnv* env,
    jobject,
    jlong splitter_id) {
  JNI_METHOD_START
  shuffle_splitter_holder_.Erase(splitter_id);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_memory_NativeMemoryAllocator_getDefaultAllocator(
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
  return default_memory_allocator_id;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_memory_NativeMemoryAllocator_createListenableAllocator(
    JNIEnv* env,
    jclass,
    jobject jlistener) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    gluten::JniThrow("Unable to get JavaVM instance");
  }
  std::shared_ptr<gluten::memory::AllocationListener> listener =
      std::make_shared<SparkAllocationListener>(
          vm,
          jlistener,
          reserve_memory_method,
          unreserve_memory_method,
          8L << 10 << 10);
  auto allocator = new gluten::memory::ListenableMemoryAllocator(
      gluten::memory::DefaultMemoryAllocator().get(), listener);
  return reinterpret_cast<jlong>(allocator);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_memory_NativeMemoryAllocator_releaseAllocator(
    JNIEnv* env,
    jclass,
    jlong allocator_id) {
  JNI_METHOD_START
  if (allocator_id == default_memory_allocator_id) {
    return;
  }
  auto* alloc =
      reinterpret_cast<gluten::memory::MemoryAllocator*>(allocator_id);
  if (alloc == nullptr) {
    return;
  }
  delete alloc;
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_memory_NativeMemoryAllocator_bytesAllocated(
    JNIEnv* env,
    jclass,
    jlong allocator_id) {
  JNI_METHOD_START
  auto* alloc =
      reinterpret_cast<gluten::memory::MemoryAllocator*>(allocator_id);
  if (alloc == nullptr) {
    gluten::JniThrow(
        "Memory allocator instance not found. It may not exist nor has been closed");
  }
  return alloc->GetBytes();
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_tpc_MallocUtils_mallocTrim(JNIEnv* env, jobject obj) {
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_trim... " << std::endl;
  malloc_trim(0);
}

JNIEXPORT void JNICALL
Java_io_glutenproject_tpc_MallocUtils_mallocStats(JNIEnv* env, jobject obj) {
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_stats... " << std::endl;
  malloc_stats();
}

#ifdef __cplusplus
}
#endif
