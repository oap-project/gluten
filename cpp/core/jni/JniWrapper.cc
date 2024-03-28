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

#include <jni.h>
#include <filesystem>

#include "compute/Runtime.h"
#include "config/GlutenConfig.h"
#include "jni/JniCommon.h"
#include "jni/JniError.h"

#include "operators/writer/Datasource.h"

#include <arrow/c/bridge.h>
#include <optional>
#include "memory/AllocationListener.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/Partitioning.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/Utils.h"
#include "shuffle/rss/CelebornPartitionWriter.h"
#include "utils/ArrowStatus.h"
#include "utils/StringUtil.h"

using namespace gluten;

static jclass javaReservationListenerClass;

static jmethodID reserveMemoryMethod;
static jmethodID unreserveMemoryMethod;

static jclass byteArrayClass;

static jclass jniByteInputStreamClass;
static jmethodID jniByteInputStreamRead;
static jmethodID jniByteInputStreamTell;
static jmethodID jniByteInputStreamClose;

static jclass splitResultClass;
static jmethodID splitResultConstructor;

static jclass columnarBatchSerializeResultClass;
static jmethodID columnarBatchSerializeResultConstructor;

static jclass serializedColumnarBatchIteratorClass;
static jclass metricsBuilderClass;
static jmethodID metricsBuilderConstructor;

static jmethodID serializedColumnarBatchIteratorHasNext;
static jmethodID serializedColumnarBatchIteratorNext;

static jclass nativeColumnarToRowInfoClass;
static jmethodID nativeColumnarToRowInfoConstructor;

static jclass shuffleReaderMetricsClass;
static jmethodID shuffleReaderMetricsSetDecompressTime;
static jmethodID shuffleReaderMetricsSetIpcTime;
static jmethodID shuffleReaderMetricsSetDeserializeTime;

static jclass block_stripes_class;
static jmethodID block_stripes_constructor;

class JavaInputStreamAdaptor final : public arrow::io::InputStream {
 public:
  JavaInputStreamAdaptor(JNIEnv* env, arrow::MemoryPool* pool, jobject jniIn) : pool_(pool) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string errorMessage = "Unable to get JavaVM instance";
      throw gluten::GlutenException(errorMessage);
    }
    jniIn_ = env->NewGlobalRef(jniIn);
  }

  ~JavaInputStreamAdaptor() override {
    try {
      auto status = JavaInputStreamAdaptor::Close();
      if (!status.ok()) {
        LOG(WARNING) << __func__ << " call JavaInputStreamAdaptor::Close() failed, status:" << status.ToString();
      }
    } catch (std::exception& e) {
      LOG(WARNING) << __func__ << " call JavaInputStreamAdaptor::Close() got exception:" << e.what();
    }
  }

  // not thread safe
  arrow::Status Close() override {
    if (closed_) {
      return arrow::Status::OK();
    }
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->CallVoidMethod(jniIn_, jniByteInputStreamClose);
    checkException(env);
    env->DeleteGlobalRef(jniIn_);
    vm_->DetachCurrentThread();
    closed_ = true;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jlong told = env->CallLongMethod(jniIn_, jniByteInputStreamTell);
    checkException(env);
    return told;
  }

  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jlong read = env->CallLongMethod(jniIn_, jniByteInputStreamRead, reinterpret_cast<jlong>(out), nbytes);
    checkException(env);
    return read;
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    GLUTEN_ASSIGN_OR_THROW(auto buffer, arrow::AllocateResizableBuffer(nbytes, pool_))
    GLUTEN_ASSIGN_OR_THROW(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))
    GLUTEN_THROW_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    return std::move(buffer);
  }

 private:
  arrow::MemoryPool* pool_;
  JavaVM* vm_;
  jobject jniIn_;
  bool closed_ = false;
};

class JniColumnarBatchIterator : public ColumnarBatchIterator {
 public:
  explicit JniColumnarBatchIterator(
      JNIEnv* env,
      jobject jColumnarBatchItr,
      Runtime* runtime,
      std::shared_ptr<ArrowWriter> writer)
      : runtime_(runtime), writer_(writer) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string errorMessage = "Unable to get JavaVM instance";
      throw gluten::GlutenException(errorMessage);
    }
    jColumnarBatchItr_ = env->NewGlobalRef(jColumnarBatchItr);
  }

  // singleton
  JniColumnarBatchIterator(const JniColumnarBatchIterator&) = delete;
  JniColumnarBatchIterator(JniColumnarBatchIterator&&) = delete;
  JniColumnarBatchIterator& operator=(const JniColumnarBatchIterator&) = delete;
  JniColumnarBatchIterator& operator=(JniColumnarBatchIterator&&) = delete;

  virtual ~JniColumnarBatchIterator() {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->DeleteGlobalRef(jColumnarBatchItr_);
    vm_->DetachCurrentThread();
  }

  std::shared_ptr<ColumnarBatch> next() override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    if (!env->CallBooleanMethod(jColumnarBatchItr_, serializedColumnarBatchIteratorHasNext)) {
      checkException(env);
      return nullptr; // stream ended
    }

    checkException(env);
    jlong handle = env->CallLongMethod(jColumnarBatchItr_, serializedColumnarBatchIteratorNext);
    checkException(env);
    auto batch = runtime_->objectStore()->retrieve<ColumnarBatch>(handle);
    if (writer_ != nullptr) {
      // save snapshot of the batch to file
      std::shared_ptr<ArrowSchema> schema = batch->exportArrowSchema();
      std::shared_ptr<ArrowArray> array = batch->exportArrowArray();
      auto rb = gluten::arrowGetOrThrow(arrow::ImportRecordBatch(array.get(), schema.get()));
      GLUTEN_THROW_NOT_OK(writer_->initWriter(*(rb->schema().get())));
      GLUTEN_THROW_NOT_OK(writer_->writeInBatches(rb));
    }
    return batch;
  }

 private:
  JavaVM* vm_;
  jobject jColumnarBatchItr_;
  Runtime* runtime_;
  std::shared_ptr<ArrowWriter> writer_;
};

std::unique_ptr<JniColumnarBatchIterator> makeJniColumnarBatchIterator(
    JNIEnv* env,
    jobject jColumnarBatchItr,
    Runtime* runtime,
    std::shared_ptr<ArrowWriter> writer) {
  return std::make_unique<JniColumnarBatchIterator>(env, jColumnarBatchItr, runtime, writer);
}

template <typename T>
T* jniCastOrThrow(ResourceHandle handle) {
  auto instance = reinterpret_cast<T*>(handle);
  GLUTEN_CHECK(instance != nullptr, "FATAL: resource instance should not be null.");
  return instance;
}

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    return JNI_ERR;
  }
  gluten::getJniCommonState()->ensureInitialized(env);
  gluten::getJniErrorState()->ensureInitialized(env);

  byteArrayClass = createGlobalClassReferenceOrError(env, "[B");

  jniByteInputStreamClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/JniByteInputStream;");
  jniByteInputStreamRead = getMethodIdOrError(env, jniByteInputStreamClass, "read", "(JJ)J");
  jniByteInputStreamTell = getMethodIdOrError(env, jniByteInputStreamClass, "tell", "()J");
  jniByteInputStreamClose = getMethodIdOrError(env, jniByteInputStreamClass, "close", "()V");

  splitResultClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/GlutenSplitResult;");
  splitResultConstructor = getMethodIdOrError(env, splitResultClass, "<init>", "(JJJJJJJ[J[J)V");

  columnarBatchSerializeResultClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/ColumnarBatchSerializeResult;");
  columnarBatchSerializeResultConstructor =
      getMethodIdOrError(env, columnarBatchSerializeResultClass, "<init>", "(J[B)V");

  metricsBuilderClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/metrics/Metrics;");

  metricsBuilderConstructor = getMethodIdOrError(
      env, metricsBuilderClass, "<init>", "([J[J[J[J[J[J[J[J[J[JJ[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J)V");

  serializedColumnarBatchIteratorClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/ColumnarBatchInIterator;");

  serializedColumnarBatchIteratorHasNext =
      getMethodIdOrError(env, serializedColumnarBatchIteratorClass, "hasNext", "()Z");

  serializedColumnarBatchIteratorNext = getMethodIdOrError(env, serializedColumnarBatchIteratorClass, "next", "()J");

  nativeColumnarToRowInfoClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/NativeColumnarToRowInfo;");
  nativeColumnarToRowInfoConstructor = getMethodIdOrError(env, nativeColumnarToRowInfoClass, "<init>", "([I[IJ)V");

  javaReservationListenerClass = createGlobalClassReference(
      env,
      "Lorg/apache/gluten/memory/nmm/"
      "ReservationListener;");

  reserveMemoryMethod = getMethodIdOrError(env, javaReservationListenerClass, "reserve", "(J)J");
  unreserveMemoryMethod = getMethodIdOrError(env, javaReservationListenerClass, "unreserve", "(J)J");

  shuffleReaderMetricsClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/ShuffleReaderMetrics;");
  shuffleReaderMetricsSetDecompressTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDecompressTime", "(J)V");
  shuffleReaderMetricsSetIpcTime = getMethodIdOrError(env, shuffleReaderMetricsClass, "setIpcTime", "(J)V");
  shuffleReaderMetricsSetDeserializeTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDeserializeTime", "(J)V");

  block_stripes_class =
      createGlobalClassReferenceOrError(env, "Lorg/apache/spark/sql/execution/datasources/BlockStripes;");
  block_stripes_constructor = env->GetMethodID(block_stripes_class, "<init>", "(J[J[II[B)V");

  return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);
  env->DeleteGlobalRef(jniByteInputStreamClass);
  env->DeleteGlobalRef(splitResultClass);
  env->DeleteGlobalRef(columnarBatchSerializeResultClass);
  env->DeleteGlobalRef(serializedColumnarBatchIteratorClass);
  env->DeleteGlobalRef(nativeColumnarToRowInfoClass);
  env->DeleteGlobalRef(byteArrayClass);
  env->DeleteGlobalRef(shuffleReaderMetricsClass);
  env->DeleteGlobalRef(block_stripes_class);

  gluten::getJniErrorState()->close();
  gluten::getJniCommonState()->close();
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_exec_RuntimeJniWrapper_createRuntime( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jbackendType,
    jbyteArray sessionConf) {
  JNI_METHOD_START
  auto backendType = jStringToCString(env, jbackendType);
  auto safeArray = gluten::getByteArrayElementsSafe(env, sessionConf);
  auto sparkConf = gluten::parseConfMap(env, safeArray.elems(), safeArray.length());
  auto runtime = gluten::Runtime::create(backendType, sparkConf);
  return reinterpret_cast<jlong>(runtime);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_exec_RuntimeJniWrapper_releaseRuntime( // NOLINT
    JNIEnv* env,
    jclass,
    jlong ctxHandle) {
  JNI_METHOD_START
  auto runtime = jniCastOrThrow<Runtime>(ctxHandle);

  gluten::Runtime::release(runtime);
  JNI_METHOD_END()
}

JNIEXPORT jstring JNICALL Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativePlanString( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray planArray,
    jboolean details) {
  JNI_METHOD_START

  auto safeArray = gluten::getByteArrayElementsSafe(env, planArray);
  auto planData = safeArray.elems();
  auto planSize = env->GetArrayLength(planArray);
  auto ctx = gluten::getRuntime(env, wrapper);
  ctx->parsePlan(planData, planSize, {}, std::nullopt);
  auto& conf = ctx->getConfMap();
  auto planString = ctx->planString(details, conf);
  return env->NewStringUTF(planString.c_str());

  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_injectWriteFilesTempPath( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray path) {
  JNI_METHOD_START

  auto len = env->GetArrayLength(path);
  auto safeArray = gluten::getByteArrayElementsSafe(env, path);
  std::string pathStr(reinterpret_cast<char*>(safeArray.elems()), len);
  auto ctx = gluten::getRuntime(env, wrapper);
  ctx->injectWriteFilesTempPath(pathStr);

  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativeCreateKernelWithIterator( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong memoryManagerHandle,
    jbyteArray planArr,
    jobjectArray splitInfosArr,
    jobjectArray iterArr,
    jint stageId,
    jint partitionId,
    jlong taskId,
    jboolean saveInput,
    jstring spillDir) {
  JNI_METHOD_START

  auto ctx = gluten::getRuntime(env, wrapper);
  auto& conf = ctx->getConfMap();

  std::string saveDir{};
  std::string fileIdentifier = "_" + std::to_string(stageId) + "_" + std::to_string(partitionId);
  if (saveInput) {
    if (conf.find(kGlutenSaveDir) == conf.end()) {
      throw gluten::GlutenException(kGlutenSaveDir + " is not configured.");
    }
    saveDir = conf.at(kGlutenSaveDir);
    std::filesystem::path f{saveDir};
    if (!std::filesystem::exists(f)) {
      throw gluten::GlutenException("Save input path " + saveDir + " does not exists");
    }
    ctx->dumpConf(saveDir + "/conf" + fileIdentifier + ".ini");
  }

  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  auto spillDirStr = jStringToCString(env, spillDir);

  for (jsize i = 0, splitInfoArraySize = env->GetArrayLength(splitInfosArr); i < splitInfoArraySize; i++) {
    jbyteArray splitInfoArray = static_cast<jbyteArray>(env->GetObjectArrayElement(splitInfosArr, i));
    jsize splitInfoSize = env->GetArrayLength(splitInfoArray);
    auto safeArray = gluten::getByteArrayElementsSafe(env, splitInfoArray);
    auto splitInfoData = safeArray.elems();
    ctx->parseSplitInfo(
        splitInfoData,
        splitInfoSize,
        saveInput ? std::optional<std::string>(saveDir + "/split" + fileIdentifier + "_" + std::to_string(i) + ".json")
                  : std::nullopt);
  }

  auto safeArray = gluten::getByteArrayElementsSafe(env, planArr);
  auto planSize = env->GetArrayLength(planArr);
  ctx->parsePlan(
      safeArray.elems(),
      planSize,
      {stageId, partitionId, taskId},
      saveInput ? std::optional<std::string>(saveDir + "/plan" + fileIdentifier + ".json") : std::nullopt);

  // Handle the Java iters
  jsize itersLen = env->GetArrayLength(iterArr);
  std::vector<std::shared_ptr<ResultIterator>> inputIters;
  for (int idx = 0; idx < itersLen; idx++) {
    std::shared_ptr<ArrowWriter> writer = nullptr;
    if (saveInput) {
      auto file = saveDir + "/data" + fileIdentifier + "_" + std::to_string(idx) + ".parquet";
      writer = std::make_shared<ArrowWriter>(file);
    }
    jobject iter = env->GetObjectArrayElement(iterArr, idx);
    auto arrayIter = makeJniColumnarBatchIterator(env, iter, ctx, writer);
    auto resultIter = std::make_shared<ResultIterator>(std::move(arrayIter));
    inputIters.push_back(std::move(resultIter));
  }

  return ctx->objectStore()->save(ctx->createResultIterator(memoryManager, spillDirStr, inputIters, conf));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeHasNext( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto iter = ctx->objectStore()->retrieve<ResultIterator>(iterHandle);
  if (iter == nullptr) {
    std::string errorMessage = "failed to get batch iterator";
    throw gluten::GlutenException(errorMessage);
  }
  return iter->hasNext();
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeNext( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto iter = ctx->objectStore()->retrieve<ResultIterator>(iterHandle);
  if (!iter->hasNext()) {
    return kInvalidResourceHandle;
  }

  std::shared_ptr<ColumnarBatch> batch = iter->next();
  auto batchHandle = ctx->objectStore()->save(batch);

  iter->setExportNanos(batch->getExportNanos());
  return batchHandle;
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeFetchMetrics( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto iter = ctx->objectStore()->retrieve<ResultIterator>(iterHandle);
  auto metrics = iter->getMetrics();
  unsigned int numMetrics = 0;
  if (metrics) {
    numMetrics = metrics->numMetrics;
  }

  jlongArray longArray[Metrics::kNum];
  for (auto i = (int)Metrics::kBegin; i != (int)Metrics::kEnd; ++i) {
    longArray[i] = env->NewLongArray(numMetrics);
    if (metrics) {
      env->SetLongArrayRegion(longArray[i], 0, numMetrics, metrics->get((Metrics::TYPE)i));
    }
  }

  return env->NewObject(
      metricsBuilderClass,
      metricsBuilderConstructor,
      longArray[Metrics::kInputRows],
      longArray[Metrics::kInputVectors],
      longArray[Metrics::kInputBytes],
      longArray[Metrics::kRawInputRows],
      longArray[Metrics::kRawInputBytes],
      longArray[Metrics::kOutputRows],
      longArray[Metrics::kOutputVectors],
      longArray[Metrics::kOutputBytes],
      longArray[Metrics::kCpuCount],
      longArray[Metrics::kWallNanos],
      metrics ? metrics->veloxToArrow : -1,
      longArray[Metrics::kPeakMemoryBytes],
      longArray[Metrics::kNumMemoryAllocations],
      longArray[Metrics::kSpilledBytes],
      longArray[Metrics::kSpilledRows],
      longArray[Metrics::kSpilledPartitions],
      longArray[Metrics::kSpilledFiles],
      longArray[Metrics::kNumDynamicFiltersProduced],
      longArray[Metrics::kNumDynamicFiltersAccepted],
      longArray[Metrics::kNumReplacedWithDynamicFilterRows],
      longArray[Metrics::kFlushRowCount],
      longArray[Metrics::kScanTime],
      longArray[Metrics::kSkippedSplits],
      longArray[Metrics::kProcessedSplits],
      longArray[Metrics::kSkippedStrides],
      longArray[Metrics::kProcessedStrides],
      longArray[Metrics::kRemainingFilterTime],
      longArray[Metrics::kIoWaitTime],
      longArray[Metrics::kPreloadSplits],
      longArray[Metrics::kPhysicalWrittenBytes],
      longArray[Metrics::kNumWrittenFiles]);

  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeSpill( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle,
    jlong size) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto it = ctx->objectStore()->retrieve<ResultIterator>(iterHandle);
  return it->spillFixedSize(size);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeClose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  ctx->objectStore()->release(iterHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_NativeColumnarToRowJniWrapper_nativeColumnarToRowInit( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  // Convert the native batch to Spark unsafe row.
  return ctx->objectStore()->save(ctx->createColumnar2RowConverter(memoryManager));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jobject JNICALL
Java_org_apache_gluten_vectorized_NativeColumnarToRowJniWrapper_nativeColumnarToRowConvert( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle,
    jlong c2rHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto columnarToRowConverter = ctx->objectStore()->retrieve<ColumnarToRowConverter>(c2rHandle);
  auto cb = ctx->objectStore()->retrieve<ColumnarBatch>(batchHandle);
  columnarToRowConverter->convert(cb);

  const auto& offsets = columnarToRowConverter->getOffsets();
  const auto& lengths = columnarToRowConverter->getLengths();

  auto numRows = cb->numRows();

  auto offsetsArr = env->NewIntArray(numRows);
  auto offsetsSrc = reinterpret_cast<const jint*>(offsets.data());
  env->SetIntArrayRegion(offsetsArr, 0, numRows, offsetsSrc);
  auto lengthsArr = env->NewIntArray(numRows);
  auto lengthsSrc = reinterpret_cast<const jint*>(lengths.data());
  env->SetIntArrayRegion(lengthsArr, 0, numRows, lengthsSrc);
  long address = reinterpret_cast<long>(columnarToRowConverter->getBufferAddress());

  jobject nativeColumnarToRowInfo =
      env->NewObject(nativeColumnarToRowInfoClass, nativeColumnarToRowInfoConstructor, offsetsArr, lengthsArr, address);
  return nativeColumnarToRowInfo;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_NativeColumnarToRowJniWrapper_nativeClose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong c2rHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  ctx->objectStore()->release(c2rHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_NativeRowToColumnarJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  return ctx->objectStore()->save(
      ctx->createRow2ColumnarConverter(memoryManager, reinterpret_cast<struct ArrowSchema*>(cSchema)));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_NativeRowToColumnarJniWrapper_nativeConvertRowToColumnar( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong r2cHandle,
    jlongArray rowLength,
    jlong memoryAddress) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  if (rowLength == nullptr) {
    throw gluten::GlutenException("Native convert row to columnar: buf_addrs can't be null");
  }
  int numRows = env->GetArrayLength(rowLength);
  auto safeArray = gluten::getLongArrayElementsSafe(env, rowLength);
  uint8_t* address = reinterpret_cast<uint8_t*>(memoryAddress);

  auto converter = ctx->objectStore()->retrieve<RowToColumnarConverter>(r2cHandle);
  auto cb = converter->convert(numRows, safeArray.elems(), address);
  return ctx->objectStore()->save(cb);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_NativeRowToColumnarJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong r2cHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  ctx->objectStore()->release(r2cHandle);
  JNI_METHOD_END()
}

JNIEXPORT jstring JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_getType( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto batch = ctx->objectStore()->retrieve<ColumnarBatch>(batchHandle);
  return env->NewStringUTF(batch->getType().c_str());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_numBytes( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto batch = ctx->objectStore()->retrieve<ColumnarBatch>(batchHandle);
  return batch->numBytes();
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_numColumns( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto batch = ctx->objectStore()->retrieve<ColumnarBatch>(batchHandle);
  return batch->numColumns();
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_numRows( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto batch = ctx->objectStore()->retrieve<ColumnarBatch>(batchHandle);
  return batch->numRows();
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_compose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlongArray batchHandles) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  int handleCount = env->GetArrayLength(batchHandles);
  auto safeArray = gluten::getLongArrayElementsSafe(env, batchHandles);

  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  for (int i = 0; i < handleCount; ++i) {
    int64_t handle = safeArray.elems()[i];
    auto batch = ctx->objectStore()->retrieve<ColumnarBatch>(handle);
    batches.push_back(batch);
  }
  auto newBatch = CompositeColumnarBatch::create(std::move(batches));
  return ctx->objectStore()->save(newBatch);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_exportToArrow( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto batch = ctx->objectStore()->retrieve<ColumnarBatch>(batchHandle);
  std::shared_ptr<ArrowSchema> exportedSchema = batch->exportArrowSchema();
  std::shared_ptr<ArrowArray> exportedArray = batch->exportArrowArray();
  ArrowSchemaMove(exportedSchema.get(), reinterpret_cast<struct ArrowSchema*>(cSchema));
  ArrowArrayMove(exportedArray.get(), reinterpret_cast<struct ArrowArray*>(cArray));
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_createWithArrowArray( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  std::unique_ptr<ArrowSchema> targetSchema = std::make_unique<ArrowSchema>();
  std::unique_ptr<ArrowArray> targetArray = std::make_unique<ArrowArray>();
  auto* arrowSchema = reinterpret_cast<ArrowSchema*>(cSchema);
  auto* arrowArray = reinterpret_cast<ArrowArray*>(cArray);
  ArrowArrayMove(arrowArray, targetArray.get());
  ArrowSchemaMove(arrowSchema, targetSchema.get());
  std::shared_ptr<ColumnarBatch> batch =
      std::make_shared<ArrowCStructColumnarBatch>(std::move(targetSchema), std::move(targetArray));
  return ctx->objectStore()->save(batch);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_getForEmptySchema( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint numRows) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  return ctx->objectStore()->save(ctx->createOrGetEmptySchemaBatch(static_cast<int32_t>(numRows)));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_select( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong memoryManagerHandle,
    jlong batchHandle,
    jintArray jcolumnIndices) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  auto safeArray = gluten::getIntArrayElementsSafe(env, jcolumnIndices);
  int size = env->GetArrayLength(jcolumnIndices);
  std::vector<int32_t> columnIndices;
  for (int32_t i = 0; i < size; i++) {
    columnIndices.push_back(safeArray.elems()[i]);
  }

  return ctx->objectStore()->save(
      ctx->select(memoryManager, ctx->objectStore()->retrieve<ColumnarBatch>(batchHandle), std::move(columnIndices)));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  ctx->objectStore()->release(batchHandle);
  JNI_METHOD_END()
}

// Shuffle
JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_nativeMake( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jstring partitioningNameJstr,
    jint numPartitions,
    jint bufferSize,
    jint mergeBufferSize,
    jdouble mergeThreshold,
    jstring codecJstr,
    jstring codecBackendJstr,
    jint compressionLevel,
    jint compressionThreshold,
    jstring compressionModeJstr,
    jstring dataFileJstr,
    jint numSubDirs,
    jstring localDirsJstr,
    jlong memoryManagerHandle,
    jdouble reallocThreshold,
    jlong firstBatchHandle,
    jlong taskAttemptId,
    jint startPartitionId,
    jint pushBufferMaxSize,
    jobject partitionPusher,
    jstring partitionWriterTypeJstr) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);
  if (partitioningNameJstr == nullptr) {
    throw gluten::GlutenException(std::string("Short partitioning name can't be null"));
  }

  auto shuffleWriterOptions = ShuffleWriterOptions{
      .bufferSize = bufferSize,
      .bufferReallocThreshold = reallocThreshold,
      .partitioning = gluten::toPartitioning(jStringToCString(env, partitioningNameJstr)),
      .taskAttemptId = (int64_t)taskAttemptId,
      .startPartitionId = startPartitionId,
  };

  jclass cls = env->FindClass("java/lang/Thread");
  jmethodID mid = env->GetStaticMethodID(cls, "currentThread", "()Ljava/lang/Thread;");
  jobject thread = env->CallStaticObjectMethod(cls, mid);
  checkException(env);
  if (thread == NULL) {
    LOG(WARNING) << "Thread.currentThread() return NULL";
  } else {
    jmethodID midGetid = getMethodIdOrError(env, cls, "getId", "()J");
    jlong sid = env->CallLongMethod(thread, midGetid);
    checkException(env);
    shuffleWriterOptions.threadId = (int64_t)sid;
  }

  auto partitionWriterOptions = PartitionWriterOptions{
      .mergeBufferSize = mergeBufferSize,
      .mergeThreshold = mergeThreshold,
      .compressionThreshold = compressionThreshold,
      .compressionType = getCompressionType(env, codecJstr),
      .compressionLevel = compressionLevel,
      .bufferedWrite = true,
      .numSubDirs = numSubDirs,
      .pushBufferMaxSize = pushBufferMaxSize > 0 ? pushBufferMaxSize : kDefaultShuffleWriterBufferSize};
  if (codecJstr != NULL) {
    partitionWriterOptions.codecBackend = getCodecBackend(env, codecBackendJstr);
    partitionWriterOptions.compressionMode = getCompressionMode(env, compressionModeJstr);
  }
  std::unique_ptr<PartitionWriter> partitionWriter;

  auto partitionWriterTypeC = env->GetStringUTFChars(partitionWriterTypeJstr, JNI_FALSE);
  auto partitionWriterType = std::string(partitionWriterTypeC);
  env->ReleaseStringUTFChars(partitionWriterTypeJstr, partitionWriterTypeC);
  if (partitionWriterType == "local") {
    if (dataFileJstr == NULL) {
      throw gluten::GlutenException(std::string("Shuffle DataFile can't be null"));
    }
    if (localDirsJstr == NULL) {
      throw gluten::GlutenException(std::string("Shuffle DataFile can't be null"));
    }
    auto dataFileC = env->GetStringUTFChars(dataFileJstr, JNI_FALSE);
    auto dataFile = std::string(dataFileC);
    env->ReleaseStringUTFChars(dataFileJstr, dataFileC);

    auto localDirsC = env->GetStringUTFChars(localDirsJstr, JNI_FALSE);
    auto configuredDirs = gluten::splitPaths(std::string(localDirsC));
    env->ReleaseStringUTFChars(localDirsJstr, localDirsC);

    partitionWriter = std::make_unique<LocalPartitionWriter>(
        numPartitions,
        std::move(partitionWriterOptions),
        memoryManager->getArrowMemoryPool(),
        dataFile,
        configuredDirs);
  } else if (partitionWriterType == "celeborn") {
    jclass celebornPartitionPusherClass =
        createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
    jmethodID celebornPushPartitionDataMethod =
        getMethodIdOrError(env, celebornPartitionPusherClass, "pushPartitionData", "(I[BI)I");
    JavaVM* vm;
    if (env->GetJavaVM(&vm) != JNI_OK) {
      throw gluten::GlutenException("Unable to get JavaVM instance");
    }
    std::shared_ptr<CelebornClient> celebornClient =
        std::make_shared<CelebornClient>(vm, partitionPusher, celebornPushPartitionDataMethod);
    partitionWriter = std::make_unique<CelebornPartitionWriter>(
        numPartitions,
        std::move(partitionWriterOptions),
        memoryManager->getArrowMemoryPool(),
        std::move(celebornClient));
  } else {
    throw gluten::GlutenException("Unrecognizable partition writer type: " + partitionWriterType);
  }

  return ctx->objectStore()->save(ctx->createShuffleWriter(
      numPartitions, std::move(partitionWriter), std::move(shuffleWriterOptions), memoryManager));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_nativeEvict( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle,
    jlong size,
    jboolean callBySelf) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto shuffleWriter = ctx->objectStore()->retrieve<ShuffleWriter>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw gluten::GlutenException(errorMessage);
  }
  int64_t evictedSize;
  gluten::arrowAssertOkOrThrow(
      shuffleWriter->reclaimFixedSize(size, &evictedSize), "(shuffle) nativeEvict: evict failed");
  return (jlong)evictedSize;
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_split( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle,
    jint numRows,
    jlong batchHandle,
    jlong memLimit) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto shuffleWriter = ctx->objectStore()->retrieve<ShuffleWriter>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw gluten::GlutenException(errorMessage);
  }

  // The column batch maybe VeloxColumnBatch or ArrowCStructColumnarBatch(FallbackRangeShuffleWriter)
  auto batch = ctx->objectStore()->retrieve<ColumnarBatch>(batchHandle);
  auto numBytes = batch->numBytes();
  gluten::arrowAssertOkOrThrow(shuffleWriter->split(batch, memLimit), "Native split: shuffle writer split failed");
  return numBytes;
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_stop( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto shuffleWriter = ctx->objectStore()->retrieve<ShuffleWriter>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw gluten::GlutenException(errorMessage);
  }

  gluten::arrowAssertOkOrThrow(shuffleWriter->stop(), "Native shuffle write: ShuffleWriter stop failed");

  const auto& partitionLengths = shuffleWriter->partitionLengths();
  auto partitionLengthArr = env->NewLongArray(partitionLengths.size());
  auto src = reinterpret_cast<const jlong*>(partitionLengths.data());
  env->SetLongArrayRegion(partitionLengthArr, 0, partitionLengths.size(), src);

  const auto& rawPartitionLengths = shuffleWriter->rawPartitionLengths();
  auto rawPartitionLengthArr = env->NewLongArray(rawPartitionLengths.size());
  auto rawSrc = reinterpret_cast<const jlong*>(rawPartitionLengths.data());
  env->SetLongArrayRegion(rawPartitionLengthArr, 0, rawPartitionLengths.size(), rawSrc);

  jobject splitResult = env->NewObject(
      splitResultClass,
      splitResultConstructor,
      0L,
      shuffleWriter->totalWriteTime(),
      shuffleWriter->totalEvictTime(),
      shuffleWriter->totalCompressTime(),
      shuffleWriter->totalBytesWritten(),
      shuffleWriter->totalBytesEvicted(),
      shuffleWriter->maxPartitionBufferSize(),
      partitionLengthArr,
      rawPartitionLengthArr);

  return splitResult;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  ctx->objectStore()->release(shuffleWriterHandle);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_OnHeapJniByteInputStream_memCopyFromHeap( // NOLINT
    JNIEnv* env,
    jobject,
    jbyteArray source,
    jlong destAddress,
    jint size) {
  JNI_METHOD_START
  auto safeArray = gluten::getByteArrayElementsSafe(env, source);
  std::memcpy(reinterpret_cast<void*>(destAddress), safeArray.elems(), size);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_make( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jlong memoryManagerHandle,
    jstring compressionType,
    jstring compressionBackend,
    jint batchSize) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  auto pool = memoryManager->getArrowMemoryPool();
  ShuffleReaderOptions options = ShuffleReaderOptions{};
  options.compressionType = getCompressionType(env, compressionType);
  if (compressionType != nullptr) {
    options.codecBackend = getCodecBackend(env, compressionBackend);
  }
  options.batchSize = batchSize;
  // TODO: Add coalesce option and maximum coalesced size.
  std::shared_ptr<arrow::Schema> schema =
      gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));

  return ctx->objectStore()->save(ctx->createShuffleReader(schema, options, pool, memoryManager));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_readStream( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle,
    jobject jniIn) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto reader = ctx->objectStore()->retrieve<ShuffleReader>(shuffleReaderHandle);
  std::shared_ptr<arrow::io::InputStream> in = std::make_shared<JavaInputStreamAdaptor>(env, reader->getPool(), jniIn);
  auto outItr = reader->readStream(in);
  return ctx->objectStore()->save(outItr);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_populateMetrics( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle,
    jobject metrics) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto reader = ctx->objectStore()->retrieve<ShuffleReader>(shuffleReaderHandle);
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDecompressTime, reader->getDecompressTime());
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetIpcTime, reader->getIpcTime());
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDeserializeTime, reader->getDeserializeTime());

  checkException(env);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto reader = ctx->objectStore()->retrieve<ShuffleReader>(shuffleReaderHandle);
  GLUTEN_THROW_NOT_OK(reader->close());
  ctx->objectStore()->release(shuffleReaderHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_datasource_DatasourceJniWrapper_nativeInitDatasource( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jstring filePath,
    jlong cSchema,
    jlong memoryManagerHandle,
    jbyteArray options) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  ResourceHandle handle = kInvalidResourceHandle;

  if (cSchema == -1) {
    // Only inspect the schema and not write
    handle = ctx->objectStore()->save(ctx->createDatasource(jStringToCString(env, filePath), memoryManager, nullptr));
  } else {
    auto safeArray = gluten::getByteArrayElementsSafe(env, options);
    auto datasourceOptions = gluten::parseConfMap(env, safeArray.elems(), safeArray.length());
    auto& sparkConf = ctx->getConfMap();
    datasourceOptions.insert(sparkConf.begin(), sparkConf.end());
    auto schema = gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));
    handle = ctx->objectStore()->save(ctx->createDatasource(jStringToCString(env, filePath), memoryManager, schema));
    auto datasource = ctx->objectStore()->retrieve<Datasource>(handle);
    datasource->init(datasourceOptions);
  }

  return handle;
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_DatasourceJniWrapper_inspectSchema( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jlong cSchema) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto datasource = ctx->objectStore()->retrieve<Datasource>(dsHandle);
  datasource->inspectSchema(reinterpret_cast<struct ArrowSchema*>(cSchema));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_DatasourceJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto datasource = ctx->objectStore()->retrieve<Datasource>(dsHandle);
  datasource->close();
  ctx->objectStore()->release(dsHandle);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_DatasourceJniWrapper_write( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jobject jIter) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto datasource = ctx->objectStore()->retrieve<Datasource>(dsHandle);
  auto iter = makeJniColumnarBatchIterator(env, jIter, ctx, nullptr);
  while (true) {
    auto batch = iter->next();
    if (!batch) {
      break;
    }
    datasource->write(batch);
  }
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_org_apache_gluten_datasource_DatasourceJniWrapper_splitBlockByPartitionAndBucket( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle,
    jintArray partitionColIndice,
    jboolean hasBucket,
    jlong memoryManagerId) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto batch = ctx->objectStore()->retrieve<ColumnarBatch>(batchHandle);
  auto safeArray = gluten::getIntArrayElementsSafe(env, partitionColIndice);
  int size = env->GetArrayLength(partitionColIndice);
  std::vector<int32_t> partitionColIndiceVec;
  for (int i = 0; i < size; ++i) {
    partitionColIndiceVec.push_back(safeArray.elems()[i]);
  }

  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  auto result = batch->getRowBytes(0);
  auto rowBytes = result.first;
  auto newBatchHandle = ctx->objectStore()->save(ctx->select(memoryManager, batch, partitionColIndiceVec));

  auto bytesSize = result.second;
  jbyteArray bytesArray = env->NewByteArray(bytesSize);
  env->SetByteArrayRegion(bytesArray, 0, bytesSize, reinterpret_cast<jbyte*>(rowBytes));
  delete[] rowBytes;

  jlongArray batchArray = env->NewLongArray(1);
  long* cBatchArray = new long[1];
  cBatchArray[0] = newBatchHandle;
  env->SetLongArrayRegion(batchArray, 0, 1, cBatchArray);
  delete[] cBatchArray;

  jobject block_stripes = env->NewObject(
      block_stripes_class,
      block_stripes_constructor,
      batchHandle,
      batchArray,
      nullptr,
      batch->numColumns(),
      bytesArray);
  return block_stripes;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_alloc_NativeMemoryAllocator_getAllocator( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jTypeName) {
  JNI_METHOD_START
  std::string typeName = jStringToCString(env, jTypeName);
  std::shared_ptr<MemoryAllocator>* allocator = new std::shared_ptr<MemoryAllocator>;
  if (typeName == "DEFAULT") {
    *allocator = defaultMemoryAllocator();
  } else {
    delete allocator;
    allocator = nullptr;
    throw GlutenException("Unexpected allocator type name: " + typeName);
  }
  return reinterpret_cast<jlong>(allocator);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_memory_alloc_NativeMemoryAllocator_releaseAllocator( // NOLINT
    JNIEnv* env,
    jclass,
    jlong allocatorId) {
  JNI_METHOD_START
  delete reinterpret_cast<std::shared_ptr<MemoryAllocator>*>(allocatorId);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_alloc_NativeMemoryAllocator_bytesAllocated( // NOLINT
    JNIEnv* env,
    jclass,
    jlong allocatorId) {
  JNI_METHOD_START
  auto* alloc = reinterpret_cast<std::shared_ptr<MemoryAllocator>*>(allocatorId);
  if (alloc == nullptr) {
    throw gluten::GlutenException("Memory allocator instance not found. It may not exist nor has been closed");
  }
  return (*alloc)->getBytes();
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_nmm_NativeMemoryManager_create( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jbackendType,
    jstring jnmmName,
    jlong allocatorId,
    jlong reservationBlockSize,
    jobject jlistener) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  auto allocator = reinterpret_cast<std::shared_ptr<MemoryAllocator>*>(allocatorId);
  if (allocator == nullptr) {
    throw gluten::GlutenException("Allocator does not exist or has been closed");
  }

  std::unique_ptr<AllocationListener> listener = std::make_unique<SparkAllocationListener>(
      vm, jlistener, reserveMemoryMethod, unreserveMemoryMethod, reservationBlockSize);

  if (gluten::backtrace_allocation) {
    listener = std::make_unique<BacktraceAllocationListener>(std::move(listener));
  }

  auto name = jStringToCString(env, jnmmName);
  auto backendType = jStringToCString(env, jbackendType);
  // TODO: move memory manager into Runtime then we can use more general Runtime.
  auto runtime = gluten::Runtime::create(backendType);
  auto manager = runtime->createMemoryManager(name, *allocator, std::move(listener));
  gluten::Runtime::release(runtime);
  return reinterpret_cast<jlong>(manager);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_gluten_memory_nmm_NativeMemoryManager_collectMemoryUsage( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  const MemoryUsageStats& stats = memoryManager->collectMemoryUsageStats();
  auto size = stats.ByteSizeLong();
  jbyteArray out = env->NewByteArray(size);
  uint8_t buffer[size];
  GLUTEN_CHECK(
      stats.SerializeToArray(reinterpret_cast<void*>(buffer), size),
      "Serialization failed when collecting memory usage stats");
  env->SetByteArrayRegion(out, 0, size, reinterpret_cast<jbyte*>(buffer));
  return out;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_nmm_NativeMemoryManager_shrink( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerHandle,
    jlong size) {
  JNI_METHOD_START
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);
  return memoryManager->shrink(static_cast<int64_t>(size));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_memory_nmm_NativeMemoryManager_hold( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);
  memoryManager->hold();
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_memory_nmm_NativeMemoryManager_release( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);
  delete memoryManager;
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_serialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlongArray handles,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  int32_t numBatches = env->GetArrayLength(handles);
  auto safeArray = gluten::getLongArrayElementsSafe(env, handles);

  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  int64_t numRows = 0L;
  for (int32_t i = 0; i < numBatches; i++) {
    auto batch = ctx->objectStore()->retrieve<ColumnarBatch>(safeArray.elems()[i]);
    GLUTEN_DCHECK(
        batch != nullptr, "Cannot find the ColumnarBatch with handle " + std::to_string(safeArray.elems()[i]));
    numRows += batch->numRows();
    batches.emplace_back(batch);
  }

  auto arrowPool = memoryManager->getArrowMemoryPool();
  auto serializer = ctx->createColumnarBatchSerializer(memoryManager, arrowPool, nullptr);
  auto buffer = serializer->serializeColumnarBatches(batches);
  auto bufferArr = env->NewByteArray(buffer->size());
  env->SetByteArrayRegion(bufferArr, 0, buffer->size(), reinterpret_cast<const jbyte*>(buffer->data()));

  jobject columnarBatchSerializeResult =
      env->NewObject(columnarBatchSerializeResultClass, columnarBatchSerializeResultConstructor, numRows, bufferArr);

  return columnarBatchSerializeResult;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  auto arrowPool = memoryManager->getArrowMemoryPool();
  return ctx->objectStore()->save(
      ctx->createColumnarBatchSerializer(memoryManager, arrowPool, reinterpret_cast<struct ArrowSchema*>(cSchema)));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_deserialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong serializerHandle,
    jbyteArray data) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto serializer = ctx->objectStore()->retrieve<ColumnarBatchSerializer>(serializerHandle);
  GLUTEN_DCHECK(serializer != nullptr, "ColumnarBatchSerializer cannot be null");
  int32_t size = env->GetArrayLength(data);
  auto safeArray = gluten::getByteArrayElementsSafe(env, data);
  auto batch = serializer->deserialize(safeArray.elems(), size);
  return ctx->objectStore()->save(batch);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong serializerHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  ctx->objectStore()->release(serializerHandle);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
