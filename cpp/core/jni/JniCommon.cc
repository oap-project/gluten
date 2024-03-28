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

#include "JniCommon.h"

void gluten::JniCommonState::ensureInitialized(JNIEnv* env) {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (initialized_) {
    return;
  }
  initialize(env);
  initialized_ = true;
}

void gluten::JniCommonState::assertInitialized() {
  if (!initialized_) {
    throw gluten::GlutenException("Fatal: JniCommonState::Initialize(...) was not called before using the utility");
  }
}

jmethodID gluten::JniCommonState::runtimeAwareCtxHandle() {
  assertInitialized();
  return runtimeAwareCtxHandle_;
}

void gluten::JniCommonState::initialize(JNIEnv* env) {
  runtimeAwareClass_ = createGlobalClassReference(env, "Lorg/apache/gluten/exec/RuntimeAware;");
  runtimeAwareCtxHandle_ = getMethodIdOrError(env, runtimeAwareClass_, "handle", "()J");
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  vm_ = vm;
}

void gluten::JniCommonState::close() {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (closed_) {
    return;
  }
  JNIEnv* env;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  env->DeleteGlobalRef(runtimeAwareClass_);
  closed_ = true;
}

gluten::Runtime* gluten::getRuntime(JNIEnv* env, jobject runtimeAware) {
  int64_t ctxHandle = env->CallLongMethod(runtimeAware, getJniCommonState()->runtimeAwareCtxHandle());
  checkException(env);
  auto ctx = reinterpret_cast<Runtime*>(ctxHandle);
  GLUTEN_CHECK(ctx != nullptr, "FATAL: resource instance should not be null.");
  return ctx;
}
