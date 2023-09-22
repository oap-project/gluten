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

#pragma once

#include <stdexcept>

#include "JniCommon.h"
#include "utils/exception.h"

#define JNI_METHOD_START try {
// macro ended

#define JNI_METHOD_END(fallback_expr)                                             \
  }                                                                               \
  catch (std::exception & e) {                                                    \
    env->ThrowNew(gluten::getJniErrorState()->runtimeExceptionClass(), e.what()); \
    return fallback_expr;                                                         \
  }
// macro ended

namespace gluten {

class JniErrorState {
 public:
  virtual ~JniErrorState() = default;

  void ensureInitialized(JNIEnv* env);

  void assertInitialized();

  void close();

  jclass runtimeExceptionClass();

  jclass illegalAccessExceptionClass();

 private:
  void initialize(JNIEnv* env);

  jclass ioExceptionClass_ = nullptr;
  jclass runtimeExceptionClass_ = nullptr;
  jclass unsupportedOperationExceptionClass_ = nullptr;
  jclass illegalAccessExceptionClass_ = nullptr;
  jclass illegalArgumentExceptionClass_ = nullptr;
  JavaVM* vm_;
  bool initialized_{false};
  bool closed_{false};
  std::mutex mtx_;
};

inline JniErrorState* getJniErrorState() {
  static JniErrorState jniErrorState;
  return &jniErrorState;
}

} // namespace gluten
