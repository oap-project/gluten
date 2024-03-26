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

#include <dlfcn.h>
#include <google/protobuf/arena.h>
#include <velox/expression/SignatureBinder.h>
#include <velox/expression/VectorFunction.h>
#include <velox/type/fbhive/HiveTypeParser.h>
#include <functional>
#include <vector>

#include "Udf.h"
#include "UdfLoader.h"
#include "utils/StringUtil.h"
#include "utils/exception.h"
#include "utils/macros.h"

namespace {

void* loadSymFromLibrary(void* handle, const std::string& libPath, const std::string& func) {
  void* sym = dlsym(handle, func.c_str());
  if (!sym) {
    throw gluten::GlutenException(func + " not found in " + libPath);
  }
  return sym;
}

} // namespace

namespace gluten {

void UdfLoader::loadUdfLibraries(const std::string& libPaths) {
  const auto& paths = splitPaths(libPaths, /*checkExists=*/true);
  loadUdfLibraries0(paths);
}

void UdfLoader::loadUdfLibraries0(const std::vector<std::string>& libPaths) {
  for (const auto& libPath : libPaths) {
    if (handles_.find(libPath) == handles_.end()) {
      void* handle = dlopen(libPath.c_str(), RTLD_LAZY);
      handles_[libPath] = handle;
    }
    LOG(INFO) << "Successfully loaded udf library: " << libPath;
  }
}

std::unordered_set<std::shared_ptr<UdfLoader::UdfSignature>> UdfLoader::getRegisteredUdfSignatures() {
  std::unordered_set<std::shared_ptr<UdfSignature>> signatures;
  for (const auto& item : handles_) {
    const auto& libPath = item.first;
    const auto& handle = item.second;
    void* getNumUdfSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_NUM_UDF));
    auto getNumUdf = reinterpret_cast<int (*)()>(getNumUdfSym);
    // allocate
    int numUdf = getNumUdf();
    UdfEntry* udfEntry = static_cast<UdfEntry*>(malloc(sizeof(UdfEntry) * numUdf));

    void* getUdfEntriesSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_UDF_ENTRIES));
    auto getUdfEntries = reinterpret_cast<void (*)(UdfEntry*)>(getUdfEntriesSym);
    getUdfEntries(udfEntry);

    for (auto i = 0; i < numUdf; ++i) {
      const auto& entry = udfEntry[i];
      auto dataType = toSubstraitTypeStr(entry.dataType);
      auto argTypes = toSubstraitTypeStr(entry.numArgs, entry.argTypes);
      if (entry.isUdaf) {
        auto intermediateType = toSubstraitTypeStr(entry.intermediateType);
        signatures.insert(std::make_shared<UdfSignature>(entry.name, dataType, argTypes, intermediateType));
      } else {
        signatures.insert(std::make_shared<UdfSignature>(entry.name, dataType, argTypes));
      }
    }
    free(udfEntry);
  }
  return signatures;
}

void UdfLoader::registerUdf() {
  for (const auto& item : handles_) {
    void* sym = loadSymFromLibrary(item.second, item.first, GLUTEN_TOSTRING(GLUTEN_REGISTER_UDF));
    auto registerUdf = reinterpret_cast<void (*)()>(sym);
    registerUdf();
  }
}

bool UdfLoader::validateUdf(const std::string& name, const std::vector<facebook::velox::TypePtr>& argTypes) {
  const auto& functionMap = facebook::velox::exec::vectorFunctionFactories();
  auto got = functionMap->find(name);
  if (got != functionMap->end()) {
    const auto& entry = got->second;
    for (auto& signature : entry.signatures) {
      facebook::velox::exec::SignatureBinder binder(*signature, argTypes);
      if (binder.tryBind()) {
        return true;
      }
    }
  }
  return false;
}

std::shared_ptr<UdfLoader> UdfLoader::getInstance() {
  static auto instance = std::make_shared<UdfLoader>();
  return instance;
}
} // namespace gluten
