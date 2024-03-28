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
#include <vector>
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/fbhive/HiveTypeParser.h"

#include "Udaf.h"
#include "Udf.h"
#include "UdfLoader.h"
#include "utils/StringUtil.h"
#include "utils/exception.h"
#include "utils/macros.h"

namespace {

void* loadSymFromLibrary(
    void* handle,
    const std::string& libPath,
    const std::string& func,
    bool throwIfNotFound = true) {
  void* sym = dlsym(handle, func.c_str());
  if (!sym && throwIfNotFound) {
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
  if (!signatures_.empty()) {
    return signatures_;
  }
  for (const auto& item : handles_) {
    const auto& libPath = item.first;
    const auto& handle = item.second;

    // Handle UDFs.
    void* getNumUdfSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_NUM_UDF), false);
    if (getNumUdfSym) {
      auto getNumUdf = reinterpret_cast<int (*)()>(getNumUdfSym);
      int numUdf = getNumUdf();
      // allocate
      UdfEntry* udfEntries = static_cast<UdfEntry*>(malloc(sizeof(UdfEntry) * numUdf));

      void* getUdfEntriesSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_UDF_ENTRIES));
      auto getUdfEntries = reinterpret_cast<void (*)(UdfEntry*)>(getUdfEntriesSym);
      getUdfEntries(udfEntries);

      for (auto i = 0; i < numUdf; ++i) {
        const auto& entry = udfEntries[i];
        auto dataType = toSubstraitTypeStr(entry.dataType);
        auto argTypes = toSubstraitTypeStr(entry.numArgs, entry.argTypes);
        signatures_.insert(std::make_shared<UdfSignature>(entry.name, dataType, argTypes));
      }
      free(udfEntries);
    } else {
      LOG(INFO) << "No UDFs found in " << libPath;
    }

    // Handle UDAFs.
    void* getNumUdafSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_NUM_UDAF), false);
    if (getNumUdafSym) {
      auto getNumUdaf = reinterpret_cast<int (*)()>(getNumUdafSym);
      int numUdaf = getNumUdaf();
      // allocate
      UdafEntry* udafEntries = static_cast<UdafEntry*>(malloc(sizeof(UdafEntry) * numUdaf));

      void* getUdafEntriesSym = loadSymFromLibrary(handle, libPath, GLUTEN_TOSTRING(GLUTEN_GET_UDAF_ENTRIES));
      auto getUdafEntries = reinterpret_cast<void (*)(UdafEntry*)>(getUdafEntriesSym);
      getUdafEntries(udafEntries);

      for (auto i = 0; i < numUdaf; ++i) {
        const auto& entry = udafEntries[i];
        auto dataType = toSubstraitTypeStr(entry.dataType);
        auto argTypes = toSubstraitTypeStr(entry.numArgs, entry.argTypes);
        auto intermediateType = toSubstraitTypeStr(entry.intermediateType);
        signatures_.insert(std::make_shared<UdfSignature>(entry.name, dataType, argTypes, intermediateType));
      }
      free(udafEntries);
    } else {
      LOG(INFO) << "No UDAFs found in " << libPath;
    }
  }
  return signatures_;
}

std::unordered_set<std::string> UdfLoader::getRegisteredUdafNames() {
  if (handles_.empty()) {
    return {};
  }
  if (!names_.empty()) {
    return names_;
  }
  if (signatures_.empty()) {
    getRegisteredUdfSignatures();
  }
  for (const auto& sig : signatures_) {
    if (!sig->intermediateType.empty()) {
      names_.insert(sig->name);
    }
  }
  return names_;
}

void UdfLoader::registerUdf() {
  for (const auto& item : handles_) {
    void* sym = loadSymFromLibrary(item.second, item.first, GLUTEN_TOSTRING(GLUTEN_REGISTER_UDF));
    auto registerUdf = reinterpret_cast<void (*)()>(sym);
    registerUdf();
  }
}

std::shared_ptr<UdfLoader> UdfLoader::getInstance() {
  static auto instance = std::make_shared<UdfLoader>();
  return instance;
}

} // namespace gluten
