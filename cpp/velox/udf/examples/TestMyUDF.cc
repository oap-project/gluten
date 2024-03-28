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

#include <iostream>
#include "udf/UdfLoader.h"

#include "velox/expression/VectorFunction.h"

int main() {
  auto udfLoader = gluten::UdfLoader::getInstance();
  udfLoader->loadUdfLibraries("libmyudf.so");
  udfLoader->registerUdf();

  auto map = facebook::velox::exec::vectorFunctionFactories();
  const std::vector<std::string> candidates = {"myudf1", "myudf2"};
  auto f = map.withRLock([&candidates](auto& self) -> bool {
    return std::all_of(candidates.begin(), candidates.end(), [&](const auto& funcName) {
      auto iter = self.find(funcName);
      std::unordered_map<std::string, std::string> values;
      const facebook::velox::core::QueryConfig config(std::move(values));
      return iter->second.factory(
                 funcName, {facebook::velox::exec::VectorFunctionArg{facebook::velox::BIGINT()}}, config) != nullptr;
    });
  });

  if (!f) {
    return 1;
  }

  return 0;
}
