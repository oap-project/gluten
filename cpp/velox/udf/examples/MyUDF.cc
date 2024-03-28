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

#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <iostream>
#include "udf/Udf.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::exec;

static const char* kInteger = "int";
static const char* kBigInt = "bigint";
static const char* kDate = "date";

template <TypeKind Kind>
class PlusConstantFunction : public exec::VectorFunction {
 public:
  explicit PlusConstantFunction(int32_t addition) : addition_(addition) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    using nativeType = typename TypeTraits<Kind>::NativeType;
    VELOX_CHECK_EQ(args.size(), 1);

    auto& arg = args[0];

    // The argument may be flat or constant.
    VELOX_CHECK(arg->isFlatEncoding() || arg->isConstantEncoding());

    BaseVector::ensureWritable(rows, createScalarType<Kind>(), context.pool(), result);

    auto* flatResult = result->asFlatVector<nativeType>();
    auto* rawResult = flatResult->mutableRawValues();

    flatResult->clearNulls(rows);

    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<nativeType>>()->valueAt(0);
      rows.applyToSelected([&](auto row) { rawResult[row] = value + addition_; });
    } else {
      auto* rawInput = arg->as<FlatVector<nativeType>>()->rawValues();

      rows.applyToSelected([&](auto row) { rawResult[row] = rawInput[row] + addition_; });
    }
  }

 private:
  const int32_t addition_;
};

template <typename T>
struct MyDateSimpleFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date, const arg_type<int32_t> addition) {
    result = date + addition;
  }
};

std::shared_ptr<facebook::velox::exec::VectorFunction> makeMyUdf1(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto typeKind = inputArgs[0].type->kind();
  switch (typeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<PlusConstantFunction<TypeKind::INTEGER>>(5);
    case TypeKind::BIGINT:
      return std::make_shared<PlusConstantFunction<TypeKind::BIGINT>>(5);
    default:
      VELOX_UNREACHABLE();
  }
}

static std::vector<std::shared_ptr<exec::FunctionSignature>> integerSignatures() {
  // integer -> integer, bigint ->bigint
  return {
      exec::FunctionSignatureBuilder().returnType("integer").argumentType("integer").build(),
      exec::FunctionSignatureBuilder().returnType("bigint").argumentType("bigint").build()};
}

static std::vector<std::shared_ptr<exec::FunctionSignature>> bigintSignatures() {
  // bigint -> bigint
  return {exec::FunctionSignatureBuilder().returnType("bigint").argumentType("bigint").build()};
}

} // namespace

const int kNumMyUdf = 4;

DEFINE_GET_NUM_UDF {
  return kNumMyUdf;
}

const char* myUdf1Arg1[] = {kInteger};
const char* myUdf1Arg2[] = {kBigInt};
const char* myUdf2Arg1[] = {kBigInt};
const char* myDateArg[] = {kDate, kInteger};
DEFINE_GET_UDF_ENTRIES {
  int index = 0;
  udfEntries[index++] = {"myudf1", kInteger, 1, myUdf1Arg1};
  udfEntries[index++] = {"myudf1", kBigInt, 1, myUdf1Arg2};
  udfEntries[index++] = {"myudf2", kBigInt, 1, myUdf2Arg1};
  udfEntries[index++] = {"mydate", kDate, 2, myDateArg};
}

DEFINE_REGISTER_UDF {
  facebook::velox::exec::registerStatefulVectorFunction("myudf1", integerSignatures(), makeMyUdf1);
  facebook::velox::exec::registerVectorFunction(
      "myudf2", bigintSignatures(), std::make_unique<PlusConstantFunction<facebook::velox::TypeKind::BIGINT>>(5));
  facebook::velox::registerFunction<MyDateSimpleFunction, Date, Date, int32_t>({"mydate"});
}
