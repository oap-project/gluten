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
#include <velox/functions/lib/aggregates/AverageAggregateBase.h>
#include <iostream>
#include "udf/Udf.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::functions::aggregate;

static const char* kInteger = "int";
static const char* kBigInt = "bigint";
static const char* kDate = "date";
static const char* kDouble = "double";

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

namespace {

template <typename TInput, typename TAccumulator, typename TResult>
class AverageAggregate : public AverageAggregateBase<TInput, TAccumulator, TResult> {
 public:
  explicit AverageAggregate(TypePtr resultType) : AverageAggregateBase<TInput, TAccumulator, TResult>(resultType) {}

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result) override {
    auto rowVector = (*result)->as<RowVector>();
    auto sumVector = rowVector->childAt(0)->asFlatVector<TAccumulator>();
    auto countVector = rowVector->childAt(1)->asFlatVector<int64_t>();

    rowVector->resize(numGroups);
    sumVector->resize(numGroups);
    countVector->resize(numGroups);
    rowVector->clearAllNulls();

    int64_t* rawCounts = countVector->mutableRawValues();
    TAccumulator* rawSums = sumVector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      // When all inputs are nulls, the partial result is (0, 0).
      char* group = groups[i];
      auto* sumCount = this->accumulator(group);
      rawCounts[i] = sumCount->count;
      rawSums[i] = sumCount->sum;
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    auto vector = (*result)->as<FlatVector<TResult>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = this->getRawNulls(vector);

    TResult* rawValues = vector->mutableRawValues();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto* sumCount = this->accumulator(group);
      if (sumCount->count == 0) {
        // In Spark, if all inputs are null, count will be 0,
        // and the result of final avg will be null.
        vector->setNull(i, true);
      } else {
        this->clearNull(rawNulls, i);
        rawValues[i] = (TResult)sumCount->sum / sumCount->count;
      }
    }
  }
};

exec::AggregateRegistrationResult
registerAverage(const std::string& name, bool withCompanionFunctions, bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : {"smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("row(double,bigint)")
                             .argumentType(inputType)
                             .build());
  }

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(argTypes.size(), 1, "{} takes at most one argument", name);
        auto inputType = argTypes[0];
        if (exec::isRawInput(step)) {
          switch (inputType->kind()) {
            case TypeKind::SMALLINT:
              return std::make_unique<AverageAggregate<int16_t, double, double>>(resultType);
            case TypeKind::INTEGER:
              return std::make_unique<AverageAggregate<int32_t, double, double>>(resultType);
            case TypeKind::BIGINT: {
              if (inputType->isShortDecimal()) {
                return std::make_unique<DecimalAverageAggregateBase<int64_t>>(resultType);
              }
              return std::make_unique<AverageAggregate<int64_t, double, double>>(resultType);
            }
            case TypeKind::HUGEINT: {
              if (inputType->isLongDecimal()) {
                return std::make_unique<DecimalAverageAggregateBase<int128_t>>(resultType);
              }
              VELOX_NYI();
            }
            case TypeKind::REAL:
              return std::make_unique<AverageAggregate<float, double, double>>(resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<AverageAggregate<double, double, double>>(resultType);
            default:
              VELOX_FAIL("Unknown input type for {} aggregation {}", name, inputType->kindName());
          }
        } else {
          checkAvgIntermediateType(inputType);
          switch (resultType->kind()) {
            case TypeKind::REAL:
              return std::make_unique<AverageAggregate<int64_t, double, float>>(resultType);
            case TypeKind::DOUBLE:
            case TypeKind::ROW:
              return std::make_unique<AverageAggregate<int64_t, double, double>>(resultType);
            case TypeKind::BIGINT:
              return std::make_unique<DecimalAverageAggregateBase<int64_t>>(resultType);
            case TypeKind::HUGEINT:
              return std::make_unique<DecimalAverageAggregateBase<int128_t>>(resultType);
            case TypeKind::VARBINARY:
              if (inputType->isLongDecimal()) {
                return std::make_unique<DecimalAverageAggregateBase<int128_t>>(resultType);
              } else if (inputType->isShortDecimal() || inputType->kind() == TypeKind::VARBINARY) {
                // If the input and out type are VARBINARY, then the
                // LongDecimalWithOverflowState is used and the template type
                // does not matter.
                return std::make_unique<DecimalAverageAggregateBase<int64_t>>(resultType);
              }
              [[fallthrough]];
            default:
              VELOX_FAIL("Unsupported result type for final aggregation: {}", resultType->kindName());
          }
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace

const int kNumMyUdf = 5;

DEFINE_GET_NUM_UDF {
  return kNumMyUdf;
}

const char* myUdf1Arg1[] = {kInteger};
const char* myUdf1Arg2[] = {kBigInt};
const char* myUdf2Arg1[] = {kBigInt};
const char* myDateArg[] = {kDate, kInteger};
const char* myAvgArg[] = {kInteger};
const char* myAvgIntermediateType = "struct<a:double,b:bigint>";
DEFINE_GET_UDF_ENTRIES {
  udfEntries[0] = {"myudf1", kInteger, 1, myUdf1Arg1};
  udfEntries[1] = {"myudf1", kBigInt, 1, myUdf1Arg2};
  udfEntries[2] = {"myudf2", kBigInt, 1, myUdf2Arg1};
  udfEntries[3] = {"mydate", kDate, 2, myDateArg};
  udfEntries[4] = {"myavg", kDouble, 1, myAvgArg, true, myAvgIntermediateType};
}

DEFINE_REGISTER_UDF {
  facebook::velox::exec::registerStatefulVectorFunction("myudf1", integerSignatures(), makeMyUdf1);
  facebook::velox::exec::registerVectorFunction(
      "myudf2", bigintSignatures(), std::make_unique<PlusConstantFunction<facebook::velox::TypeKind::BIGINT>>(5));
  facebook::velox::registerFunction<MyDateSimpleFunction, Date, Date, int32_t>({"mydate"});

  registerAverage("myavg", true, true);
}
