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

#include <velox/exec/SimpleAggregateAdapter.h>
#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/lib/aggregates/AverageAggregateBase.h>
#include <iostream>
#include "udf/Udf.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::aggregate;

static const char* kInteger = "int";
static const char* kBigInt = "bigint";
static const char* kDate = "date";
static const char* kFloat = "float";
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

/////////////////////////////// Simple Aggregate Function ///////////////////////////////

// Copied from velox/exec/tests/SimpleAverageAggregate.cpp

// Implementation of the average aggregation function through the
// SimpleAggregateAdapter.
template <typename T>
class AverageAggregate {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = Row<T>;

  // Type of intermediate result vector wrapped in Row.
  using IntermediateType =
      Row</*sum*/ double,
          /*count*/ int64_t>;

  // Type of output vector.
  using OutputType = std::conditional_t<std::is_same_v<T, float>, float, double>;

  static bool toIntermediate(exec::out_type<Row<double, int64_t>>& out, exec::arg_type<T> in) {
    out.copy_from(std::make_tuple(static_cast<double>(in), 1));
    return true;
  }

  struct AccumulatorType {
    double sum_;
    int64_t count_;

    AccumulatorType() = delete;

    // Constructor used in initializeNewGroups().
    explicit AccumulatorType(HashStringAllocator* /*allocator*/) {
      sum_ = 0;
      count_ = 0;
    }

    // addInput expects one parameter of exec::arg_type<T> for each child-type T
    // wrapped in InputType.
    void addInput(HashStringAllocator* /*allocator*/, exec::arg_type<T> data) {
      sum_ += data;
      count_ = checkedPlus<int64_t>(count_, 1);
    }

    // combine expects one parameter of exec::arg_type<IntermediateType>.
    void combine(HashStringAllocator* /*allocator*/, exec::arg_type<Row<double, int64_t>> other) {
      // Both field of an intermediate result should be non-null because
      // writeIntermediateResult() never make an intermediate result with a
      // single null.
      VELOX_CHECK(other.at<0>().has_value());
      VELOX_CHECK(other.at<1>().has_value());
      sum_ += other.at<0>().value();
      count_ = checkedPlus<int64_t>(count_, other.at<1>().value());
    }

    bool writeFinalResult(exec::out_type<OutputType>& out) {
      out = sum_ / count_;
      return true;
    }

    bool writeIntermediateResult(exec::out_type<IntermediateType>& out) {
      out = std::make_tuple(sum_, count_);
      return true;
    }
  };
};

exec::AggregateRegistrationResult registerSimpleAverageAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : {"smallint", "integer", "bigint", "double"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("row(double,bigint)")
                             .argumentType(inputType)
                             .build());
  }

  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("real")
                           .intermediateType("row(double,bigint)")
                           .argumentType("real")
                           .build());

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
              return std::make_unique<SimpleAggregateAdapter<AverageAggregate<int16_t>>>(resultType);
            case TypeKind::INTEGER:
              return std::make_unique<SimpleAggregateAdapter<AverageAggregate<int32_t>>>(resultType);
            case TypeKind::BIGINT:
              return std::make_unique<SimpleAggregateAdapter<AverageAggregate<int64_t>>>(resultType);
            case TypeKind::REAL:
              return std::make_unique<SimpleAggregateAdapter<AverageAggregate<float>>>(resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<SimpleAggregateAdapter<AverageAggregate<double>>>(resultType);
            default:
              VELOX_FAIL("Unknown input type for {} aggregation {}", name, inputType->kindName());
          }
        } else {
          switch (resultType->kind()) {
            case TypeKind::REAL:
              return std::make_unique<SimpleAggregateAdapter<AverageAggregate<float>>>(resultType);
            case TypeKind::DOUBLE:
            case TypeKind::ROW:
              return std::make_unique<SimpleAggregateAdapter<AverageAggregate<double>>>(resultType);
            default:
              VELOX_FAIL("Unsupported result type for final aggregation: {}", resultType->kindName());
          }
        }
      },
      true /*registerCompanionFunctions*/,
      true /*overwrite*/);
}

} // namespace

const int kNumMyUdf = 8;

DEFINE_GET_NUM_UDF {
  return kNumMyUdf;
}

const char* myUdf1Arg1[] = {kInteger};
const char* myUdf1Arg2[] = {kBigInt};
const char* myUdf2Arg1[] = {kBigInt};
const char* myDateArg[] = {kDate, kInteger};
const char* myAvgArg1[] = {kInteger};
const char* myAvgArg2[] = {kBigInt};
const char* myAvgArg3[] = {kFloat};
const char* myAvgArg4[] = {kDouble};
const char* myAvgIntermediateType = "struct<a:double,b:bigint>";
DEFINE_GET_UDF_ENTRIES {
  int index = 0;
  udfEntries[index++] = {"myudf1", kInteger, 1, myUdf1Arg1};
  udfEntries[index++] = {"myudf1", kBigInt, 1, myUdf1Arg2};
  udfEntries[index++] = {"myudf2", kBigInt, 1, myUdf2Arg1};
  udfEntries[index++] = {"mydate", kDate, 2, myDateArg};
  udfEntries[index++] = {"myavg", kDouble, 1, myAvgArg1, myAvgIntermediateType};
  udfEntries[index++] = {"myavg", kDouble, 1, myAvgArg2, myAvgIntermediateType};
  udfEntries[index++] = {"myavg", kDouble, 1, myAvgArg3, myAvgIntermediateType};
  udfEntries[index++] = {"myavg", kDouble, 1, myAvgArg4, myAvgIntermediateType};
}

DEFINE_REGISTER_UDF {
  facebook::velox::exec::registerStatefulVectorFunction("myudf1", integerSignatures(), makeMyUdf1);
  facebook::velox::exec::registerVectorFunction(
      "myudf2", bigintSignatures(), std::make_unique<PlusConstantFunction<facebook::velox::TypeKind::BIGINT>>(5));
  facebook::velox::registerFunction<MyDateSimpleFunction, Date, Date, int32_t>({"mydate"});

  registerSimpleAverageAggregate("myavg");
}
