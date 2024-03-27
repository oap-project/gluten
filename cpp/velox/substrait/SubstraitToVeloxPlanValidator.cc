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

#include "SubstraitToVeloxPlanValidator.h"
#include <google/protobuf/wrappers.pb.h>
#include <re2/re2.h>
#include <string>
#include "TypeUtils.h"
#include "utils/Common.h"
#include "velox/core/ExpressionEvaluator.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/Expr.h"
#include "velox/expression/SignatureBinder.h"

namespace gluten {

namespace {
static const char* extractFileName(const char* file) {
  return strrchr(file, '/') ? strrchr(file, '/') + 1 : file;
}

#define LOG_VALIDATION_MSG_FROM_EXCEPTION(err)                                                                                        \
  logValidateMsg(fmt::format(                                                                                                         \
      "Validation failed due to exception caught at file:{} line:{} function:{}, thrown from file:{} line:{} function:{}, reason:{}", \
      extractFileName(__FILE__),                                                                                                      \
      __LINE__,                                                                                                                       \
      __FUNCTION__,                                                                                                                   \
      extractFileName(err.file()),                                                                                                    \
      err.line(),                                                                                                                     \
      err.function(),                                                                                                                 \
      err.message()))

#define LOG_VALIDATION_MSG(reason)                                     \
  logValidateMsg(fmt::format(                                          \
      "Validation failed at file:{}, line:{}, function:{}, reason:{}", \
      extractFileName(__FILE__),                                       \
      __LINE__,                                                        \
      __FUNCTION__,                                                    \
      reason))

static const std::unordered_set<std::string> kRegexFunctions = {
    "regexp_extract",
    "regexp_extract_all",
    "regexp_replace",
    "rlike"};

static const std::unordered_set<std::string> kBlackList = {
    "split_part",
    "factorial",
    "concat_ws",
    "from_json",
    "json_array_length",
    "repeat",
    "trunc",
    "sequence",
    "arrays_overlap",
    "approx_percentile",
    "get_array_struct_fields"};

} // namespace

bool SubstraitToVeloxPlanValidator::validateInputTypes(
    const ::substrait::extensions::AdvancedExtension& extension,
    std::vector<TypePtr>& types) {
  // The input type is wrapped in enhancement.
  if (!extension.has_enhancement()) {
    LOG_VALIDATION_MSG("Input type is not wrapped in enhancement.");
    return false;
  }
  const auto& enhancement = extension.enhancement();
  ::substrait::Type inputType;
  if (!enhancement.UnpackTo(&inputType)) {
    LOG_VALIDATION_MSG("Enhancement can't be unpacked to inputType.");
    return false;
  }
  if (!inputType.has_struct_()) {
    LOG_VALIDATION_MSG("Input type has no struct.");
    return false;
  }

  // Get the input types.
  const auto& sTypes = inputType.struct_().types();
  for (const auto& sType : sTypes) {
    try {
      types.emplace_back(SubstraitParser::parseType(sType));
    } catch (const VeloxException& err) {
      LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
      return false;
    }
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validateRound(
    const ::substrait::Expression::ScalarFunction& scalarFunction,
    const RowTypePtr& inputType) {
  const auto& arguments = scalarFunction.arguments();
  if (arguments.size() < 2) {
    return false;
  }

  if (!arguments[1].value().has_literal()) {
    LOG_VALIDATION_MSG("Round scale is expected.");
    return false;
  }

  // Velox has different result with Spark on negative scale.
  auto typeCase = arguments[1].value().literal().literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
      return (arguments[1].value().literal().i32() >= 0);
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
      return (arguments[1].value().literal().i64() >= 0);
    default:
      LOG_VALIDATION_MSG("Round scale validation is not supported for type case " + std::to_string(typeCase));
      return false;
  }
}

bool SubstraitToVeloxPlanValidator::validateExtractExpr(const std::vector<core::TypedExprPtr>& params) {
  if (params.size() != 2) {
    LOG_VALIDATION_MSG("Value expected in variant in ExtractExpr.");
    return false;
  }

  auto functionArg = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(params[0]);
  if (functionArg) {
    // Get the function argument.
    const auto& variant = functionArg->value();
    if (!variant.hasValue()) {
      LOG_VALIDATION_MSG("Value expected in variant in ExtractExpr.");
      return false;
    }

    return true;
  }
  LOG_VALIDATION_MSG("Constant is expected to be the first parameter in extract.");
  return false;
}

bool SubstraitToVeloxPlanValidator::validateRegexExpr(
    const std::string& name,
    const ::substrait::Expression::ScalarFunction& scalarFunction) {
  if (scalarFunction.arguments().size() < 2) {
    LOG_VALIDATION_MSG("Wrong number of arguments for " + name);
  }

  const auto& patternArg = scalarFunction.arguments()[1].value();
  if (!patternArg.has_literal() || !patternArg.literal().has_string()) {
    LOG_VALIDATION_MSG("Pattern is not string literal for " + name);
    return false;
  }

  const auto& pattern = patternArg.literal().string();
  std::string error;
  if (!validatePattern(pattern, error)) {
    LOG_VALIDATION_MSG(name + " due to " + error);
    return false;
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validateScalarFunction(
    const ::substrait::Expression::ScalarFunction& scalarFunction,
    const RowTypePtr& inputType) {
  std::vector<core::TypedExprPtr> params;
  params.reserve(scalarFunction.arguments().size());
  for (const auto& argument : scalarFunction.arguments()) {
    if (argument.has_value() && !validateExpression(argument.value(), inputType)) {
      return false;
    }
    params.emplace_back(exprConverter_->toVeloxExpr(argument.value(), inputType));
  }

  const auto& function =
      SubstraitParser::findFunctionSpec(planConverter_.getFunctionMap(), scalarFunction.function_reference());
  const auto& name = SubstraitParser::getNameBeforeDelimiter(function);
  std::vector<std::string> types = SubstraitParser::getSubFunctionTypes(function);

  if (name == "round") {
    return validateRound(scalarFunction, inputType);
  } else if (name == "extract") {
    return validateExtractExpr(params);
  } else if (name == "char_length") {
    VELOX_CHECK(types.size() == 1);
    if (types[0] == "vbin") {
      LOG_VALIDATION_MSG("Binary type is not supported in " + name);
      return false;
    }
  } else if (name == "map_from_arrays") {
    LOG_VALIDATION_MSG("map_from_arrays is not supported.");
    return false;
  } else if (name == "get_array_item") {
    LOG_VALIDATION_MSG("get_array_item is not supported.");
    return false;
  } else if (name == "concat") {
    for (const auto& type : types) {
      if (type.find("struct") != std::string::npos || type.find("map") != std::string::npos ||
          type.find("list") != std::string::npos) {
        LOG_VALIDATION_MSG(type + " is not supported in concat.");
        return false;
      }
    }
  } else if (name == "murmur3hash") {
    for (const auto& type : types) {
      if (type.find("struct") != std::string::npos || type.find("map") != std::string::npos ||
          type.find("list") != std::string::npos) {
        LOG_VALIDATION_MSG(type + " is not supported in murmur3hash.");
        return false;
      }
    }
  }

  // Validate regex functions.
  if (kRegexFunctions.find(name) != kRegexFunctions.end()) {
    return validateRegexExpr(name, scalarFunction);
  }

  if (kBlackList.find(name) != kBlackList.end()) {
    LOG_VALIDATION_MSG("Function is not supported: " + name);
    return false;
  }

  return true;
}

bool SubstraitToVeloxPlanValidator::validateLiteral(
    const ::substrait::Expression_Literal& literal,
    const RowTypePtr& inputType) {
  if (literal.has_list()) {
    for (auto child : literal.list().values()) {
      if (!validateLiteral(child, inputType)) {
        // the error msg has been set, so do not need to set it again.
        return false;
      }
    }
  } else if (literal.has_map()) {
    for (auto child : literal.map().key_values()) {
      if (!validateLiteral(child.key(), inputType) || !validateLiteral(child.value(), inputType)) {
        // the error msg has been set, so do not need to set it again.
        return false;
      }
    }
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validateCast(
    const ::substrait::Expression::Cast& castExpr,
    const RowTypePtr& inputType) {
  if (!validateExpression(castExpr.input(), inputType)) {
    return false;
  }

  const auto& toType = SubstraitParser::parseType(castExpr.type());
  if (toType->kind() == TypeKind::TIMESTAMP) {
    LOG_VALIDATION_MSG("Casting to TIMESTAMP is not supported.");
    return false;
  }

  core::TypedExprPtr input = exprConverter_->toVeloxExpr(castExpr.input(), inputType);

  // Casting from some types is not supported. See CastExpr::applyCast.
  if (input->type()->isDate()) {
    if (toType->kind() == TypeKind::TIMESTAMP) {
      LOG_VALIDATION_MSG("Casting from DATE to TIMESTAMP is not supported.");
      return false;
    }
    if (toType->kind() != TypeKind::VARCHAR) {
      LOG_VALIDATION_MSG("Casting from DATE to " + toType->toString() + " is not supported.");
      return false;
    }
  }
  switch (input->type()->kind()) {
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
    case TypeKind::VARBINARY:
      LOG_VALIDATION_MSG("Invalid input type in casting: ARRAY/MAP/ROW/VARBINARY.");
      return false;
    case TypeKind::TIMESTAMP: {
      LOG_VALIDATION_MSG("Casting from TIMESTAMP is not supported or has incorrect result.");
      return false;
    }
    default: {
    }
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validateIfThen(
    const ::substrait::Expression_IfThen& ifThen,
    const RowTypePtr& inputType) {
  for (const auto& subIfThen : ifThen.ifs()) {
    return validateExpression(subIfThen.if_(), inputType) && validateExpression(subIfThen.then(), inputType);
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validateSingularOrList(
    const ::substrait::Expression::SingularOrList& singularOrList,
    const RowTypePtr& inputType) {
  for (const auto& option : singularOrList.options()) {
    if (!option.has_literal()) {
      LOG_VALIDATION_MSG("Option is expected as Literal.");
      return false;
    }
    if (!validateLiteral(option.literal(), inputType)) {
      return false;
    }
  }

  return validateExpression(singularOrList.value(), inputType);
}

bool SubstraitToVeloxPlanValidator::validateExpression(
    const ::substrait::Expression& expression,
    const RowTypePtr& inputType) {
  auto typeCase = expression.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kScalarFunction:
      return validateScalarFunction(expression.scalar_function(), inputType);
    case ::substrait::Expression::RexTypeCase::kLiteral:
      return validateLiteral(expression.literal(), inputType);
    case ::substrait::Expression::RexTypeCase::kCast:
      return validateCast(expression.cast(), inputType);
    case ::substrait::Expression::RexTypeCase::kIfThen:
      return validateIfThen(expression.if_then(), inputType);
    case ::substrait::Expression::RexTypeCase::kSingularOrList:
      return validateSingularOrList(expression.singular_or_list(), inputType);
    default:
      return true;
  }
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::WriteRel& writeRel) {
  if (writeRel.has_input() && !validate(writeRel.input())) {
    LOG_VALIDATION_MSG("Validation failed for input type validation in WriteRel.");
    return false;
  }

  // Validate input data type.
  std::vector<TypePtr> types;
  if (writeRel.has_named_table()) {
    const auto& extension = writeRel.named_table().advanced_extension();
    if (!validateInputTypes(extension, types)) {
      LOG_VALIDATION_MSG("Validation failed for input type validation in WriteRel.");
      return false;
    }
  }

  // Validate partition key type.
  if (writeRel.has_table_schema()) {
    const auto& tableSchema = writeRel.table_schema();
    std::vector<bool> isMetadataColumns;
    std::vector<bool> isPartitionColumns;
    SubstraitParser::parsePartitionAndMetadataColumns(tableSchema, isPartitionColumns, isMetadataColumns);
    for (auto i = 0; i < types.size(); i++) {
      if (isPartitionColumns[i]) {
        switch (types[i]->kind()) {
          case TypeKind::BOOLEAN:
          case TypeKind::TINYINT:
          case TypeKind::SMALLINT:
          case TypeKind::INTEGER:
          case TypeKind::BIGINT:
          case TypeKind::VARCHAR:
          case TypeKind::VARBINARY:
            break;
          default:
            LOG_VALIDATION_MSG(
                "Validation failed for input type validation in WriteRel, not support partition column type: " +
                mapTypeKindToName(types[i]->kind()));
            return false;
        }
      }
    }
  }

  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::FetchRel& fetchRel) {
  RowTypePtr rowType = nullptr;
  // Get and validate the input types from extension.
  if (fetchRel.has_advanced_extension()) {
    const auto& extension = fetchRel.advanced_extension();
    std::vector<TypePtr> types;
    if (!validateInputTypes(extension, types)) {
      LOG_VALIDATION_MSG("Unsupported input types in ExpandRel.");
      return false;
    }

    int32_t inputPlanNodeId = 0;
    std::vector<std::string> names;
    names.reserve(types.size());
    for (auto colIdx = 0; colIdx < types.size(); colIdx++) {
      names.emplace_back(SubstraitParser::makeNodeName(inputPlanNodeId, colIdx));
    }
    rowType = std::make_shared<RowType>(std::move(names), std::move(types));
  }

  if (fetchRel.offset() < 0 || fetchRel.count() < 0) {
    LOG_VALIDATION_MSG("Offset and count should be valid in FetchRel.");
    return false;
  }

  // Check the input of fetchRel, if it's sortRel, we need to check whether the sorting key is duplicated.
  bool topNFlag = false;
  if (fetchRel.has_input()) {
    topNFlag = fetchRel.input().has_sort();
    if (topNFlag) {
      ::substrait::SortRel sortRel = fetchRel.input().sort();
      auto [sortingKeys, sortingOrders] = planConverter_.processSortField(sortRel.sorts(), rowType);
      folly::F14FastSet<std::string> sortingKeyNames;
      for (const auto& sortingKey : sortingKeys) {
        auto result = sortingKeyNames.insert(sortingKey->name());
        if (!result.second) {
          LOG_VALIDATION_MSG(
              "If the input of fetchRel is a SortRel, we will convert it to a TopNNode. In Velox, it is important to ensure unique sorting keys. However, duplicate keys were found in this case.");
          return false;
        }
      }
    }
  }

  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::GenerateRel& generateRel) {
  if (generateRel.has_input() && !validate(generateRel.input())) {
    LOG_VALIDATION_MSG("Input validation fails in GenerateRel.");
    return false;
  }

  // Get and validate the input types from extension.
  if (!generateRel.has_advanced_extension()) {
    LOG_VALIDATION_MSG("Input types are expected in GenerateRel.");
    return false;
  }
  const auto& extension = generateRel.advanced_extension();
  std::vector<TypePtr> types;
  if (!validateInputTypes(extension, types)) {
    LOG_VALIDATION_MSG("Validation failed for input types in GenerateRel.");
    return false;
  }

  int32_t inputPlanNodeId = 0;
  // Create the fake input names to be used in row type.
  std::vector<std::string> names;
  names.reserve(types.size());
  for (uint32_t colIdx = 0; colIdx < types.size(); colIdx++) {
    names.emplace_back(SubstraitParser::makeNodeName(inputPlanNodeId, colIdx));
  }
  auto rowType = std::make_shared<RowType>(std::move(names), std::move(types));

  if (generateRel.has_generator() && !validateExpression(generateRel.generator(), rowType)) {
    LOG_VALIDATION_MSG("Input validation fails in GenerateRel.");
    return false;
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::ExpandRel& expandRel) {
  if (expandRel.has_input() && !validate(expandRel.input())) {
    LOG_VALIDATION_MSG("Input validation fails in ExpandRel.");
    return false;
  }
  RowTypePtr rowType = nullptr;
  // Get and validate the input types from extension.
  if (expandRel.has_advanced_extension()) {
    const auto& extension = expandRel.advanced_extension();
    std::vector<TypePtr> types;
    if (!validateInputTypes(extension, types)) {
      LOG_VALIDATION_MSG("Unsupported input types in ExpandRel.");
      return false;
    }

    int32_t inputPlanNodeId = 0;
    std::vector<std::string> names;
    names.reserve(types.size());
    for (auto colIdx = 0; colIdx < types.size(); colIdx++) {
      names.emplace_back(SubstraitParser::makeNodeName(inputPlanNodeId, colIdx));
    }
    rowType = std::make_shared<RowType>(std::move(names), std::move(types));
  }

  int32_t projectSize = 0;
  // Validate fields.
  for (const auto& fields : expandRel.fields()) {
    std::vector<core::TypedExprPtr> expressions;
    if (fields.has_switching_field()) {
      auto projectExprs = fields.switching_field().duplicates();
      expressions.reserve(projectExprs.size());
      if (projectSize == 0) {
        projectSize = projectExprs.size();
      } else if (projectSize != projectExprs.size()) {
        LOG_VALIDATION_MSG("SwitchingField expressions size should be constant in ExpandRel.");
        return false;
      }

      try {
        for (const auto& projectExpr : projectExprs) {
          const auto& typeCase = projectExpr.rex_type_case();
          switch (typeCase) {
            case ::substrait::Expression::RexTypeCase::kSelection:
            case ::substrait::Expression::RexTypeCase::kLiteral:
              break;
            default:
              LOG_VALIDATION_MSG("Only field or literal is supported in project of ExpandRel.");
              return false;
          }
          if (rowType) {
            expressions.emplace_back(exprConverter_->toVeloxExpr(projectExpr, rowType));
          }
        }

        if (rowType) {
          // Try to compile the expressions. If there is any unregistered
          // function or mismatched type, exception will be thrown.
          exec::ExprSet exprSet(std::move(expressions), execCtx_);
        }

      } catch (const VeloxException& err) {
        LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
        return false;
      }
    } else {
      LOG_VALIDATION_MSG("Only SwitchingField is supported in ExpandRel.");
      return false;
    }
  }

  return true;
}

bool validateBoundType(::substrait::Expression_WindowFunction_Bound boundType) {
  switch (boundType.kind_case()) {
    case ::substrait::Expression_WindowFunction_Bound::kUnboundedFollowing:
    case ::substrait::Expression_WindowFunction_Bound::kUnboundedPreceding:
    case ::substrait::Expression_WindowFunction_Bound::kCurrentRow:
    case ::substrait::Expression_WindowFunction_Bound::kFollowing:
    case ::substrait::Expression_WindowFunction_Bound::kPreceding:
      break;
    default:
      return false;
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::WindowRel& windowRel) {
  if (windowRel.has_input() && !validate(windowRel.input())) {
    LOG_VALIDATION_MSG("WindowRel input fails to validate.");
    return false;
  }

  // Get and validate the input types from extension.
  if (!windowRel.has_advanced_extension()) {
    LOG_VALIDATION_MSG("Input types are expected in WindowRel.");
    return false;
  }
  const auto& extension = windowRel.advanced_extension();
  std::vector<TypePtr> types;
  if (!validateInputTypes(extension, types)) {
    LOG_VALIDATION_MSG("Validation failed for input types in WindowRel.");
    return false;
  }

  int32_t inputPlanNodeId = 0;
  std::vector<std::string> names;
  names.reserve(types.size());
  for (auto colIdx = 0; colIdx < types.size(); colIdx++) {
    names.emplace_back(SubstraitParser::makeNodeName(inputPlanNodeId, colIdx));
  }
  auto rowType = std::make_shared<RowType>(std::move(names), std::move(types));

  // Validate WindowFunction
  std::vector<std::string> funcSpecs;
  funcSpecs.reserve(windowRel.measures().size());
  for (const auto& smea : windowRel.measures()) {
    try {
      const auto& windowFunction = smea.measure();
      funcSpecs.emplace_back(planConverter_.findFuncSpec(windowFunction.function_reference()));
      SubstraitParser::parseType(windowFunction.output_type());
      for (const auto& arg : windowFunction.arguments()) {
        auto typeCase = arg.value().rex_type_case();
        switch (typeCase) {
          case ::substrait::Expression::RexTypeCase::kSelection:
          case ::substrait::Expression::RexTypeCase::kLiteral:
            break;
          default:
            LOG_VALIDATION_MSG("Only field or constant is supported in window functions.");
            return false;
        }
      }
      // Validate BoundType and Frame Type
      switch (windowFunction.window_type()) {
        case ::substrait::WindowType::ROWS:
        case ::substrait::WindowType::RANGE:
          break;
        default:
          LOG_VALIDATION_MSG(
              "the window type only support ROWS and RANGE, and the input type is " +
              std::to_string(windowFunction.window_type()));
          return false;
      }

      bool boundTypeSupported =
          validateBoundType(windowFunction.upper_bound()) && validateBoundType(windowFunction.lower_bound());
      if (!boundTypeSupported) {
        LOG_VALIDATION_MSG(
            "Found unsupported Bound Type: upper " + std::to_string(windowFunction.upper_bound().kind_case()) +
            ", lower " + std::to_string(windowFunction.lower_bound().kind_case()));
        return false;
      }
    } catch (const VeloxException& err) {
      LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
      return false;
    }
  }

  // Validate supported aggregate functions.
  static const std::unordered_set<std::string> unsupportedFuncs = {"collect_list", "collect_set"};
  for (const auto& funcSpec : funcSpecs) {
    auto funcName = SubstraitParser::getNameBeforeDelimiter(funcSpec);
    if (unsupportedFuncs.find(funcName) != unsupportedFuncs.end()) {
      LOG_VALIDATION_MSG(funcName + " was not supported in WindowRel.");
      return false;
    }
  }

  // Validate groupby expression
  const auto& groupByExprs = windowRel.partition_expressions();
  std::vector<core::TypedExprPtr> expressions;
  expressions.reserve(groupByExprs.size());
  try {
    for (const auto& expr : groupByExprs) {
      auto expression = exprConverter_->toVeloxExpr(expr, rowType);
      auto exprField = dynamic_cast<const core::FieldAccessTypedExpr*>(expression.get());
      if (exprField == nullptr) {
        LOG_VALIDATION_MSG("Only field is supported for partition key in Window Operator!");
        return false;
      } else {
        expressions.emplace_back(expression);
      }
    }
    // Try to compile the expressions. If there is any unregistred funciton or
    // mismatched type, exception will be thrown.
    exec::ExprSet exprSet(std::move(expressions), execCtx_);
  } catch (const VeloxException& err) {
    LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
    return false;
  }

  // Validate Sort expression
  const auto& sorts = windowRel.sorts();
  for (const auto& sort : sorts) {
    switch (sort.direction()) {
      case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
      case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
      case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
      case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
        break;
      default:
        LOG_VALIDATION_MSG("in windowRel, unsupported Sort direction " + std::to_string(sort.direction()));
        return false;
    }

    if (sort.has_expr()) {
      try {
        auto expression = exprConverter_->toVeloxExpr(sort.expr(), rowType);
        auto exprField = dynamic_cast<const core::FieldAccessTypedExpr*>(expression.get());
        if (!exprField) {
          LOG_VALIDATION_MSG("in windowRel, the sorting key in Sort Operator only support field.");
          return false;
        }
        exec::ExprSet exprSet({std::move(expression)}, execCtx_);
      } catch (const VeloxException& err) {
        LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
        return false;
      }
    }
  }

  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::SortRel& sortRel) {
  if (sortRel.has_input() && !validate(sortRel.input())) {
    return false;
  }

  // Get and validate the input types from extension.
  if (!sortRel.has_advanced_extension()) {
    LOG_VALIDATION_MSG("Input types are expected in SortRel.");
    return false;
  }

  const auto& extension = sortRel.advanced_extension();
  std::vector<TypePtr> types;
  if (!validateInputTypes(extension, types)) {
    LOG_VALIDATION_MSG("Validation failed for input types in SortRel.");
    return false;
  }

  int32_t inputPlanNodeId = 0;
  std::vector<std::string> names;
  names.reserve(types.size());
  for (auto colIdx = 0; colIdx < types.size(); colIdx++) {
    names.emplace_back(SubstraitParser::makeNodeName(inputPlanNodeId, colIdx));
  }
  auto rowType = std::make_shared<RowType>(std::move(names), std::move(types));

  const auto& sorts = sortRel.sorts();
  for (const auto& sort : sorts) {
    switch (sort.direction()) {
      case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
      case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
      case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
      case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
        break;
      default:
        LOG_VALIDATION_MSG("unsupported Sort direction " + std::to_string(sort.direction()));
        return false;
    }

    if (sort.has_expr()) {
      try {
        auto expression = exprConverter_->toVeloxExpr(sort.expr(), rowType);
        auto exprField = dynamic_cast<const core::FieldAccessTypedExpr*>(expression.get());
        if (!exprField) {
          LOG_VALIDATION_MSG("in SortRel, the sorting key in Sort Operator only support field.");
          return false;
        }
        exec::ExprSet exprSet({std::move(expression)}, execCtx_);
      } catch (const VeloxException& err) {
        LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
        return false;
      }
    }
  }

  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::ProjectRel& projectRel) {
  if (projectRel.has_input() && !validate(projectRel.input())) {
    LOG_VALIDATION_MSG("ProjectRel input");
    return false;
  }

  // Get and validate the input types from extension.
  if (!projectRel.has_advanced_extension()) {
    LOG_VALIDATION_MSG("Input types are expected in ProjectRel.");
    return false;
  }
  const auto& extension = projectRel.advanced_extension();
  std::vector<TypePtr> types;
  if (!validateInputTypes(extension, types)) {
    LOG_VALIDATION_MSG("Validation failed for input types in ProjectRel.");
    return false;
  }

  int32_t inputPlanNodeId = 0;
  // Create the fake input names to be used in row type.
  std::vector<std::string> names;
  names.reserve(types.size());
  for (uint32_t colIdx = 0; colIdx < types.size(); colIdx++) {
    names.emplace_back(SubstraitParser::makeNodeName(inputPlanNodeId, colIdx));
  }
  auto rowType = std::make_shared<RowType>(std::move(names), std::move(types));

  // Validate the project expressions.
  const auto& projectExprs = projectRel.expressions();
  std::vector<core::TypedExprPtr> expressions;
  expressions.reserve(projectExprs.size());
  try {
    for (const auto& expr : projectExprs) {
      if (!validateExpression(expr, rowType)) {
        return false;
      }
      expressions.emplace_back(exprConverter_->toVeloxExpr(expr, rowType));
    }
    // Try to compile the expressions. If there is any unregistered function or
    // mismatched type, exception will be thrown.
    exec::ExprSet exprSet(std::move(expressions), execCtx_);
  } catch (const VeloxException& err) {
    LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
    return false;
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::FilterRel& filterRel) {
  if (filterRel.has_input() && !validate(filterRel.input())) {
    LOG_VALIDATION_MSG("input of FilterRel validation fails");
    return false;
  }

  // Get and validate the input types from extension.
  if (!filterRel.has_advanced_extension()) {
    LOG_VALIDATION_MSG("Input types are expected in FilterRel.");
    return false;
  }
  const auto& extension = filterRel.advanced_extension();
  std::vector<TypePtr> types;
  if (!validateInputTypes(extension, types)) {
    LOG_VALIDATION_MSG("Validation failed for input types in FilterRel.");
    return false;
  }

  int32_t inputPlanNodeId = 0;
  // Create the fake input names to be used in row type.
  std::vector<std::string> names;
  names.reserve(types.size());
  for (uint32_t colIdx = 0; colIdx < types.size(); colIdx++) {
    names.emplace_back(SubstraitParser::makeNodeName(inputPlanNodeId, colIdx));
  }
  auto rowType = std::make_shared<RowType>(std::move(names), std::move(types));

  std::vector<core::TypedExprPtr> expressions;
  try {
    if (!validateExpression(filterRel.condition(), rowType)) {
      return false;
    }
    expressions.emplace_back(exprConverter_->toVeloxExpr(filterRel.condition(), rowType));
    // Try to compile the expressions. If there is any unregistered function
    // or mismatched type, exception will be thrown.
    exec::ExprSet exprSet(std::move(expressions), execCtx_);
  } catch (const VeloxException& err) {
    LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
    return false;
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::JoinRel& joinRel) {
  if (joinRel.has_left() && !validate(joinRel.left())) {
    LOG_VALIDATION_MSG("Validation fails for join left input.");
    return false;
  }

  if (joinRel.has_right() && !validate(joinRel.right())) {
    LOG_VALIDATION_MSG("Validation fails for join right input.");
    return false;
  }

  if (joinRel.has_advanced_extension() &&
      SubstraitParser::configSetInOptimization(joinRel.advanced_extension(), "isSMJ=")) {
    switch (joinRel.type()) {
      case ::substrait::JoinRel_JoinType_JOIN_TYPE_INNER:
      case ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT:
        break;
      default:
        LOG_VALIDATION_MSG("Sort merge join only support inner and left join.");
        return false;
    }
  }
  switch (joinRel.type()) {
    case ::substrait::JoinRel_JoinType_JOIN_TYPE_INNER:
    case ::substrait::JoinRel_JoinType_JOIN_TYPE_OUTER:
    case ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT:
    case ::substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT:
    case ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
    case ::substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
    case ::substrait::JoinRel_JoinType_JOIN_TYPE_ANTI:
      break;
    default:
      LOG_VALIDATION_MSG("Sort merge join only support inner and left join.");
      return false;
  }

  // Validate input types.
  if (!joinRel.has_advanced_extension()) {
    LOG_VALIDATION_MSG("Input types are expected in JoinRel.");
    return false;
  }

  const auto& extension = joinRel.advanced_extension();
  std::vector<TypePtr> types;
  if (!validateInputTypes(extension, types)) {
    LOG_VALIDATION_MSG("Validation failed for input types in JoinRel.");
    return false;
  }

  int32_t inputPlanNodeId = 0;
  std::vector<std::string> names;
  names.reserve(types.size());
  for (auto colIdx = 0; colIdx < types.size(); colIdx++) {
    names.emplace_back(SubstraitParser::makeNodeName(inputPlanNodeId, colIdx));
  }
  auto rowType = std::make_shared<RowType>(std::move(names), std::move(types));

  if (joinRel.has_expression()) {
    std::vector<const ::substrait::Expression::FieldReference*> leftExprs, rightExprs;
    try {
      planConverter_.extractJoinKeys(joinRel.expression(), leftExprs, rightExprs);
    } catch (const VeloxException& err) {
      LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
      return false;
    }
  }

  if (joinRel.has_post_join_filter()) {
    try {
      auto expression = exprConverter_->toVeloxExpr(joinRel.post_join_filter(), rowType);
      exec::ExprSet exprSet({std::move(expression)}, execCtx_);
    } catch (const VeloxException& err) {
      LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
      return false;
    }
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::CrossRel& crossRel) {
  if (crossRel.has_left() && !validate(crossRel.left())) {
    logValidateMsg("Native validation failed due to: validation fails for cross join left input. ");
    return false;
  }

  if (crossRel.has_right() && !validate(crossRel.right())) {
    logValidateMsg("Native validation failed due to: validation fails for cross join right input. ");
    return false;
  }

  // Validate input types.
  if (!crossRel.has_advanced_extension()) {
    logValidateMsg("Native validation failed due to: Input types are expected in CrossRel.");
    return false;
  }

  switch (crossRel.type()) {
    case ::substrait::CrossRel_JoinType_JOIN_TYPE_INNER:
    case ::substrait::CrossRel_JoinType_JOIN_TYPE_LEFT:
      break;
    default:
      LOG_VALIDATION_MSG("Unsupported Join type in CrossRel");
      return false;
  }

  const auto& extension = crossRel.advanced_extension();
  std::vector<TypePtr> types;
  if (!validateInputTypes(extension, types)) {
    logValidateMsg("Native validation failed due to: Validation failed for input types in CrossRel");
    return false;
  }

  int32_t inputPlanNodeId = 0;
  std::vector<std::string> names;
  names.reserve(types.size());
  for (auto colIdx = 0; colIdx < types.size(); colIdx++) {
    names.emplace_back(SubstraitParser::makeNodeName(inputPlanNodeId, colIdx));
  }
  auto rowType = std::make_shared<RowType>(std::move(names), std::move(types));

  if (crossRel.has_expression()) {
    try {
      auto expression = exprConverter_->toVeloxExpr(crossRel.expression(), rowType);
      exec::ExprSet exprSet({std::move(expression)}, execCtx_);
    } catch (const VeloxException& err) {
      logValidateMsg("Native validation failed due to: crossRel expression validation fails, " + err.message());
      return false;
    }
  }

  return true;
}

bool SubstraitToVeloxPlanValidator::validateAggRelFunctionType(const ::substrait::AggregateRel& aggRel) {
  if (aggRel.measures_size() == 0) {
    return true;
  }

  for (const auto& smea : aggRel.measures()) {
    const auto& aggFunction = smea.measure();
    const auto& funcStep = planConverter_.toAggregationFunctionStep(aggFunction);
    auto funcSpec = planConverter_.findFuncSpec(aggFunction.function_reference());
    std::vector<TypePtr> types;
    bool isDecimal = false;
    try {
      types = SubstraitParser::sigToTypes(funcSpec);
      for (const auto& type : types) {
        if (!isDecimal && type->isDecimal()) {
          isDecimal = true;
        }
      }
    } catch (const VeloxException& err) {
      LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
      return false;
    }
    auto baseFuncName =
        SubstraitParser::mapToVeloxFunction(SubstraitParser::getNameBeforeDelimiter(funcSpec), isDecimal);
    auto funcName = planConverter_.toAggregationFunctionName(baseFuncName, funcStep);
    auto signaturesOpt = exec::getAggregateFunctionSignatures(funcName);
    if (!signaturesOpt) {
      LOG_VALIDATION_MSG("can not find function signature for " + funcName + " in AggregateRel.");
      return false;
    }

    bool resolved = false;
    for (const auto& signature : signaturesOpt.value()) {
      exec::SignatureBinder binder(*signature, types);
      if (binder.tryBind()) {
        auto resolveType = binder.tryResolveType(
            exec::isPartialOutput(funcStep) ? signature->intermediateType() : signature->returnType());
        if (resolveType == nullptr) {
          LOG_VALIDATION_MSG("Validation failed for function " + funcName + " resolve type in AggregateRel.");
          return false;
        }
        resolved = true;
        break;
      }
    }
    if (!resolved) {
      LOG_VALIDATION_MSG("Validation failed for function " + funcName + " bind signatures in AggregateRel.");
      return false;
    }
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::AggregateRel& aggRel) {
  if (aggRel.has_input() && !validate(aggRel.input())) {
    LOG_VALIDATION_MSG("Input validation fails in AggregateRel.");
    return false;
  }

  // Validate input types.
  if (aggRel.has_advanced_extension()) {
    std::vector<TypePtr> types;
    const auto& extension = aggRel.advanced_extension();
    // Aggregate always has advanced extension for streaming aggregate optimization,
    // but only some of them have enhancement for validation.
    if (extension.has_enhancement() && !validateInputTypes(extension, types)) {
      LOG_VALIDATION_MSG("Validation failed for input types in AggregateRel.");
      return false;
    }
  }

  // Validate groupings.
  for (const auto& grouping : aggRel.groupings()) {
    for (const auto& groupingExpr : grouping.grouping_expressions()) {
      const auto& typeCase = groupingExpr.rex_type_case();
      switch (typeCase) {
        case ::substrait::Expression::RexTypeCase::kSelection:
          break;
        default:
          LOG_VALIDATION_MSG("Only field is supported in groupings.");
          return false;
      }
    }
  }

  // Validate aggregate functions.
  std::vector<std::string> funcSpecs;
  funcSpecs.reserve(aggRel.measures().size());
  for (const auto& smea : aggRel.measures()) {
    try {
      // Validate the filter expression
      if (smea.has_filter()) {
        ::substrait::Expression aggRelMask = smea.filter();
        if (aggRelMask.ByteSizeLong() > 0) {
          auto typeCase = aggRelMask.rex_type_case();
          switch (typeCase) {
            case ::substrait::Expression::RexTypeCase::kSelection:
              break;
            default:
              LOG_VALIDATION_MSG("Only field is supported in aggregate filter expression.");
              return false;
          }
        }
      }

      const auto& aggFunction = smea.measure();
      const auto& functionSpec = planConverter_.findFuncSpec(aggFunction.function_reference());
      funcSpecs.emplace_back(functionSpec);
      SubstraitParser::parseType(aggFunction.output_type());
      // Validate the size of arguments.
      if (SubstraitParser::getNameBeforeDelimiter(functionSpec) == "count" && aggFunction.arguments().size() > 1) {
        LOG_VALIDATION_MSG("Count should have only one argument.");
        // Count accepts only one argument.
        return false;
      }

      for (const auto& arg : aggFunction.arguments()) {
        auto typeCase = arg.value().rex_type_case();
        switch (typeCase) {
          case ::substrait::Expression::RexTypeCase::kSelection:
          case ::substrait::Expression::RexTypeCase::kLiteral:
            break;
          default:
            LOG_VALIDATION_MSG("Only field is supported in aggregate functions.");
            return false;
        }
      }
    } catch (const VeloxException& err) {
      LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
      return false;
    }
  }

  // The supported aggregation functions. TODO: Remove this set when Presto aggregate functions in Velox are not
  // needed to be registered.
  static const std::unordered_set<std::string> supportedAggFuncs = {
      "sum",
      "collect_set",
      "collect_list",
      "count",
      "avg",
      "min",
      "max",
      "min_by",
      "max_by",
      "stddev_samp",
      "stddev_pop",
      "bloom_filter_agg",
      "var_samp",
      "var_pop",
      "bit_and",
      "bit_or",
      "bit_xor",
      "first",
      "first_ignore_null",
      "last",
      "last_ignore_null",
      "corr",
      "covar_pop",
      "covar_samp",
      "approx_distinct",
      "skewness"};

  static const std::string kUdafPrefix = "gluten_udaf_";

  for (const auto& funcSpec : funcSpecs) {
    auto funcName = SubstraitParser::getNameBeforeDelimiter(funcSpec);
    if (supportedAggFuncs.find(funcName) == supportedAggFuncs.end() && funcName.find(kUdafPrefix) != 0) {
      LOG_VALIDATION_MSG(funcName + " was not supported in AggregateRel.");
      return false;
    }
  }

  if (!validateAggRelFunctionType(aggRel)) {
    return false;
  }

  // Validate both groupby and aggregates input are empty, which is corner case.
  if (aggRel.measures_size() == 0) {
    bool hasExpr = false;
    for (const auto& grouping : aggRel.groupings()) {
      if (grouping.grouping_expressions().size() > 0) {
        hasExpr = true;
        break;
      }
    }

    if (!hasExpr) {
      LOG_VALIDATION_MSG("Aggregation must specify either grouping keys or aggregates.");
      return false;
    }
  }
  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::ReadRel& readRel) {
  try {
    planConverter_.toVeloxPlan(readRel);
  } catch (const VeloxException& err) {
    LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
    return false;
  }

  // Validate filter in ReadRel.
  if (readRel.has_filter()) {
    std::vector<TypePtr> veloxTypeList;
    if (readRel.has_base_schema()) {
      const auto& baseSchema = readRel.base_schema();
      veloxTypeList = SubstraitParser::parseNamedStruct(baseSchema);
    }

    int32_t inputPlanNodeId = 0;
    std::vector<std::string> names;
    names.reserve(veloxTypeList.size());
    for (auto colIdx = 0; colIdx < veloxTypeList.size(); colIdx++) {
      names.emplace_back(SubstraitParser::makeNodeName(inputPlanNodeId, colIdx));
    }

    auto rowType = std::make_shared<RowType>(std::move(names), std::move(veloxTypeList));
    std::vector<core::TypedExprPtr> expressions;
    try {
      if (!validateExpression(readRel.filter(), rowType)) {
        return false;
      }
      expressions.emplace_back(exprConverter_->toVeloxExpr(readRel.filter(), rowType));
      // Try to compile the expressions. If there is any unregistered function
      // or mismatched type, exception will be thrown.
      exec::ExprSet exprSet(std::move(expressions), execCtx_);
    } catch (const VeloxException& err) {
      LOG_VALIDATION_MSG_FROM_EXCEPTION(err);
      return false;
    }
  }

  return true;
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::Rel& rel) {
  if (rel.has_aggregate()) {
    return validate(rel.aggregate());
  } else if (rel.has_project()) {
    return validate(rel.project());
  } else if (rel.has_filter()) {
    return validate(rel.filter());
  } else if (rel.has_join()) {
    return validate(rel.join());
  } else if (rel.has_cross()) {
    return validate(rel.cross());
  } else if (rel.has_read()) {
    return validate(rel.read());
  } else if (rel.has_sort()) {
    return validate(rel.sort());
  } else if (rel.has_expand()) {
    return validate(rel.expand());
  } else if (rel.has_generate()) {
    return validate(rel.generate());
  } else if (rel.has_fetch()) {
    return validate(rel.fetch());
  } else if (rel.has_window()) {
    return validate(rel.window());
  } else if (rel.has_write()) {
    return validate(rel.write());
  } else {
    return false;
  }
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::RelRoot& relRoot) {
  if (relRoot.has_input()) {
    return validate(relRoot.input());
  } else {
    return false;
  }
}

bool SubstraitToVeloxPlanValidator::validate(const ::substrait::Plan& plan) {
  // Create plan converter and expression converter to help the validation.
  planConverter_.constructFunctionMap(plan);
  exprConverter_ = planConverter_.getExprConverter();

  for (const auto& rel : plan.relations()) {
    if (rel.has_root()) {
      return validate(rel.root());
    } else if (rel.has_rel()) {
      return validate(rel.rel());
    }
  }

  return false;
}

} // namespace gluten
