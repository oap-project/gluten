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

#include <cmath>
#include <string>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Parser/AggregateFunctionParser.h>
#include <Parser/aggregate_function_parser/ApproxPercentileParser.h>
#include <Poco/StringTokenizer.h>
#include <Common/CHUtil.h>
#include "substrait/algebra.pb.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

void ApproxPercentileParser::assertArgumentsSize(substrait::AggregationPhase phase, size_t size, size_t expect) const
{
    if (size != expect)
        throw Exception(
            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} in phase {} requires exactly {} arguments but got {} arguments",
            getName(),
            magic_enum::enum_name(phase),
            expect,
            size);
}

const substrait::Expression::Literal &
ApproxPercentileParser::assertAndGetLiteral(substrait::AggregationPhase phase, const substrait::Expression & expr) const
{
    if (!expr.has_literal())
        throw Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "The argument of function {} in phase {} must be literal, but is {}",
            getName(),
            magic_enum::enum_name(phase),
            expr.DebugString());
    return expr.literal();
}

String ApproxPercentileParser::getCHFunctionName(const CommonFunctionInfo & func_info) const
{
    const auto & output_type = func_info.output_type;
    return output_type.has_list() ? "quantilesGK" : "quantileGK";
}

String ApproxPercentileParser::getCHFunctionName(DB::DataTypes & types) const
{
    /// Always invoked during second stage
    assertArgumentsSize(substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT, types.size(), 2);

    auto type = removeNullable(types[1]);
    types.resize(1);
    return isArray(type) ? "quantilesGK" : "quantileGK";
}

DB::Array ApproxPercentileParser::parseFunctionParameters(
    const CommonFunctionInfo & func_info, DB::ActionsDAG::NodeRawConstPtrs & arg_nodes) const
{
    if (func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
        || func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT || func_info.phase == substrait::AGGREGATION_PHASE_UNSPECIFIED)
    {
        Array params;
        const auto & arguments = func_info.arguments;
        assertArgumentsSize(func_info.phase, arguments.size(), 3);

        const auto & accuracy_expr = arguments[2].value();
        const auto & accuracy_literal =  assertAndGetLiteral(func_info.phase, accuracy_expr);
        auto [type1, field1] = parseLiteral(accuracy_literal);
        params.emplace_back(std::move(field1));

        const auto & percentage_expr = arguments[1].value();
        const auto & percentage_literal = assertAndGetLiteral(func_info.phase, percentage_expr);
        auto [type2, field2] = parseLiteral(percentage_literal);
        if (isArray(type2))
        {
            /// Multiple percentages for quantilesGK
            const Array & percentags = field2.get<Array>();
            for (const auto & percentage : percentags)
                params.emplace_back(percentage);
        }
        else
        {
            /// Single percentage for quantileGK
            params.emplace_back(std::move(field2));
        }

        /// Delete percentage and accuracy argument for clickhouse compatiability
        arg_nodes.resize(1);
        return params;
    }
    else
    {
        assertArgumentsSize(func_info.phase, arg_nodes.size(), 1);
        const auto & result_type = arg_nodes[0]->result_type;
        const auto * aggregate_function_type = DB::checkAndGetDataType<DB::DataTypeAggregateFunction>(result_type.get());
        if (!aggregate_function_type)
            throw Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "The first argument type of function {} in phase {} must be AggregateFunction, but is {}",
                getName(),
                magic_enum::enum_name(func_info.phase),
                result_type->getName());

        return aggregate_function_type->getParameters();
    }
}

DB::Array ApproxPercentileParser::getDefaultFunctionParameters() const
{
    return {10000, 1};
}


static const AggregateFunctionParserRegister<ApproxPercentileParser> register_approx_percentile;
}
