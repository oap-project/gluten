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

DB::Array ApproxPercentileParser::parseFunctionParameters(
    const CommonFunctionInfo & func_info, DB::ActionsDAG::NodeRawConstPtrs & arg_nodes) const
{
    if (func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE || func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT)
    {
        Array params;
        const auto & arguments = func_info.arguments;
        if (arguments.size() != 3)
            throw Exception(
                DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} in phase {} requires exactly 3 arguments",
                getName(),
                magic_enum::enum_name(func_info.phase));

        const auto & accuracy_expr = arguments[2].value();
        if (accuracy_expr.has_literal())
        {
            auto [type, field] = parseLiteral(accuracy_expr.literal());
            params.emplace_back(std::move(field));
        }
        else
            throw Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "The third argument of function {} in phase {} must be literal, but is {}",
                getName(),
                magic_enum::enum_name(func_info.phase),
                accuracy_expr.DebugString());


        const auto & arg_percentage = arguments[1].value();
        if (arg_percentage.has_literal())
        {
            auto [type, field] = parseLiteral(arg_percentage.literal());
            if (isArray(type))
            {
                /// Multiple percentages
                const Array & percentags = field.get<Array>();
                for (const auto & percentage : percentags)
                    params.emplace_back(percentage);
            }
            else
                params.emplace_back(std::move(field));
        }
        else
            throw Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "The second argument of function {} in phase {} must be literal, but is {}",
                getName(),
                magic_enum::enum_name(func_info.phase),
                arg_percentage.DebugString());

        /// Delete percentage and accuracy argument for clickhouse compatiability
        arg_nodes.resize(1);
        return params;
    }
    else
    {
        if (arg_nodes.size() != 1)
            throw Exception(
                DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} in phase {} requires exactly 1 arguments",
                getName(),
                magic_enum::enum_name(func_info.phase));

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
