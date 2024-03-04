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
#include <Parser/AggregateFunctionParser.h>


/*
spark: approx_percentile(col, percentage [, accuracy])
1. When percentage is an array literal, spark returns an array of percentiles, corresponding to CH: quantilesGK(accuracy, percentage[0], ...)(col)
1. Otherwise spark return a single percentile, corresponding to CH: quantileGK(accuracy, percentage)(col)
*/

namespace local_engine
{
class ApproxPercentileParser : public AggregateFunctionParser
{
public:
    explicit ApproxPercentileParser(SerializedPlanParser * plan_parser_) : AggregateFunctionParser(plan_parser_) { }
    ~ApproxPercentileParser() override = default;
    String getName() const override { return name; }
    static constexpr auto name = "approx_percentile";
    String getCHFunctionName(const CommonFunctionInfo & func_info) const override;
    String getCHFunctionName(DB::DataTypes & types) const override;

    DB::Array
    parseFunctionParameters(const CommonFunctionInfo & /*func_info*/, DB::ActionsDAG::NodeRawConstPtrs & arg_nodes) const override;

    DB::Array getDefaultFunctionParameters() const override;

private:
    void assertArgumentsSize(substrait::AggregationPhase phase, size_t size, size_t expect) const;
    const substrait::Expression::Literal & assertAndGetLiteral(substrait::AggregationPhase phase, const substrait::Expression & expr) const;
};
}
