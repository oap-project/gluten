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

// In CH, BloomFilterAgg has parameters, e.g. groupBloomFilterState(param1, param2, param3)(input1)
// so cannot use common aggregate parser.
namespace local_engine
{
class AggregateFunctionParserBloomFilterAgg : public AggregateFunctionParser
{
public:
    explicit AggregateFunctionParserBloomFilterAgg(SerializedPlanParser * plan_parser_) : AggregateFunctionParser(plan_parser_) { }
    ~AggregateFunctionParserBloomFilterAgg() override = default;
    String getName() const override { return name; }
    static constexpr auto name = "bloom_filter_agg";
    String getCHFunctionName(const CommonFunctionInfo &) const override { return "groupBloomFilterState"; }
    String getCHFunctionName(DB::DataTypes &) const override { return "groupBloomFilterState"; }

    DB::Array
    parseFunctionParameters(const CommonFunctionInfo & /*func_info*/, DB::ActionsDAG::NodeRawConstPtrs & arg_nodes) const override;
};
}
