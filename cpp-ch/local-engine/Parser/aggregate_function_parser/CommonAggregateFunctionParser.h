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
#include <Parser/AggregateFunctionParser.h>


namespace local_engine
{

#define REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(cls_name, substait_name, ch_name) \
    class AggregateFunctionParser##cls_name : public AggregateFunctionParser \
    { \
    public: \
        AggregateFunctionParser##cls_name(SerializedPlanParser * plan_parser_) : AggregateFunctionParser(plan_parser_) \
        { \
        } \
        ~AggregateFunctionParser##cls_name() override = default; \
        String getName() const override { return  #substait_name; } \
        static constexpr auto name = #substait_name; \
        String getCHFunctionName(const CommonFunctionInfo &) const override { return #ch_name; } \
        String getCHFunctionName(DB::DataTypes &) const override { return #ch_name; } \
    }; \
    static const AggregateFunctionParserRegister<AggregateFunctionParser##cls_name> register_##cls_name = AggregateFunctionParserRegister<AggregateFunctionParser##cls_name>();

}
