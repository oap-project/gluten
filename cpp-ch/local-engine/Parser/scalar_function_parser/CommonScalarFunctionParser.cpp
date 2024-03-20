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
#include <Parser/FunctionParser.h>

namespace local_engine
{
#define REGISTER_COMMON_SCALAR_FUNCTION_PARSER(cls_name, substrait_name, ch_name) \
    class ScalarFunctionParser##cls_name : public FunctionParser \
    { \
    public: \
        ScalarFunctionParser##cls_name(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) \
        { \
        } \
        ~ScalarFunctionParser##cls_name() override = default; \
        static constexpr auto name = #substrait_name; \
        String getName() const override \
        { \
            return #substrait_name; \
        } \
\
    protected: \
        String getCHFunctionName(const substrait::Expression_ScalarFunction & /*substrait_func*/) const override \
        { \
            return #ch_name; \
        } \
    }; \
    static const FunctionParserRegister<ScalarFunctionParser##cls_name> register_scalar_function_parser_##cls_name;

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(NextDay, next_day, spark_next_day)
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(LastDay, last_day, toLastDayOfMonth)
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Str2Map, str_to_map, spark_str_to_map)
}
