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

#include <DataTypes/IDataType.h>
#include <Parser/scalar_function_parser/arrayElement.h>

namespace local_engine
{
class FunctionParserElementAt : public FunctionParserArrayElement
{
public:
    explicit FunctionParserElementAt(SerializedPlanParser * plan_parser_) : FunctionParserArrayElement(plan_parser_) { }
    ~FunctionParserElementAt() override = default;
    static constexpr auto name = "element_at";
    String getName() const override { return name; }

    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAGPtr & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());
        if (isMap(removeNullable(parsed_args[0]->result_type)))
            return toFunctionNode(actions_dag, "arrayElement", parsed_args);
        else
            return FunctionParserArrayElement::parse(substrait_func, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserElementAt> register_element_at;
}
