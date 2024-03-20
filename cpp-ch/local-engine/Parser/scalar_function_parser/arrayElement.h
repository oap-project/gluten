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
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/FunctionParser.h>
#include <Common/CHUtil.h>

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

class FunctionParserArrayElement : public FunctionParser
{
public:
    explicit FunctionParserArrayElement(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionParserArrayElement() override = default;

    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAGPtr & actions_dag) const override
    {
        /**
            parse arrayElement(arr, idx) as
            if (idx > size(arr))
                null
            else 
                arrayElement(arr, idx)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        //arrayElement(arrary, idx)
        auto * array_element_node = toFunctionNode(actions_dag, "arrayElement", parsed_args);
        //idx > length(array)
        auto * length_node = toFunctionNode(actions_dag, "length", {parsed_args[0]});
        auto * greater_or_equals_node = toFunctionNode(actions_dag, "greater", {parsed_args[1], length_node});
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(parsed_args[0]->result_type).get());
        if (!array_type)
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "First argument for function {} must be an array", getName());
        const DataTypePtr element_type = array_type->getNestedType();
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, makeNullable(element_type), Field{});
        //if(idx > length(array), NULL, arrayElement(array, idx))
        auto * if_node = toFunctionNode(actions_dag, "if", {greater_or_equals_node, null_const_node, array_element_node});
        return if_node;
    }

protected:
    String getCHFunctionName(const substrait::Expression_ScalarFunction & /*substrait_func*/) const override { return "arrayElement"; }
};

}
