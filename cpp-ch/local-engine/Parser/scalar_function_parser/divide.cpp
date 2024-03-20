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

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Parser/FunctionParser.h>

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

class FunctionParserDivide : public FunctionParser
{
public:
    explicit FunctionParserDivide(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionParserDivide() override = default;

    static constexpr auto name = "divide";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAGPtr & actions_dag) const override
    {
        /// Parse divide(left, right) as if (right == 0) null else left / right
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        ActionsDAG::NodeRawConstPtrs new_args{parsed_args[0], parsed_args[1]};
        plan_parser->convertBinaryArithmeticFunDecimalArgs(actions_dag, new_args, substrait_func);

        const auto * left_arg = new_args[0];
        const auto * right_arg = new_args[1];

        if (isDecimal(removeNullable(left_arg->result_type)) || isDecimal(removeNullable(right_arg->result_type)))
            return toFunctionNode(actions_dag, "sparkDivideDecimal", {left_arg, right_arg});

        const auto * divide_node = toFunctionNode(actions_dag, "divide", {left_arg, right_arg});
        DataTypePtr result_type = divide_node->result_type;

        const auto * zero_node = addColumnToActionsDAG(actions_dag, result_type, removeNullable(result_type)->getDefault());
        const auto * null_node = addColumnToActionsDAG(actions_dag, makeNullable(result_type), Field{});
        const auto * is_zero_node = toFunctionNode(actions_dag, "equals", {right_arg, zero_node});
        const auto * result_node = toFunctionNode(actions_dag, "if", {is_zero_node, null_node, divide_node});
        return result_node;
    }
};

static FunctionParserRegister<FunctionParserDivide> register_divide;
}
