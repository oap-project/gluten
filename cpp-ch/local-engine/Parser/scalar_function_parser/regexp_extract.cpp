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

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

using namespace DB;

namespace local_engine
{
class FunctionParserRegexpExtract : public FunctionParser
{
public:
    explicit FunctionParserRegexpExtract(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~FunctionParserRegexpExtract() override = default;

    static constexpr auto name = "regexp_extract";
    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        const auto & args = substrait_func.arguments();
        if (args.size() != 2 && args.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 2 or 3 arguments", getName());
        
        if(args[1].value().has_literal())
        {
            auto literal_expr = args[1].value().literal();
            if (literal_expr.has_string())
            {
                std::string expr_str = literal_expr.string();
                size_t expr_size = expr_str.size();
                if (expr_str.data()[expr_size - 1] == '$')
                    expr_str.replace(expr_str.find_last_of("$"), 1, "(?:(\n)*)$");
                
                auto * regex_expr_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), expr_str);
                auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
                parsed_args[1] =  regex_expr_node;
                return toFunctionNode(actions_dag, "regexpExtract", parsed_args);
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} 2nd argument's type must be const string", getName());
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} 2nd argument's type must be const", getName());
    }
};

static FunctionParserRegister<FunctionParserRegexpExtract> register_regexp_extract;
}
