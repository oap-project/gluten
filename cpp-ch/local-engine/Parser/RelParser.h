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
#include <map>
#include <optional>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <base/types.h>
#include <substrait/extensions/extensions.pb.h>
#include <substrait/plan.pb.h>
namespace local_engine
{
/// parse a single substrait relation
class RelParser
{
public:
    explicit RelParser(SerializedPlanParser * plan_parser_) : plan_parser(plan_parser_) { }

    virtual ~RelParser() = default;
    virtual DB::QueryPlanPtr
    parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
        = 0;
    virtual DB::QueryPlanPtr parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack);
    virtual const substrait::Rel & getSingleInput(const substrait::Rel & rel) = 0;
    const std::vector<IQueryPlanStep *> & getSteps() const { return steps; }

    static AggregateFunctionPtr getAggregateFunction(
        const String & name, DB::DataTypes arg_types, DB::AggregateFunctionProperties & properties, const DB::Array & parameters = {});

protected:
    inline SerializedPlanParser * getPlanParser() const { return plan_parser; }
    inline ContextPtr getContext() const { return plan_parser->context; }

    inline String getUniqueName(const std::string & name) { return plan_parser->getUniqueName(name); }

    inline const std::unordered_map<std::string, std::string> & getFunctionMapping() { return plan_parser->function_mapping; }

    // Get function signature name.
    std::optional<String> parseSignatureFunctionName(UInt32 function_ref);
    // Get coresponding function name in ClickHouse.
    std::optional<String> parseFunctionName(UInt32 function_ref, const substrait::Expression_ScalarFunction & function);

    const DB::ActionsDAG::Node * parseArgument(ActionsDAGPtr action_dag, const substrait::Expression & rel)
    {
        return plan_parser->parseExpression(action_dag, rel);
    }

    const DB::ActionsDAG::Node * parseExpression(ActionsDAGPtr action_dag, const substrait::Expression & rel)
    {
        return plan_parser->parseExpression(action_dag, rel);
    }
    DB::ActionsDAGPtr expressionsToActionsDAG(const std::vector<substrait::Expression> & expressions, const DB::Block & header)
    {
        return plan_parser->expressionsToActionsDAG(expressions, header, header);
    }
    std::pair<DataTypePtr, Field> parseLiteral(const substrait::Expression_Literal & literal) { return plan_parser->parseLiteral(literal); }
    // collect all steps for metrics
    std::vector<IQueryPlanStep *> steps;

    const ActionsDAG::Node *
    buildFunctionNode(ActionsDAGPtr action_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args)
    {
        return plan_parser->toFunctionNode(action_dag, function, args);
    }

    static std::map<std::string, std::string>
    parseFormattedRelAdvancedOptimization(const substrait::extensions::AdvancedExtension & advanced_extension);
    static std::string
    getStringConfig(const std::map<std::string, std::string> & configs, const std::string & key, const std::string & default_value = "");

private:
    SerializedPlanParser * plan_parser;
};

class RelParserFactory
{
protected:
    RelParserFactory() = default;

public:
    using RelParserBuilder = std::function<std::shared_ptr<RelParser>(SerializedPlanParser *)>;
    static RelParserFactory & instance();
    void registerBuilder(UInt32 k, RelParserBuilder builder);
    RelParserBuilder getBuilder(UInt32 k);

private:
    std::map<UInt32, RelParserBuilder> builders;
};

void registerRelParsers();
}
