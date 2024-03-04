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
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Parser/SerializedPlanParser.h>
#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <substrait/algebra.pb.h>
#include <Poco/Logger.h>
#include <Common/IFactoryWithAliases.h>
#include <Common/logger_useful.h>

namespace local_engine
{
class AggregateFunctionParser
{
public:
    /// CommonFunctionInfo is commmon representation for different function types,
    struct CommonFunctionInfo
    {
        /// basic common function informations
        using Arguments = google::protobuf::RepeatedPtrField<substrait::FunctionArgument>;
        using SortFields = google::protobuf::RepeatedPtrField<substrait::SortField>;
        Int32 function_ref;
        Arguments arguments;
        substrait::Type output_type;

        /// Following is for aggregate and window functions.
        substrait::AggregationPhase phase;
        SortFields sort_fields;
        // only be used in aggregate functions at present.
        substrait::Expression filter;
        bool is_in_window = false;
        bool is_aggregate_function = false;
        bool has_filter = false;

        CommonFunctionInfo() { function_ref = -1; }

        CommonFunctionInfo(const substrait::WindowRel::Measure & win_measure)
            : function_ref(win_measure.measure().function_reference())
            , arguments(win_measure.measure().arguments())
            , output_type(win_measure.measure().output_type())
            , phase(win_measure.measure().phase())
            , sort_fields(win_measure.measure().sorts())
        {
            is_in_window = true;
            is_aggregate_function = true;
        }

        CommonFunctionInfo(const substrait::AggregateRel::Measure & agg_measure)
            : function_ref(agg_measure.measure().function_reference())
            , arguments(agg_measure.measure().arguments())
            , output_type(agg_measure.measure().output_type())
            , phase(agg_measure.measure().phase())
            , sort_fields(agg_measure.measure().sorts())
            , filter(agg_measure.filter())
        {
            has_filter = agg_measure.has_filter();
            is_aggregate_function = true;
        }
    };

    AggregateFunctionParser(SerializedPlanParser * plan_parser_)
        : plan_parser(plan_parser_)
    {
    }

    virtual ~AggregateFunctionParser() = default;

    virtual String getName() const = 0;

    /// In some special cases, different arguments size or different arguments types may refer to different
    /// CH function implementation.
    virtual String getCHFunctionName(const CommonFunctionInfo & func_info) const = 0;

    /// In most cases, arguments size and types are enough to determine the CH function implementation.
    /// It is only be used in TypeParser::buildBlockFromNamedStruct
    /// Users are allowed to modify arg types to make it fit for ggregateFunctionFactory::instance().get(...) in TypeParser::buildBlockFromNamedStruct
    virtual String getCHFunctionName(DB::DataTypes & args) const = 0;

    /// Do some preprojections for the function arguments, and return the necessary arguments for the CH function.
    virtual DB::ActionsDAG::NodeRawConstPtrs
    parseFunctionArguments(const CommonFunctionInfo & func_info, const String & ch_func_name, DB::ActionsDAGPtr & actions_dag) const;

    DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(const CommonFunctionInfo & func_info, DB::ActionsDAGPtr & actions_dag) const
    {
        return parseFunctionArguments(func_info, getCHFunctionName(func_info), actions_dag);
    }

    // `PartialMerge` is applied on the merging stages.
    // `If` is applied when the aggreate function has a filter. This should only happen on the 1st stage.
    // If no combinator is applied, return (ch_func_name,arg_column_types)
    virtual std::pair<String, DB::DataTypes>
    tryApplyCHCombinator(const CommonFunctionInfo & func_info, const String & ch_func_name, const DB::DataTypes & arg_column_types) const;

    /// Make a postprojection for the function result.
    virtual const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const CommonFunctionInfo & func_info,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAGPtr & actions_dag,
        bool with_nullability) const;

    /// Parameters are only used in aggregate functions at present. e.g. percentiles(0.5)(x).
    /// 0.5 is the parameter of percentiles function.
    virtual DB::Array
    parseFunctionParameters(const CommonFunctionInfo & /*func_info*/, DB::ActionsDAG::NodeRawConstPtrs & /*arg_nodes*/) const
    {
        return DB::Array();
    }

    /// Return the default parameters of the function. It's useful for creating a default function instance.
    virtual DB::Array getDefaultFunctionParameters() const { return DB::Array(); }

protected:
    DB::ContextPtr getContext() const { return plan_parser->context; }

    String getUniqueName(const String & name) const { return plan_parser->getUniqueName(name); }

    const DB::ActionsDAG::Node *
    addColumnToActionsDAG(DB::ActionsDAGPtr & actions_dag, const DB::DataTypePtr & type, const DB::Field & field) const
    {
        return &actions_dag->addColumn(ColumnWithTypeAndName(type->createColumnConst(1, field), type, getUniqueName(toString(field))));
    }

    const DB::ActionsDAG::Node *
    toFunctionNode(DB::ActionsDAGPtr & action_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
    {
        return plan_parser->toFunctionNode(action_dag, func_name, args);
    }

    const DB::ActionsDAG::Node * toFunctionNode(
        DB::ActionsDAGPtr & action_dag,
        const String & func_name,
        const String & result_name,
        const DB::ActionsDAG::NodeRawConstPtrs & args) const
    {
        auto function_builder = DB::FunctionFactory::instance().get(func_name, getContext());
        return &action_dag->addFunction(function_builder, args, result_name);
    }

    const DB::ActionsDAG::Node * parseExpression(DB::ActionsDAGPtr actions_dag, const substrait::Expression & rel) const
    {
        return plan_parser->parseExpression(actions_dag, rel);
    }

    std::pair<DataTypePtr, Field> parseLiteral(const substrait::Expression_Literal & literal) const
    {
        return plan_parser->parseLiteral(literal);
    }

    SerializedPlanParser * plan_parser;
    Poco::Logger * logger = &Poco::Logger::get("AggregateFunctionParserFactory");
};

using AggregateFunctionParserPtr = std::shared_ptr<AggregateFunctionParser>;
using AggregateFunctionParserCreator = std::function<AggregateFunctionParserPtr(SerializedPlanParser *)>;

class AggregateFunctionParserFactory : public DB::IFactoryWithAliases<AggregateFunctionParserCreator>
{
public:
    using Parsers = std::unordered_map<String, Value>;

    static AggregateFunctionParserFactory & instance();

    void registerAggregateFunctionParser(const String & name, Value value);

    template <typename Parser>
    void registerAggregateFunctionParser(const String & name)
    {
        auto creator
            = [](SerializedPlanParser * plan_parser) -> AggregateFunctionParserPtr { return std::make_shared<Parser>(plan_parser); };
        registerAggregateFunctionParser(name, creator);
    }

    AggregateFunctionParserPtr get(const String & name, SerializedPlanParser * plan_parser) const;
    AggregateFunctionParserPtr tryGet(const String & name, SerializedPlanParser * plan_parser) const;

    const Parsers & getMap() const override { return parsers; }

private:
    Parsers parsers;

    /// Always empty
    Parsers case_insensitive_parsers;

    const Parsers & getCaseInsensitiveMap() const override { return case_insensitive_parsers; }

    String getFactoryName() const override { return "AggregateFunctionParserFactory"; }
};

template <typename Parser>
struct AggregateFunctionParserRegister
{
    AggregateFunctionParserRegister() { AggregateFunctionParserFactory::instance().registerAggregateFunctionParser<Parser>(Parser::name); }
};
}
