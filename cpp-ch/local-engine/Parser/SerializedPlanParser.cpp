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
#include "SerializedPlanParser.h"
#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnSet.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryPriorities.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Operator/BlocksBufferPoolTransform.h>
#include <Parser/FunctionParser.h>
#include <Parser/JoinRelParser.h>
#include <Parser/RelParser.h>
#include <Parser/TypeParser.h>
#include <Parser/MergeTreeRelParser.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBlockOutputFormat.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/printPipeline.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTreeFactory.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/wrappers.pb.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/MergeTreeTool.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_DATA_PART;
    extern const int UNKNOWN_FUNCTION;
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INVALID_JOIN_ON_EXPRESSION;
}
}

namespace local_engine
{
using namespace DB;

std::string join(const ActionsDAG::NodeRawConstPtrs & v, char c)
{
    std::string res;
    for (size_t i = 0; i < v.size(); ++i)
    {
        if (i)
            res += c;
        res += v[i]->result_name;
    }
    return res;
}

void logDebugMessage(const google::protobuf::Message & message, const char * type)
{
    auto * logger = &Poco::Logger::get("SerializedPlanParser");
    if (logger->debug())
    {
        namespace pb_util = google::protobuf::util;
        pb_util::JsonOptions options;
        std::string json;
        auto s = pb_util::MessageToJsonString(message, &json, options);
        if (!s.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not convert {} to Json", type);
        LOG_DEBUG(logger, "{}:\n{}", type, json);
    }
}

const ActionsDAG::Node * SerializedPlanParser::addColumn(ActionsDAGPtr actions_dag, const DataTypePtr & type, const Field & field)
{
    return &actions_dag->addColumn(
        ColumnWithTypeAndName(type->createColumnConst(1, field), type, getUniqueName(toString(field).substr(0, 10))));
}


void SerializedPlanParser::parseExtensions(
    const ::google::protobuf::RepeatedPtrField<substrait::extensions::SimpleExtensionDeclaration> & extensions)
{
    for (const auto & extension : extensions)
    {
        if (extension.has_extension_function())
        {
            function_mapping.emplace(
                std::to_string(extension.extension_function().function_anchor()),
                extension.extension_function().name());
        }
    }
}

std::shared_ptr<ActionsDAG> SerializedPlanParser::expressionsToActionsDAG(
    const std::vector<substrait::Expression> & expressions,
    const Block & header,
    const Block & read_schema)
{
    auto actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(header));
    NamesWithAliases required_columns;
    std::set<String> distinct_columns;

    for (const auto & expr : expressions)
    {
        if (expr.has_selection())
        {
            auto position = expr.selection().direct_reference().struct_field().field();
            auto col_name = read_schema.getByPosition(position).name;
            const ActionsDAG::Node * field = actions_dag->tryFindInOutputs(col_name);
            if (distinct_columns.contains(field->result_name))
            {
                auto unique_name = getUniqueName(field->result_name);
                required_columns.emplace_back(NameWithAlias(field->result_name, unique_name));
                distinct_columns.emplace(unique_name);
            }
            else
            {
                required_columns.emplace_back(NameWithAlias(field->result_name, field->result_name));
                distinct_columns.emplace(field->result_name);
            }
        }
        else if (expr.has_scalar_function())
        {
            const auto & scalar_function = expr.scalar_function();
            auto function_signature = function_mapping.at(std::to_string(scalar_function.function_reference()));

            std::vector<String> result_names;
            if (startsWith(function_signature, "explode:"))
                actions_dag = parseArrayJoin(header, expr, result_names, actions_dag, true, false);
            else if (startsWith(function_signature, "posexplode:"))
                actions_dag = parseArrayJoin(header, expr, result_names, actions_dag, true, true);
            else if (startsWith(function_signature, "json_tuple:"))
                actions_dag = parseJsonTuple(header, expr, result_names, actions_dag, true, false);
            else
            {
                result_names.resize(1);
                actions_dag = parseFunction(header, expr, result_names[0], actions_dag, true);
            }

            for (const auto & result_name : result_names)
            {
                if (result_name.empty())
                    continue;

                if (distinct_columns.contains(result_name))
                {
                    auto unique_name = getUniqueName(result_name);
                    required_columns.emplace_back(NameWithAlias(result_name, unique_name));
                    distinct_columns.emplace(unique_name);
                }
                else
                {
                    required_columns.emplace_back(NameWithAlias(result_name, result_name));
                    distinct_columns.emplace(result_name);
                }
            }
        }
        else if (expr.has_cast() || expr.has_if_then() || expr.has_literal())
        {
            const auto * node = parseExpression(actions_dag, expr);
            actions_dag->addOrReplaceInOutputs(*node);
            if (distinct_columns.contains(node->result_name))
            {
                auto unique_name = getUniqueName(node->result_name);
                required_columns.emplace_back(NameWithAlias(node->result_name, unique_name));
                distinct_columns.emplace(unique_name);
            }
            else
            {
                required_columns.emplace_back(NameWithAlias(node->result_name, node->result_name));
                distinct_columns.emplace(node->result_name);
            }
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported projection type {}.", magic_enum::enum_name(expr.rex_type_case()));
    }
    actions_dag->project(required_columns);
    return actions_dag;
}

std::string getDecimalFunction(const substrait::Type_Decimal & decimal, bool null_on_overflow)
{
    std::string ch_function_name;
    UInt32 precision = decimal.precision();

    if (precision <= DataTypeDecimal32::maxPrecision())
        ch_function_name = "toDecimal32";
    else if (precision <= DataTypeDecimal64::maxPrecision())
        ch_function_name = "toDecimal64";
    else if (precision <= DataTypeDecimal128::maxPrecision())
        ch_function_name = "toDecimal128";
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);

    if (null_on_overflow)
        ch_function_name = ch_function_name + "OrNull";
    return ch_function_name;
}

bool SerializedPlanParser::isReadRelFromJava(const substrait::ReadRel & rel)
{
    return rel.has_local_files() && rel.local_files().items().size() == 1 && rel.local_files().items().at(0).uri_file().starts_with(
        "iterator");
}

bool SerializedPlanParser::isReadFromMergeTree(const substrait::ReadRel & rel)
{
    assert(rel.has_advanced_extension());
    bool is_read_from_merge_tree;
    google::protobuf::StringValue optimization;
    optimization.ParseFromString(rel.advanced_extension().optimization().value());
    ReadBufferFromString in(optimization.value());
    assertString("isMergeTree=", in);
    readBoolText(is_read_from_merge_tree, in);
    assertChar('\n', in);
    return is_read_from_merge_tree;
}

QueryPlanStepPtr SerializedPlanParser::parseReadRealWithLocalFile(const substrait::ReadRel & rel)
{
    auto header = TypeParser::buildBlockFromNamedStruct(rel.base_schema());
    substrait::ReadRel::LocalFiles local_files;
    if (rel.has_local_files())
        local_files = rel.local_files();
    else
        local_files = parseLocalFiles(split_infos.at(nextSplitInfoIndex()));
    auto source = std::make_shared<SubstraitFileSource>(context, header, local_files);
    auto source_pipe = Pipe(source);
    auto source_step = std::make_unique<SubstraitFileSourceStep>(context, std::move(source_pipe), "substrait local files");
    source_step->setStepDescription("read local files");
    if (rel.has_filter())
    {
        const ActionsDAGPtr actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(header));
        const ActionsDAG::Node * filter_node = parseExpression(actions_dag, rel.filter());
        actions_dag->addOrReplaceInOutputs(*filter_node);
        assert(filter_node == &(actions_dag->findInOutputs(filter_node->result_name)));
        source_step->addFilter(actions_dag, filter_node->result_name);
    }
    return source_step;
}

QueryPlanStepPtr SerializedPlanParser::parseReadRealWithJavaIter(const substrait::ReadRel & rel)
{
    assert(rel.has_local_files());
    assert(rel.local_files().items().size() == 1);
    auto iter = rel.local_files().items().at(0).uri_file();
    auto pos = iter.find(':');
    auto iter_index = std::stoi(iter.substr(pos + 1, iter.size()));

    auto source = std::make_shared<SourceFromJavaIter>(
        context,
        TypeParser::buildBlockFromNamedStruct(rel.base_schema()),
        input_iters[iter_index],
        materialize_inputs[iter_index]);
    QueryPlanStepPtr source_step = std::make_unique<ReadFromPreparedSource>(Pipe(source));
    source_step->setStepDescription("Read From Java Iter");
    return source_step;
}

IQueryPlanStep * SerializedPlanParser::addRemoveNullableStep(QueryPlan & plan, const std::set<String> & columns)
{
    if (columns.empty())
        return nullptr;

    auto remove_nullable_actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(plan.getCurrentDataStream().header));
    removeNullableForRequiredColumns(columns, remove_nullable_actions_dag);
    auto expression_step = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), remove_nullable_actions_dag);
    expression_step->setStepDescription("Remove nullable properties");
    auto * step_ptr = expression_step.get();
    plan.addStep(std::move(expression_step));
    return step_ptr;
}

PrewhereInfoPtr SerializedPlanParser::parsePreWhereInfo(const substrait::Expression & rel, Block & input)
{
    auto prewhere_info = std::make_shared<PrewhereInfo>();
    prewhere_info->prewhere_actions = std::make_shared<ActionsDAG>(input.getNamesAndTypesList());
    std::string filter_name;
    // for in function
    if (rel.has_singular_or_list())
    {
        const auto * in_node = parseExpression(prewhere_info->prewhere_actions, rel);
        prewhere_info->prewhere_actions->addOrReplaceInOutputs(*in_node);
        filter_name = in_node->result_name;
    }
    else
    {
        parseFunctionWithDAG(rel, filter_name, prewhere_info->prewhere_actions, true);
    }
    prewhere_info->prewhere_column_name = filter_name;
    prewhere_info->need_filter = true;
    prewhere_info->remove_prewhere_column = true;
    auto cols = prewhere_info->prewhere_actions->getRequiredColumnsNames();
    // Keep it the same as the input.
    prewhere_info->prewhere_actions->removeUnusedActions(Names{filter_name}, false, true);
    prewhere_info->prewhere_actions->projectInput(false);
    for (const auto & name : input.getNames())
    {
        prewhere_info->prewhere_actions->tryRestoreColumn(name);
    }
    return prewhere_info;
}

DataTypePtr wrapNullableType(substrait::Type_Nullability nullable, DataTypePtr nested_type)
{
    return wrapNullableType(nullable == substrait::Type_Nullability_NULLABILITY_NULLABLE, nested_type);
}

DataTypePtr wrapNullableType(bool nullable, DataTypePtr nested_type)
{
    if (nullable && !nested_type->isNullable())
    {
        if (nested_type->isLowCardinalityNullable())
        {
            return nested_type;
        }
        else
        {
            if (!nested_type->lowCardinality())
                return std::make_shared<DataTypeNullable>(nested_type);
            else
                return std::make_shared<DataTypeLowCardinality>(
                    std::make_shared<DataTypeNullable>(
                        dynamic_cast<const DataTypeLowCardinality &>(*nested_type).getDictionaryType()));
        }
    }


    if (nullable && !nested_type->isNullable())
        return std::make_shared<DataTypeNullable>(nested_type);
    else
        return nested_type;
}

QueryPlanPtr SerializedPlanParser::parse(std::unique_ptr<substrait::Plan> plan)
{
    logDebugMessage(*plan, "substrait plan");
    parseExtensions(plan->extensions());
    if (plan->relations_size() == 1)
    {
        auto root_rel = plan->relations().at(0);
        if (!root_rel.has_root())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "must have root rel!");
        }
        std::list<const substrait::Rel *> rel_stack;
        auto query_plan = parseOp(root_rel.root().input(), rel_stack);
        if (root_rel.root().names_size())
        {
            ActionsDAGPtr actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(query_plan->getCurrentDataStream().header));
            NamesWithAliases aliases;
            auto cols = query_plan->getCurrentDataStream().header.getNamesAndTypesList();
            if (cols.getNames().size() != static_cast<size_t>(root_rel.root().names_size()))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Missmatch result columns size.");
            }
            for (int i = 0; i < static_cast<int>(cols.getNames().size()); i++)
            {
                aliases.emplace_back(NameWithAlias(cols.getNames()[i], root_rel.root().names(i)));
            }
            actions_dag->project(aliases);
            auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), actions_dag);
            expression_step->setStepDescription("Rename Output");
            query_plan->addStep(std::move(expression_step));
        }

        // fixes: issue-1874, to keep the nullability as expected.
        const auto & output_schema = root_rel.root().output_schema();
        if (output_schema.types_size())
        {
            auto original_header = query_plan->getCurrentDataStream().header;
            const auto & original_cols = original_header.getColumnsWithTypeAndName();
            if (static_cast<size_t>(output_schema.types_size()) != original_cols.size())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Mismatch output schema");
            }
            bool need_final_project = false;
            ColumnsWithTypeAndName final_cols;
            for (int i = 0; i < output_schema.types_size(); ++i)
            {
                const auto & col = original_cols[i];
                auto type = TypeParser::parseType(output_schema.types(i));
                // At present, we only check nullable mismatch.
                // intermediate aggregate data is special, no check here.
                if (type->isNullable() != col.type->isNullable() && !typeid_cast<const DataTypeAggregateFunction *>(col.type.get()))
                {
                    if (type->isNullable())
                    {
                        auto wrapped = wrapNullableType(true, col.type);
                        final_cols.emplace_back(type->createColumn(), wrapped, col.name);
                        need_final_project = !wrapped->equals(*col.type);
                    }
                    else
                    {
                        final_cols.emplace_back(type->createColumn(), removeNullable(col.type), col.name);
                        need_final_project = true;
                    }
                }
                else
                {
                    final_cols.push_back(col);
                }
            }
            if (need_final_project)
            {
                ActionsDAGPtr final_project
                    = ActionsDAG::makeConvertingActions(original_cols, final_cols, ActionsDAG::MatchColumnsMode::Position);
                QueryPlanStepPtr final_project_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), final_project);
                final_project_step->setStepDescription("Project for output schema");
                query_plan->addStep(std::move(final_project_step));
            }
        }
        return query_plan;
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "too many relations found");
    }
}

QueryPlanPtr SerializedPlanParser::parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    QueryPlanPtr query_plan;
    std::vector<IQueryPlanStep *> steps;
    switch (rel.rel_type_case())
    {
        case substrait::Rel::RelTypeCase::kFetch: {
            rel_stack.push_back(&rel);
            const auto & limit = rel.fetch();
            query_plan = parseOp(limit.input(), rel_stack);
            rel_stack.pop_back();
            auto limit_step = std::make_unique<LimitStep>(query_plan->getCurrentDataStream(), limit.count(), limit.offset());
            limit_step->setStepDescription("LIMIT");
            steps.emplace_back(limit_step.get());
            query_plan->addStep(std::move(limit_step));
            break;
        }
        case substrait::Rel::RelTypeCase::kRead: {
            const auto & read = rel.read();
            // TODO: We still maintain the old logic of parsing LocalFiles or ExtensionTable in RealRel
            // to be compatiable with some suites about metrics.
            // Remove this compatiability in later and then only java iter has local files in ReadRel.
            if (read.has_local_files() || (!read.has_extension_table() && !isReadFromMergeTree(read)))
            {
                assert(read.has_base_schema());
                QueryPlanStepPtr step;
                if (isReadRelFromJava(read))
                    step = parseReadRealWithJavaIter(read);
                else
                    step = parseReadRealWithLocalFile(read);

                query_plan = std::make_unique<QueryPlan>();
                steps.emplace_back(step.get());
                query_plan->addStep(std::move(step));

                // Add a buffer after source, it try to preload data from source and reduce the
                // waiting time of downstream nodes.
                if (context->getSettingsRef().max_threads > 1)
                {
                    auto buffer_step = std::make_unique<BlocksBufferPoolStep>(query_plan->getCurrentDataStream());
                    steps.emplace_back(buffer_step.get());
                    query_plan->addStep(std::move(buffer_step));
                }
            }
            else
            {
                substrait::ReadRel::ExtensionTable extension_table;
                if (read.has_extension_table())
                    extension_table = read.extension_table();
                else
                    extension_table = parseExtensionTable(split_infos.at(nextSplitInfoIndex()));

                MergeTreeRelParser mergeTreeParser(this, context, query_context, global_context);
                std::list<const substrait::Rel *> stack;
                query_plan = mergeTreeParser.parseReadRel(std::make_unique<QueryPlan>(), read, extension_table, stack);
                steps = mergeTreeParser.getSteps();
            }
            break;
        }
        case substrait::Rel::RelTypeCase::kFilter:
        case substrait::Rel::RelTypeCase::kGenerate:
        case substrait::Rel::RelTypeCase::kProject:
        case substrait::Rel::RelTypeCase::kAggregate:
        case substrait::Rel::RelTypeCase::kSort:
        case substrait::Rel::RelTypeCase::kWindow:
        case substrait::Rel::RelTypeCase::kJoin:
        case substrait::Rel::RelTypeCase::kExpand: {
            auto op_parser = RelParserFactory::instance().getBuilder(rel.rel_type_case())(this);
            query_plan = op_parser->parseOp(rel, rel_stack);
            auto parser_steps = op_parser->getSteps();
            steps.insert(steps.end(), parser_steps.begin(), parser_steps.end());
            break;
        }
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "doesn't support relation type: {}.\n{}", rel.rel_type_case(), rel.DebugString());
    }

    if (!context->getSettingsRef().query_plan_enable_optimizations)
    {
        if (rel.rel_type_case() == substrait::Rel::RelTypeCase::kRead)
        {
            size_t id = metrics.empty() ? 0 : metrics.back()->getId() + 1;
            metrics.emplace_back(std::make_shared<RelMetric>(id, String(magic_enum::enum_name(rel.rel_type_case())), steps));
        }
        else
            metrics = {std::make_shared<RelMetric>(String(magic_enum::enum_name(rel.rel_type_case())), metrics, steps)};
    }

    return query_plan;
}

NamesAndTypesList SerializedPlanParser::blockToNameAndTypeList(const Block & header)
{
    NamesAndTypesList types;
    for (const auto & name : header.getNames())
    {
        const auto * column = header.findByName(name);
        types.push_back(NameAndTypePair(column->name, column->type));
    }
    return types;
}

std::string
SerializedPlanParser::getFunctionName(const std::string & function_signature, const substrait::Expression_ScalarFunction & function)
{
    auto args = function.arguments();
    auto pos = function_signature.find(':');
    auto function_name = function_signature.substr(0, pos);
    if (!SCALAR_FUNCTIONS.contains(function_name))
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unsupported function {}", function_name);

    std::string ch_function_name;
    if (function_name == "trim")
        ch_function_name = args.size() == 1 ? "trimBoth" : "trimBothSpark";
    else if (function_name == "ltrim")
        ch_function_name = args.size() == 1 ? "trimLeft" : "trimLeftSpark";
    else if (function_name == "rtrim")
        ch_function_name = args.size() == 1 ? "trimRight" : "trimRightSpark";
    else if (function_name == "extract")
    {
        if (args.size() != 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Spark function extract requires two args, function:{}",
                function.ShortDebugString());

        // Get the first arg: field
        const auto & extract_field = args.at(0);

        if (extract_field.value().has_literal())
        {
            const auto & field_value = extract_field.value().literal().string();
            if (field_value == "YEAR")
                ch_function_name = "toYear"; // spark: extract(YEAR FROM) or year
            else if (field_value == "YEAR_OF_WEEK")
                ch_function_name = "toISOYear"; // spark: extract(YEAROFWEEK FROM)
            else if (field_value == "QUARTER")
                ch_function_name = "toQuarter"; // spark: extract(QUARTER FROM) or quarter
            else if (field_value == "MONTH")
                ch_function_name = "toMonth"; // spark: extract(MONTH FROM) or month
            else if (field_value == "WEEK_OF_YEAR")
                ch_function_name = "toISOWeek"; // spark: extract(WEEK FROM) or weekofyear
            else if (field_value == "WEEK_DAY")
                /// Spark WeekDay(date) (0 = Monday, 1 = Tuesday, ..., 6 = Sunday)
                /// Substrait: extract(WEEK_DAY from date)
                /// CH: toDayOfWeek(date, 1)
                ch_function_name = "toDayOfWeek";
            else if (field_value == "DAY_OF_WEEK")
                /// Spark: DayOfWeek(date) (1 = Sunday, 2 = Monday, ..., 7 = Saturday)
                /// Substrait: extract(DAY_OF_WEEK from date)
                /// CH: toDayOfWeek(date, 3)
                /// DAYOFWEEK is alias of function toDayOfWeek.
                /// This trick is to distinguish between extract fields DAY_OF_WEEK and WEEK_DAY in latter codes
                ch_function_name = "DAYOFWEEK";
            else if (field_value == "DAY")
                ch_function_name = "toDayOfMonth"; // spark: extract(DAY FROM) or dayofmonth
            else if (field_value == "DAY_OF_YEAR")
                ch_function_name = "toDayOfYear"; // spark: extract(DOY FROM) or dayofyear
            else if (field_value == "HOUR")
                ch_function_name = "toHour"; // spark: extract(HOUR FROM) or hour
            else if (field_value == "MINUTE")
                ch_function_name = "toMinute"; // spark: extract(MINUTE FROM) or minute
            else if (field_value == "SECOND")
                ch_function_name = "toSecond"; // spark: extract(SECOND FROM) or secondwithfraction
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first arg of spark extract function is wrong.");
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first arg of spark extract function is wrong.");
    }
    else if (function_name == "check_overflow")
    {
        if (args.size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "check_overflow function requires at least two args.");
        ch_function_name = SCALAR_FUNCTIONS.at(function_name);
        auto null_on_overflow = args.at(1).value().literal().boolean();
        if (null_on_overflow)
            ch_function_name = ch_function_name + "OrNull";
    }
    else if (function_name == "make_decimal")
    {
        if (args.size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "make_decimal function requires at least 2 args.");
        ch_function_name = SCALAR_FUNCTIONS.at(function_name);
        auto null_on_overflow = args.at(1).value().literal().boolean();
        if (null_on_overflow)
            ch_function_name = ch_function_name + "OrNull";
    }
    else if (function_name == "char_length")
    {
        /// In Spark
        /// char_length returns the number of bytes when input is binary type, corresponding to CH length function
        /// char_length returns the number of characters when input is string type, corresponding to CH char_length function
        ch_function_name = SCALAR_FUNCTIONS.at(function_name);
        if (function_signature.find("vbin") != std::string::npos)
            ch_function_name = "length";
    }
    else if (function_name == "reverse")
    {
        if (function.output_type().has_list())
            ch_function_name = "arrayReverse";
        else
            ch_function_name = "reverseUTF8";
    }
    else if (function_name == "concat")
    {
        /// 1. ConcatOverloadResolver cannot build arrayConcat for Nullable(Array) type which causes failures when using functions like concat(split()).
        ///    So we use arrayConcat directly if the output type is array.
        /// 2. CH ConcatImpl can only accept at least 2 arguments, but Spark concat can accept 1 argument, like concat('a')
        ///    in such case we use identity function
        if (function.output_type().has_list())
            ch_function_name = "arrayConcat";
        else if (args.size() == 1)
            ch_function_name = "identity";
        else
            ch_function_name = "concat";
    }
    else
        ch_function_name = SCALAR_FUNCTIONS.at(function_name);

    return ch_function_name;
}

void SerializedPlanParser::parseArrayJoinArguments(
    ActionsDAGPtr & actions_dag,
    const std::string & function_name,
    const substrait::Expression_ScalarFunction & scalar_function,
    bool position,
    ActionsDAG::NodeRawConstPtrs & parsed_args,
    bool & is_map)
{
    if (function_name != "arrayJoin")
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Function parseArrayJoinArguments should only process arrayJoin function instead of {}",
            function_name);

    /// The argument number of arrayJoin(converted from Spark explode/posexplode) should be 1
    if (scalar_function.arguments_size() != 1)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Argument number of arrayJoin should be 1 instead of {}",
            scalar_function.arguments_size());

    auto function_name_copy = function_name;
    parseFunctionArguments(actions_dag, parsed_args, function_name_copy, scalar_function);

    auto arg = parsed_args[0];
    auto arg_type = removeNullable(arg->result_type);
    if (isMap(arg_type))
        is_map = true;
    else if (isArray(arg_type))
        is_map = false;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument type of arrayJoin should be Array or Map but is {}", arg_type->getName());

    /// Remove Nullable for input argument of arrayJoin function because arrayJoin function only accept non-nullable input
    /// array() or map()
    const auto * empty_node = addColumn(actions_dag, arg_type, is_map ? Field(Map()) : Field(Array()));
    /// ifNull(arg, array()) or ifNull(arg, map())
    const auto * if_null_node = toFunctionNode(actions_dag, "ifNull", {arg, empty_node});
    /// assumeNotNull(ifNull(arg, array())) or assumeNotNull(ifNull(arg, map()))
    const auto * not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {if_null_node});
    /// Wrap with materalize function to make sure column input to ARRAY JOIN STEP is materaized
    arg = &actions_dag->materializeNode(*not_null_node);

    /// If spark function is posexplode, we need to add position column together with input argument
    if (position)
    {
        /// length(arg)
        const auto * length_node = toFunctionNode(actions_dag, "length", {arg});
        /// range(length(arg))
        const auto * range_node = toFunctionNode(actions_dag, "range", {length_node});
        /// mapFromArrays(range(length(arg)), arg)
        arg = toFunctionNode(actions_dag, "mapFromArrays", {range_node, arg});
    }

    parsed_args[0] = arg;
}

ActionsDAG::NodeRawConstPtrs SerializedPlanParser::parseArrayJoinWithDAG(
    const substrait::Expression & rel,
    std::vector<String> & result_names,
    ActionsDAGPtr actions_dag,
    bool keep_result,
    bool position)
{
    if (!rel.has_scalar_function())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The root of expression should be a scalar function:\n {}", rel.DebugString());

    const auto & scalar_function = rel.scalar_function();

    auto function_signature = function_mapping.at(std::to_string(rel.scalar_function().function_reference()));
    auto function_name = getFunctionName(function_signature, scalar_function);

    /// Whether the input argument of explode/posexplode is map type
    bool is_map;
    ActionsDAG::NodeRawConstPtrs args;
    parseArrayJoinArguments(actions_dag, function_name, scalar_function, position, args, is_map);

    /// Note: Make sure result_name keep the same after applying arrayJoin function, which makes it much easier to transform arrayJoin function to ARRAY JOIN STEP
    /// Otherwise an alias node must be appended after ARRAY JOIN STEP, which is not a graceful implementation.
    const auto & arg_not_null = args[0];
    auto array_join_name = arg_not_null->result_name;
    /// arrayJoin(arg_not_null)
    const auto * array_join_node = &actions_dag->addArrayJoin(*arg_not_null, array_join_name);

    auto tuple_element_builder = FunctionFactory::instance().get("sparkTupleElement", context);
    auto tuple_index_type = std::make_shared<DataTypeUInt32>();
    auto add_tuple_element = [&](const ActionsDAG::Node * tuple_node, size_t i) -> const ActionsDAG::Node * {
        ColumnWithTypeAndName index_col(tuple_index_type->createColumnConst(1, i), tuple_index_type, getUniqueName(std::to_string(i)));
        const auto * index_node = &actions_dag->addColumn(std::move(index_col));
        auto result_name = "sparkTupleElement(" + tuple_node->result_name + ", " + index_node->result_name + ")";
        return &actions_dag->addFunction(tuple_element_builder, {tuple_node, index_node}, result_name);
    };

    /// Special process to keep compatiable with Spark
    if (!position)
    {
        /// Spark: explode(array_or_map) -> CH: arrayJoin(array_or_map)
        if (is_map)
        {
            /// In Spark: explode(map(k, v)) output 2 columns with default names "key" and "value"
            /// In CH: arrayJoin(map(k, v)) output 1 column with Tuple Type.
            /// So we must wrap arrayJoin with sparkTupleElement function for compatiability.

            /// arrayJoin(arg_not_null).1
            const auto * key_node = add_tuple_element(array_join_node, 1);

            /// arrayJoin(arg_not_null).2
            const auto * val_node = add_tuple_element(array_join_node, 2);

            result_names.push_back(key_node->result_name);
            result_names.push_back(val_node->result_name);
            if (keep_result)
            {
                actions_dag->addOrReplaceInOutputs(*key_node);
                actions_dag->addOrReplaceInOutputs(*val_node);
            }
            return {key_node, val_node};
        }
        else
        {
            result_names.push_back(array_join_name);
            if (keep_result)
                actions_dag->addOrReplaceInOutputs(*array_join_node);
            return {array_join_node};
        }
    }
    else
    {
        /// Spark: posexplode(array_or_map) -> CH: arrayJoin(map), in which map = mapFromArrays(range(length(array_or_map)), array_or_map)

        /// In Spark: posexplode(array_of_map) output 2 or 3 columns: (pos, col) or (pos, key, value)
        /// In CH: arrayJoin(map(k, v)) output 1 column with Tuple Type.
        /// So we must wrap arrayJoin with sparkTupleElement function for compatiability.

        /// pos = cast(arrayJoin(arg_not_null).1, "Int32")
        const auto * pos_node = add_tuple_element(array_join_node, 1);
        pos_node = ActionsDAGUtil::convertNodeType(actions_dag, pos_node, "Int32");

        /// if is_map is false, output col = arrayJoin(arg_not_null).2
        /// if is_map is true,  output (key, value) = arrayJoin(arg_not_null).2
        const auto * item_node = add_tuple_element(array_join_node, 2);

        if (is_map)
        {
            /// key = arrayJoin(arg_not_null).2.1
            const auto * key_node = add_tuple_element(item_node, 1);

            /// value = arrayJoin(arg_not_null).2.2
            const auto * value_node = add_tuple_element(item_node, 2);

            result_names.push_back(pos_node->result_name);
            result_names.push_back(key_node->result_name);
            result_names.push_back(value_node->result_name);
            if (keep_result)
            {
                actions_dag->addOrReplaceInOutputs(*pos_node);
                actions_dag->addOrReplaceInOutputs(*key_node);
                actions_dag->addOrReplaceInOutputs(*value_node);
            }

            return {pos_node, key_node, value_node};
        }
        else
        {
            /// col = arrayJoin(arg_not_null).2
            result_names.push_back(pos_node->result_name);
            result_names.push_back(item_node->result_name);
            if (keep_result)
            {
                actions_dag->addOrReplaceInOutputs(*pos_node);
                actions_dag->addOrReplaceInOutputs(*item_node);
            }
            return {pos_node, item_node};
        }
    }
}

const ActionsDAG::Node * SerializedPlanParser::parseFunctionWithDAG(
    const substrait::Expression & rel,
    std::string & result_name,
    ActionsDAGPtr actions_dag,
    bool keep_result)
{
    if (!rel.has_scalar_function())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "the root of expression should be a scalar function:\n {}", rel.DebugString());

    const auto & scalar_function = rel.scalar_function();
    auto function_signature = function_mapping.at(std::to_string(scalar_function.function_reference()));

    /// If the substrait function name is registered in FunctionParserFactory, use it to parse the function, and return result directly
    auto pos = function_signature.find(':');
    auto func_name = function_signature.substr(0, pos);

    auto func_parser = FunctionParserFactory::instance().tryGet(func_name, this);
    if (func_parser)
    {
        LOG_DEBUG(
            &Poco::Logger::get("SerializedPlanParser"),
            "parse function {} by function parser: {}",
            func_name,
            func_parser->getName());
        const auto * result_node = func_parser->parse(scalar_function, actions_dag);
        if (keep_result)
            actions_dag->addOrReplaceInOutputs(*result_node);

        result_name = result_node->result_name;
        return result_node;
    }

    auto ch_func_name = getFunctionName(function_signature, scalar_function);
    ActionsDAG::NodeRawConstPtrs args;
    parseFunctionArguments(actions_dag, args, ch_func_name, scalar_function);

    /// If the first argument of function formatDateTimeInJodaSyntax is integer, replace formatDateTimeInJodaSyntax with fromUnixTimestampInJodaSyntax
    /// to avoid exception
    if (ch_func_name == "formatDateTimeInJodaSyntax")
    {
        if (args.size() > 1 && isInteger(removeNullable(args[0]->result_type)))
            ch_func_name = "fromUnixTimestampInJodaSyntax";
    }

    if (ch_func_name == "alias")
    {
        result_name = args[0]->result_name;
        actions_dag->addOrReplaceInOutputs(*args[0]);
        return &actions_dag->addAlias(actions_dag->findInOutputs(result_name), result_name);
    }

    const ActionsDAG::Node * result_node;

    if (ch_func_name == "splitByRegexp")
    {
        if (args.size() >= 2)
        {
            /// In Spark: split(str, regex [, limit] )
            /// In CH: splitByRegexp(regexp, str [, limit])
            std::swap(args[0], args[1]);
        }
    }

    if (function_signature.find("check_overflow:", 0) != function_signature.npos)
    {
        if (scalar_function.arguments().size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "check_overflow function requires at least two args.");

        ActionsDAG::NodeRawConstPtrs new_args;
        new_args.reserve(3);
        new_args.emplace_back(args[0]);

        UInt32 precision = rel.scalar_function().output_type().decimal().precision();
        UInt32 scale = rel.scalar_function().output_type().decimal().scale();
        auto uint32_type = std::make_shared<DataTypeUInt32>();
        new_args.emplace_back(
            &actions_dag->addColumn(
                ColumnWithTypeAndName(uint32_type->createColumnConst(1, precision), uint32_type, getUniqueName(toString(precision)))));
        new_args.emplace_back(
            &actions_dag->addColumn(
                ColumnWithTypeAndName(uint32_type->createColumnConst(1, scale), uint32_type, getUniqueName(toString(scale)))));
        args = std::move(new_args);
    }
    else if (startsWith(function_signature, "make_decimal:"))
    {
        if (scalar_function.arguments().size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "make_decimal function requires at least 2 args.");

        ActionsDAG::NodeRawConstPtrs new_args;
        new_args.reserve(3);
        new_args.emplace_back(args[0]);

        UInt32 precision = rel.scalar_function().output_type().decimal().precision();
        UInt32 scale = rel.scalar_function().output_type().decimal().scale();
        auto uint32_type = std::make_shared<DataTypeUInt32>();
        new_args.emplace_back(
            &actions_dag->addColumn(
                ColumnWithTypeAndName(uint32_type->createColumnConst(1, precision), uint32_type, getUniqueName(toString(precision)))));
        new_args.emplace_back(
            &actions_dag->addColumn(
                ColumnWithTypeAndName(uint32_type->createColumnConst(1, scale), uint32_type, getUniqueName(toString(scale)))));
        args = std::move(new_args);
    }

    bool converted_decimal_args = convertBinaryArithmeticFunDecimalArgs(actions_dag, args, scalar_function);
    auto function_builder = FunctionFactory::instance().get(ch_func_name, context);
    std::string args_name = join(args, ',');
    result_name = ch_func_name + "(" + args_name + ")";
    const auto * function_node = &actions_dag->addFunction(function_builder, args, result_name);
    result_node = function_node;
    if (!TypeParser::isTypeMatched(rel.scalar_function().output_type(), function_node->result_type) && !converted_decimal_args)
    {
        auto result_type = TypeParser::parseType(rel.scalar_function().output_type());
        if (isDecimalOrNullableDecimal(result_type))
        {
            result_node = ActionsDAGUtil::convertNodeType(
                actions_dag,
                function_node,
                // as stated in isTypeMatched， currently we don't change nullability of the result type
                function_node->result_type->isNullable()
                ? local_engine::wrapNullableType(true, result_type)->getName()
                : local_engine::removeNullable(result_type)->getName(),
                function_node->result_name,
                CastType::accurateOrNull);
        }
        else
        {
            result_node = ActionsDAGUtil::convertNodeType(
                actions_dag,
                function_node,
                // as stated in isTypeMatched， currently we don't change nullability of the result type
                function_node->result_type->isNullable()
                ? local_engine::wrapNullableType(true, result_type)->getName()
                : local_engine::removeNullable(result_type)->getName(),
                function_node->result_name);
        }
    }

    if (ch_func_name == "JSON_VALUE")
        result_node->function->setResolver(function_builder);

    if (keep_result)
        actions_dag->addOrReplaceInOutputs(*result_node);

    return result_node;
}

bool SerializedPlanParser::convertBinaryArithmeticFunDecimalArgs(
    ActionsDAGPtr actions_dag,
    ActionsDAG::NodeRawConstPtrs & args,
    const substrait::Expression_ScalarFunction & arithmeticFun)
{
    auto function_signature = function_mapping.at(std::to_string(arithmeticFun.function_reference()));
    auto pos = function_signature.find(':');
    auto func_name = function_signature.substr(0, pos);

    if (func_name == "divide" || func_name == "multiply" || func_name == "plus" || func_name == "minus")
    {
        /// for divide/plus/minus, we need to convert first arg to result precision and scale
        /// for multiply, we need to convert first arg to result precision, but keep scale
        auto arg1_type = removeNullable(args[0]->result_type);
        auto arg2_type = removeNullable(args[1]->result_type);
        if (isDecimal(arg1_type) && isDecimal(arg2_type))
        {
            UInt32 p1 = getDecimalPrecision(*arg1_type);
            UInt32 s1 = getDecimalScale(*arg1_type);
            UInt32 p2 = getDecimalPrecision(*arg2_type);
            UInt32 s2 = getDecimalScale(*arg2_type);

            UInt32 precision;
            UInt32 scale;

            if (func_name == "plus" || func_name == "minus")
            {
                scale = s1;
                precision = scale + std::max(p1 - s1, p2 - s2) + 1;
            }
            else if (func_name == "divide")
            {
                scale = std::max(static_cast<UInt32>(6), s1 + p2 + 1);
                precision = p1 - s1 + s2 + scale;
            }
            else // multiply
            {
                scale = s1;
                precision = p1 + p2 + 1;
            }

            UInt32 maxPrecision = DataTypeDecimal256::maxPrecision();
            UInt32 maxScale = DataTypeDecimal128::maxPrecision();
            precision = std::min(precision, maxPrecision);
            scale = std::min(scale, maxScale);

            ActionsDAG::NodeRawConstPtrs new_args;
            new_args.reserve(args.size());

            ActionsDAG::NodeRawConstPtrs cast_args;
            cast_args.reserve(2);
            cast_args.emplace_back(args[0]);
            DataTypePtr ch_type = createDecimal<DataTypeDecimal>(precision, scale);
            ch_type = wrapNullableType(arithmeticFun.output_type().decimal().nullability(), ch_type);
            String type_name = ch_type->getName();
            DataTypePtr str_type = std::make_shared<DataTypeString>();
            const ActionsDAG::Node * type_node = &actions_dag->addColumn(
                ColumnWithTypeAndName(str_type->createColumnConst(1, type_name), str_type, getUniqueName(type_name)));
            cast_args.emplace_back(type_node);
            const ActionsDAG::Node * cast_node = toFunctionNode(actions_dag, "CAST", cast_args);
            actions_dag->addOrReplaceInOutputs(*cast_node);
            new_args.emplace_back(cast_node);
            new_args.emplace_back(args[1]);
            args = std::move(new_args);
            return true;
        }
    }
    return false;
}

void SerializedPlanParser::parseFunctionArguments(
    ActionsDAGPtr & actions_dag,
    ActionsDAG::NodeRawConstPtrs & parsed_args,
    std::string & function_name,
    const substrait::Expression_ScalarFunction & scalar_function)
{
    auto function_signature = function_mapping.at(std::to_string(scalar_function.function_reference()));
    const auto & args = scalar_function.arguments();
    parsed_args.reserve(args.size());

    // Some functions need to be handled specially.
    if (function_name == "JSONExtract")
    {
        parseFunctionArgument(actions_dag, parsed_args, function_name, args[0]);
        auto data_type = TypeParser::parseType(scalar_function.output_type());
        parsed_args.emplace_back(addColumn(actions_dag, std::make_shared<DataTypeString>(), data_type->getName()));
    }
    else if (function_name == "sparkTupleElement" || function_name == "tupleElement")
    {
        parseFunctionArgument(actions_dag, parsed_args, function_name, args[0]);

        if (!args[1].value().has_literal())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "get_struct_field's second argument must be a literal");

        auto [data_type, field] = parseLiteral(args[1].value().literal());
        if (data_type->getTypeId() != TypeIndex::Int32)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "get_struct_field's second argument must be i32");

        // tuple indecies start from 1, in spark, start from 0
        Int32 field_index = static_cast<Int32>(field.get<Int32>() + 1);
        const auto * index_node = addColumn(actions_dag, std::make_shared<DataTypeUInt32>(), field_index);
        parsed_args.emplace_back(index_node);
    }
    else if (function_name == "tuple")
    {
        // Arguments in the format, (<field name>, <value expression>[, <field name>, <value expression> ...])
        // We don't need to care the field names here.
        for (int index = 1; index < args.size(); index += 2)
            parseFunctionArgument(actions_dag, parsed_args, function_name, args[index]);
    }
    else if (function_name == "repeat")
    {
        // repeat. the field index must be unsigned integer in CH, cast the signed integer in substrait
        // which must be a positive value into unsigned integer here.
        parseFunctionArgument(actions_dag, parsed_args, function_name, args[0]);
        const ActionsDAG::Node * repeat_times_node = parseFunctionArgument(actions_dag, function_name, args[1]);
        DataTypeNullable target_type(std::make_shared<DataTypeUInt32>());
        repeat_times_node = ActionsDAGUtil::convertNodeType(actions_dag, repeat_times_node, target_type.getName());
        parsed_args.emplace_back(repeat_times_node);
    }
    else if (function_name == "isNaN")
    {
        // the result of isNaN(NULL) is NULL in CH, but false in Spark
        const ActionsDAG::Node * arg_node = nullptr;
        if (args[0].value().has_cast())
        {
            arg_node = parseExpression(actions_dag, args[0].value().cast().input());
            const auto * res_type = arg_node->result_type.get();
            if (res_type->isNullable())
            {
                res_type = typeid_cast<const DataTypeNullable *>(res_type)->getNestedType().get();
            }
            if (isString(*res_type))
            {
                ActionsDAG::NodeRawConstPtrs cast_func_args = {arg_node};
                arg_node = toFunctionNode(actions_dag, "toFloat64OrZero", cast_func_args);
            }
            else
            {
                arg_node = parseFunctionArgument(actions_dag, function_name, args[0]);
            }
        }
        else
        {
            arg_node = parseFunctionArgument(actions_dag, function_name, args[0]);
        }

        ActionsDAG::NodeRawConstPtrs ifnull_func_args = {arg_node, addColumn(actions_dag, std::make_shared<DataTypeInt32>(), 0)};
        parsed_args.emplace_back(toFunctionNode(actions_dag, "IfNull", ifnull_func_args));
    }
    else if (function_name == "space")
    {
        // convert space function to repeat
        const ActionsDAG::Node * repeat_times_node = parseFunctionArgument(actions_dag, "repeat", args[0]);
        const ActionsDAG::Node * space_str_node = addColumn(actions_dag, std::make_shared<DataTypeString>(), " ");
        function_name = "repeat";
        parsed_args.emplace_back(space_str_node);
        parsed_args.emplace_back(repeat_times_node);
    }
    else if (function_name == "trimBothSpark" || function_name == "trimLeftSpark" || function_name == "trimRightSpark")
    {
        /// In substrait, the first arg is srcStr, the second arg is trimStr
        /// But in CH, the first arg is trimStr, the second arg is srcStr
        parseFunctionArgument(actions_dag, parsed_args, function_name, args[1]);
        parseFunctionArgument(actions_dag, parsed_args, function_name, args[0]);
    }
    else if (startsWith(function_signature, "extract:"))
    {
        /// Skip the first arg of extract in substrait
        for (int i = 1; i < args.size(); i++)
            parseFunctionArgument(actions_dag, parsed_args, function_name, args[i]);

        /// Append extra mode argument for extract(WEEK_DAY from date) or extract(DAY_OF_WEEK from date) in substrait
        if (function_name == "toDayOfWeek" || function_name == "DAYOFWEEK")
        {
            UInt8 mode = function_name == "toDayOfWeek" ? 1 : 3;
            auto mode_type = std::make_shared<DataTypeUInt8>();
            ColumnWithTypeAndName mode_col(mode_type->createColumnConst(1, mode), mode_type, getUniqueName(std::to_string(mode)));
            const auto & mode_node = actions_dag->addColumn(std::move(mode_col));
            parsed_args.emplace_back(&mode_node);
        }
    }
    else if (startsWith(function_signature, "sha2:"))
    {
        for (int i = 0; i < args.size() - 1; i++)
            parseFunctionArgument(actions_dag, parsed_args, function_name, args[i]);
    }
    else
    {
        // Default handle
        for (const auto & arg : args)
            parseFunctionArgument(actions_dag, parsed_args, function_name, arg);
    }
}

void SerializedPlanParser::parseFunctionArgument(
    ActionsDAGPtr & actions_dag,
    ActionsDAG::NodeRawConstPtrs & parsed_args,
    const std::string & function_name,
    const substrait::FunctionArgument & arg)
{
    parsed_args.emplace_back(parseFunctionArgument(actions_dag, function_name, arg));
}

const ActionsDAG::Node * SerializedPlanParser::parseFunctionArgument(
    ActionsDAGPtr & actions_dag,
    const std::string & function_name,
    const substrait::FunctionArgument & arg)
{
    const ActionsDAG::Node * res;
    if (arg.value().has_scalar_function())
    {
        std::string arg_name;
        bool keep_arg = FUNCTION_NEED_KEEP_ARGUMENTS.contains(function_name);
        parseFunctionWithDAG(arg.value(), arg_name, actions_dag, keep_arg);
        res = &actions_dag->getNodes().back();
    }
    else
    {
        res = parseExpression(actions_dag, arg.value());
    }
    return res;
}

// Convert signed integer index into unsigned integer index
std::pair<DataTypePtr, Field> SerializedPlanParser::convertStructFieldType(const DataTypePtr & type, const Field & field)
{
    // For tupelElement, field index starts from 1, but int substrait plan, it starts from 0.
#define UINT_CONVERT(type_ptr, field, type_name) \
    if ((type_ptr)->getTypeId() == TypeIndex::type_name) \
    { \
        return {std::make_shared<DataTypeU##type_name>(), static_cast<U##type_name>((field).get<type_name>()) + 1}; \
    }

    auto type_id = type->getTypeId();
    if (type_id == TypeIndex::UInt8 || type_id == TypeIndex::UInt16 || type_id == TypeIndex::UInt32
        || type_id == TypeIndex::UInt64)
    {
        return {type, field};
    }
    UINT_CONVERT(type, field, Int8)
    UINT_CONVERT(type, field, Int16)
    UINT_CONVERT(type, field, Int32)
    UINT_CONVERT(type, field, Int64)
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Not valid interger type: {}", type->getName());
#undef UINT_CONVERT
}

ActionsDAGPtr SerializedPlanParser::parseFunction(
    const Block & header,
    const substrait::Expression & rel,
    std::string & result_name,
    ActionsDAGPtr actions_dag,
    bool keep_result)
{
    if (!actions_dag)
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(header));

    parseFunctionWithDAG(rel, result_name, actions_dag, keep_result);
    return actions_dag;
}

ActionsDAGPtr SerializedPlanParser::parseFunctionOrExpression(
    const Block & header,
    const substrait::Expression & rel,
    std::string & result_name,
    ActionsDAGPtr actions_dag,
    bool keep_result)
{
    if (!actions_dag)
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(header));

    if (rel.has_scalar_function())
        parseFunctionWithDAG(rel, result_name, actions_dag, keep_result);
    else
    {
        const auto * result_node = parseExpression(actions_dag, rel);
        result_name = result_node->result_name;
    }

    return actions_dag;
}

ActionsDAGPtr SerializedPlanParser::parseArrayJoin(
    const Block & input,
    const substrait::Expression & rel,
    std::vector<String> & result_names,
    ActionsDAGPtr actions_dag,
    bool keep_result,
    bool position)
{
    if (!actions_dag)
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input));

    parseArrayJoinWithDAG(rel, result_names, actions_dag, keep_result, position);
    return actions_dag;
}

ActionsDAGPtr SerializedPlanParser::parseJsonTuple(
    const Block & input,
    const substrait::Expression & rel,
    std::vector<String> & result_names,
    ActionsDAGPtr actions_dag,
    bool keep_result,
    bool)
{
    if (!actions_dag)
    {
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input));
    }

    const auto & scalar_function = rel.scalar_function();
    auto function_signature = function_mapping.at(std::to_string(rel.scalar_function().function_reference()));
    auto function_name = getFunctionName(function_signature, scalar_function);
    auto args = scalar_function.arguments();
    if (args.size() < 2)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The function json_tuple should has at least 2 arguments.");
    }
    auto first_arg = args[0].value();
    const ActionsDAG::Node * json_expr_node = parseExpression(actions_dag, first_arg);
    std::string extract_expr = "Tuple(";
    for (int i = 1; i < args.size(); i++)
    {
        auto arg_value = args[i].value();
        if (!arg_value.has_literal())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The arguments of function {} must be string literal", function_name);
        }

        Field f = arg_value.literal().string();
        std::string s;
        if (f.tryGet(s))
        {
            extract_expr.append(s).append(" Nullable(String)");
            if (i != args.size() - 1)
            {
                extract_expr.append(",");
            }
        }
    }
    extract_expr.append(")");
    const ActionsDAG::Node * extract_expr_node = addColumn(actions_dag, std::make_shared<DataTypeString>(), extract_expr);
    auto json_extract_builder = FunctionFactory::instance().get("JSONExtract", context);
    auto json_extract_result_name = "JSONExtract(" + json_expr_node->result_name + "," + extract_expr_node->result_name + ")";
    const ActionsDAG::Node * json_extract_node
        = &actions_dag->addFunction(json_extract_builder, {json_expr_node, extract_expr_node}, json_extract_result_name);
    auto tuple_element_builder = FunctionFactory::instance().get("sparkTupleElement", context);
    auto tuple_index_type = std::make_shared<DataTypeUInt32>();
    auto add_tuple_element = [&](const ActionsDAG::Node * tuple_node, size_t i) -> const ActionsDAG::Node * {
        ColumnWithTypeAndName index_col(tuple_index_type->createColumnConst(1, i), tuple_index_type, getUniqueName(std::to_string(i)));
        const auto * index_node = &actions_dag->addColumn(std::move(index_col));
        auto result_name = "sparkTupleElement(" + tuple_node->result_name + ", " + index_node->result_name + ")";
        return &actions_dag->addFunction(tuple_element_builder, {tuple_node, index_node}, result_name);
    };
    for (int i = 1; i < args.size(); i++)
    {
        const ActionsDAG::Node * tuple_node = add_tuple_element(json_extract_node, i);
        if (keep_result)
        {
            actions_dag->addOrReplaceInOutputs(*tuple_node);
            result_names.push_back(tuple_node->result_name);
        }
    }
    return actions_dag;
}

const ActionsDAG::Node *
SerializedPlanParser::toFunctionNode(ActionsDAGPtr actions_dag, const String & function, const ActionsDAG::NodeRawConstPtrs & args)
{
    auto function_builder = FunctionFactory::instance().get(function, context);
    std::string args_name = join(args, ',');
    auto result_name = function + "(" + args_name + ")";
    const auto * function_node = &actions_dag->addFunction(function_builder, args, result_name);
    return function_node;
}

std::pair<DataTypePtr, Field> SerializedPlanParser::parseLiteral(const substrait::Expression_Literal & literal)
{
    DataTypePtr type;
    Field field;

    switch (literal.literal_type_case())
    {
        case substrait::Expression_Literal::kFp64: {
            type = std::make_shared<DataTypeFloat64>();
            field = literal.fp64();
            break;
        }
        case substrait::Expression_Literal::kFp32: {
            type = std::make_shared<DataTypeFloat32>();
            field = literal.fp32();
            break;
        }
        case substrait::Expression_Literal::kString: {
            type = std::make_shared<DataTypeString>();
            field = literal.string();
            break;
        }
        case substrait::Expression_Literal::kBinary: {
            type = std::make_shared<DataTypeString>();
            field = literal.binary();
            break;
        }
        case substrait::Expression_Literal::kI64: {
            type = std::make_shared<DataTypeInt64>();
            field = literal.i64();
            break;
        }
        case substrait::Expression_Literal::kI32: {
            type = std::make_shared<DataTypeInt32>();
            field = literal.i32();
            break;
        }
        case substrait::Expression_Literal::kBoolean: {
            type = std::make_shared<DataTypeUInt8>();
            field = literal.boolean() ? UInt8(1) : UInt8(0);
            break;
        }
        case substrait::Expression_Literal::kI16: {
            type = std::make_shared<DataTypeInt16>();
            field = literal.i16();
            break;
        }
        case substrait::Expression_Literal::kI8: {
            type = std::make_shared<DataTypeInt8>();
            field = literal.i8();
            break;
        }
        case substrait::Expression_Literal::kDate: {
            type = std::make_shared<DataTypeDate32>();
            field = literal.date();
            break;
        }
        case substrait::Expression_Literal::kTimestamp: {
            type = std::make_shared<DataTypeDateTime64>(6);
            field = DecimalField<DateTime64>(literal.timestamp(), 6);
            break;
        }
        case substrait::Expression_Literal::kDecimal: {
            UInt32 precision = literal.decimal().precision();
            UInt32 scale = literal.decimal().scale();
            const auto & bytes = literal.decimal().value();

            if (precision <= DataTypeDecimal32::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal32>(precision, scale);
                auto value = *reinterpret_cast<const Int32 *>(bytes.data());
                field = DecimalField<Decimal32>(value, scale);
            }
            else if (precision <= DataTypeDecimal64::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal64>(precision, scale);
                auto value = *reinterpret_cast<const Int64 *>(bytes.data());
                field = DecimalField<Decimal64>(value, scale);
            }
            else if (precision <= DataTypeDecimal128::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal128>(precision, scale);
                String bytes_copy(bytes);
                auto value = *reinterpret_cast<Decimal128 *>(bytes_copy.data());
                field = DecimalField<Decimal128>(value, scale);
            }
            else
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);
            break;
        }
        case substrait::Expression_Literal::kList: {
            const auto & values = literal.list().values();
            if (values.empty())
            {
                type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());
                field = Array();
                break;
            }

            DataTypePtr common_type;
            std::tie(common_type, std::ignore) = parseLiteral(values[0]);
            size_t list_len = values.size();
            Array array(list_len);
            for (int i = 0; i < static_cast<int>(list_len); ++i)
            {
                auto type_and_field = parseLiteral(values[i]);
                common_type = getLeastSupertype(DataTypes{common_type, type_and_field.first});
                array[i] = std::move(type_and_field.second);
            }

            type = std::make_shared<DataTypeArray>(common_type);
            field = std::move(array);
            break;
        }
        case substrait::Expression_Literal::kEmptyList: {
            type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());
            field = Array();
            break;
        }
        case substrait::Expression_Literal::kMap: {
            const auto & key_values = literal.map().key_values();
            if (key_values.empty())
            {
                type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeNothing>(), std::make_shared<DataTypeNothing>());
                field = Map();
                break;
            }

            const auto & first_key_value = key_values[0];

            DataTypePtr common_key_type;
            std::tie(common_key_type, std::ignore) = parseLiteral(first_key_value.key());

            DataTypePtr common_value_type;
            std::tie(common_value_type, std::ignore) = parseLiteral(first_key_value.value());

            Map map;
            map.reserve(key_values.size());
            for (const auto & key_value : key_values)
            {
                Tuple tuple(2);

                DataTypePtr key_type;
                std::tie(key_type, tuple[0]) = parseLiteral(key_value.key());
                /// Each key should has the same type
                if (!common_key_type->equals(*key_type))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Literal map key type mismatch:{} and {}",
                        common_key_type->getName(),
                        key_type->getName());

                DataTypePtr value_type;
                std::tie(value_type, tuple[1]) = parseLiteral(key_value.value());
                /// Each value should has least super type for all of them
                common_value_type = getLeastSupertype(DataTypes{common_value_type, value_type});

                map.emplace_back(std::move(tuple));
            }

            type = std::make_shared<DataTypeMap>(common_key_type, common_value_type);
            field = std::move(map);
            break;
        }
        case substrait::Expression_Literal::kEmptyMap: {
            type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeNothing>(), std::make_shared<DataTypeNothing>());
            field = Map();
            break;
        }
        case substrait::Expression_Literal::kStruct: {
            const auto & fields = literal.struct_().fields();

            DataTypes types;
            types.reserve(fields.size());
            Tuple tuple;
            tuple.reserve(fields.size());
            for (const auto & f : fields)
            {
                DataTypePtr field_type;
                Field field_value;
                std::tie(field_type, field_value) = parseLiteral(f);

                types.emplace_back(std::move(field_type));
                tuple.emplace_back(std::move(field_value));
            }

            type = std::make_shared<DataTypeTuple>(types);
            field = std::move(tuple);
            break;
        }
        case substrait::Expression_Literal::kNull: {
            type = TypeParser::parseType(literal.null());
            field = Field{};
            break;
        }
        default: {
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported spark literal type {}",
                magic_enum::enum_name(literal.literal_type_case()));
        }
    }
    return std::make_pair(std::move(type), std::move(field));
}

const ActionsDAG::Node * SerializedPlanParser::parseExpression(ActionsDAGPtr actions_dag, const substrait::Expression & rel)
{
    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            DataTypePtr type;
            Field field;
            std::tie(type, field) = parseLiteral(rel.literal());
            return addColumn(actions_dag, type, field);
        }

        case substrait::Expression::RexTypeCase::kSelection: {
            if (!rel.selection().has_direct_reference() || !rel.selection().direct_reference().has_struct_field())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can only have direct struct references in selections");

            const auto * field = actions_dag->getInputs()[rel.selection().direct_reference().struct_field().field()];
            return actions_dag->tryFindInOutputs(field->result_name);
        }

        case substrait::Expression::RexTypeCase::kCast: {
            if (!rel.cast().has_type() || !rel.cast().has_input())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Doesn't have type or input in cast node.");
            ActionsDAG::NodeRawConstPtrs args;

            const auto & input = rel.cast().input();
            args.emplace_back(parseExpression(actions_dag, input));

            const auto & substrait_type = rel.cast().type();
            const auto & input_type = args[0]->result_type;
            DataTypePtr non_nullable_input_type = removeNullable(input_type);
            DataTypePtr output_type = TypeParser::parseType(substrait_type);
            DataTypePtr non_nullable_output_type = removeNullable(output_type);

            const ActionsDAG::Node * function_node = nullptr;
            if (substrait_type.has_binary())
            {
                /// Spark cast(x as BINARY) -> CH reinterpretAsStringSpark(x)
                function_node = toFunctionNode(actions_dag, "reinterpretAsStringSpark", args);
            }
            else if (isString(non_nullable_input_type) && isDate32(non_nullable_output_type))
                function_node = toFunctionNode(actions_dag, "sparkToDate", args);
            else if (isString(non_nullable_input_type) && isDateTime64(non_nullable_output_type))
                function_node = toFunctionNode(actions_dag, "sparkToDateTime", args);
            else if (isDecimal(non_nullable_input_type) && isString(non_nullable_output_type))
            {
                /// Spark cast(x as STRING) if x is Decimal -> CH toDecimalString(x, scale)
                UInt8 scale = getDecimalScale(*non_nullable_input_type);
                args.emplace_back(addColumn(actions_dag, std::make_shared<DataTypeUInt8>(), Field(scale)));
                function_node = toFunctionNode(actions_dag, "toDecimalString", args);
            }
            else if (isFloat(non_nullable_input_type) && isInt(non_nullable_output_type))
            {
                String function_name = "sparkCastFloatTo" + non_nullable_output_type->getName();
                function_node = toFunctionNode(actions_dag, function_name, args);
            }
            else
            {
                if (isString(non_nullable_input_type) && isInt(non_nullable_output_type))
                {
                    /// Spark cast(x as INT) if x is String -> CH cast(trim(x) as INT)
                    /// Refer to https://github.com/apache/incubator-gluten/issues/4956
                    args[0] = toFunctionNode(actions_dag, "trim", {args[0]});
                }

                /// Common process
                args.emplace_back(addColumn(actions_dag, std::make_shared<DataTypeString>(), output_type->getName()));
                function_node = toFunctionNode(actions_dag, "CAST", args);
            }

            actions_dag->addOrReplaceInOutputs(*function_node);
            return function_node;
        }

        case substrait::Expression::RexTypeCase::kIfThen: {
            const auto & if_then = rel.if_then();
            FunctionOverloadResolverPtr function_ptr = nullptr;
            auto condition_nums = if_then.ifs_size();
            if (condition_nums == 1)
                function_ptr = FunctionFactory::instance().get("if", context);
            else
                function_ptr = FunctionFactory::instance().get("multiIf", context);
            ActionsDAG::NodeRawConstPtrs args;

            for (int i = 0; i < condition_nums; ++i)
            {
                const auto & ifs = if_then.ifs(i);
                const auto * if_node = parseExpression(actions_dag, ifs.if_());
                args.emplace_back(if_node);

                const auto * then_node = parseExpression(actions_dag, ifs.then());
                args.emplace_back(then_node);
            }

            const auto * else_node = parseExpression(actions_dag, if_then.else_());
            args.emplace_back(else_node);
            std::string args_name = join(args, ',');
            std::string result_name;
            if (condition_nums == 1)
                result_name = "if(" + args_name + ")";
            else
                result_name = "multiIf(" + args_name + ")";
            const auto * function_node = &actions_dag->addFunction(function_ptr, args, result_name);
            actions_dag->addOrReplaceInOutputs(*function_node);
            return function_node;
        }

        case substrait::Expression::RexTypeCase::kScalarFunction: {
            std::string result;
            return parseFunctionWithDAG(rel, result, actions_dag, false);
        }

        case substrait::Expression::RexTypeCase::kSingularOrList: {
            const auto & options = rel.singular_or_list().options();
            /// options is empty always return false
            if (options.empty())
                return addColumn(actions_dag, std::make_shared<DataTypeUInt8>(), 0);
            /// options should be literals
            if (!options[0].has_literal())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Options of SingularOrList must have literal type");

            ActionsDAG::NodeRawConstPtrs args;
            args.emplace_back(parseExpression(actions_dag, rel.singular_or_list().value()));

            bool nullable = false;
            int options_len = static_cast<int>(options.size());
            for (int i = 0; i < options_len; ++i)
            {
                if (!options[i].has_literal())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "in expression values must be the literal!");
                if (!nullable)
                    nullable = options[i].literal().has_null();
            }

            DataTypePtr elem_type;
            std::tie(elem_type, std::ignore) = parseLiteral(options[0].literal());
            elem_type = wrapNullableType(nullable, elem_type);

            MutableColumnPtr elem_column = elem_type->createColumn();
            elem_column->reserve(options_len);
            for (int i = 0; i < options_len; ++i)
            {
                auto type_and_field = parseLiteral(options[i].literal());
                auto option_type = wrapNullableType(nullable, type_and_field.first);
                if (!elem_type->equals(*option_type))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "SingularOrList options type mismatch:{} and {}",
                        elem_type->getName(),
                        option_type->getName());

                elem_column->insert(type_and_field.second);
            }

            MutableColumns elem_columns;
            elem_columns.emplace_back(std::move(elem_column));

            auto name = getUniqueName("__set");
            Block elem_block;
            elem_block.insert(ColumnWithTypeAndName(nullptr, elem_type, name));
            elem_block.setColumns(std::move(elem_columns));

            auto future_set = std::make_shared<FutureSetFromTuple>(elem_block, context->getSettingsRef());
            auto arg = ColumnSet::create(1, std::move(future_set));
            args.emplace_back(&actions_dag->addColumn(ColumnWithTypeAndName(std::move(arg), std::make_shared<DataTypeSet>(), name)));

            const auto * function_node = toFunctionNode(actions_dag, "in", args);
            actions_dag->addOrReplaceInOutputs(*function_node);
            if (nullable)
            {
                /// if sets has `null` and value not in sets
                /// In Spark: return `null`, is the standard behaviour from ANSI.(SPARK-37920)
                /// In CH: return `false`
                /// So we used if(a, b, c) cast `false` to `null` if sets has `null`
                auto type = wrapNullableType(true, function_node->result_type);
                ActionsDAG::NodeRawConstPtrs cast_args(
                    {function_node, addColumn(actions_dag, type, true), addColumn(actions_dag, type, Field())});
                auto cast = FunctionFactory::instance().get("if", context);
                function_node = toFunctionNode(actions_dag, "if", cast_args);
                actions_dag->addOrReplaceInOutputs(*function_node);
            }
            return function_node;
        }

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported spark expression type {} : {}",
                magic_enum::enum_name(rel.rex_type_case()),
                rel.DebugString());
    }
}

substrait::ReadRel::ExtensionTable SerializedPlanParser::parseExtensionTable(const std::string & split_info)
{
    substrait::ReadRel::ExtensionTable extension_table;
    google::protobuf::io::CodedInputStream coded_in(
        reinterpret_cast<const uint8_t *>(split_info.data()),
        static_cast<int>(split_info.size()));
    coded_in.SetRecursionLimit(100000);

    auto ok = extension_table.ParseFromCodedStream(&coded_in);
    if (!ok)
        throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse substrait::ReadRel::ExtensionTable from string failed");
    logDebugMessage(extension_table, "extension_table");
    return extension_table;
}

substrait::ReadRel::LocalFiles SerializedPlanParser::parseLocalFiles(const std::string & split_info)
{
    substrait::ReadRel::LocalFiles local_files;
    google::protobuf::io::CodedInputStream coded_in(
        reinterpret_cast<const uint8_t *>(split_info.data()),
        static_cast<int>(split_info.size()));
    coded_in.SetRecursionLimit(100000);

    auto ok = local_files.ParseFromCodedStream(&coded_in);
    if (!ok)
        throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse substrait::ReadRel::LocalFiles from string failed");
    logDebugMessage(local_files, "local_files");
    return local_files;
}


QueryPlanPtr SerializedPlanParser::parse(const std::string & plan)
{
    auto plan_ptr = std::make_unique<substrait::Plan>();
    /// https://stackoverflow.com/questions/52028583/getting-error-parsing-protobuf-data
    /// Parsing may fail when the number of recursive layers is large.
    /// Here, set a limit large enough to avoid this problem.
    /// Once this problem occurs, it is difficult to troubleshoot, because the pb of c++ will not provide any valid information
    google::protobuf::io::CodedInputStream coded_in(reinterpret_cast<const uint8_t *>(plan.data()), static_cast<int>(plan.size()));
    coded_in.SetRecursionLimit(100000);

    auto ok = plan_ptr->ParseFromCodedStream(&coded_in);
    if (!ok)
        throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse substrait::Plan from string failed");

    auto res = parse(std::move(plan_ptr));

    auto * logger = &Poco::Logger::get("SerializedPlanParser");
    if (logger->debug())
    {
        auto out = PlanUtil::explainPlan(*res);
        LOG_DEBUG(logger, "clickhouse plan:\n{}", out);
    }
    return res;
}

QueryPlanPtr SerializedPlanParser::parseJson(const std::string & json_plan)
{
    auto plan_ptr = std::make_unique<substrait::Plan>();
    auto s = google::protobuf::util::JsonStringToMessage(absl::string_view(json_plan.c_str()), plan_ptr.get());
    if (!s.ok())
        throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse substrait::Plan from json string failed: {}", s.ToString());
    return parse(std::move(plan_ptr));
}

SerializedPlanParser::SerializedPlanParser(const ContextPtr & context_)
    : context(context_)
{
}

ContextMutablePtr SerializedPlanParser::global_context = nullptr;

Context::ConfigurationPtr SerializedPlanParser::config = nullptr;

void SerializedPlanParser::collectJoinKeys(
    const substrait::Expression & condition,
    std::vector<std::pair<int32_t, int32_t>> & join_keys,
    int32_t right_key_start)
{
    auto condition_name = getFunctionName(
        function_mapping.at(std::to_string(condition.scalar_function().function_reference())),
        condition.scalar_function());
    if (condition_name == "and")
    {
        collectJoinKeys(condition.scalar_function().arguments(0).value(), join_keys, right_key_start);
        collectJoinKeys(condition.scalar_function().arguments(1).value(), join_keys, right_key_start);
    }
    else if (condition_name == "equals")
    {
        const auto & function = condition.scalar_function();
        auto left_key_idx = function.arguments(0).value().selection().direct_reference().struct_field().field();
        auto right_key_idx = function.arguments(1).value().selection().direct_reference().struct_field().field() - right_key_start;
        join_keys.emplace_back(std::pair(left_key_idx, right_key_idx));
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "doesn't support condition {}", condition_name);
    }
}

ActionsDAGPtr ASTParser::convertToActions(const NamesAndTypesList & name_and_types, const ASTPtr & ast)
{
    NamesAndTypesList aggregation_keys;
    ColumnNumbersList aggregation_keys_indexes_list;
    AggregationKeysInfo info(aggregation_keys, aggregation_keys_indexes_list, GroupByKind::NONE);
    SizeLimits size_limits_for_set;
    ActionsMatcher::Data visitor_data(
        context,
        size_limits_for_set,
        size_t(0),
        name_and_types,
        std::make_shared<ActionsDAG>(name_and_types),
        std::make_shared<PreparedSets>(),
        false /* no_subqueries */,
        false /* no_makeset */,
        false /* only_consts */,
        info);
    ActionsVisitor(visitor_data).visit(ast);
    return visitor_data.getActions();
}

ASTPtr ASTParser::parseToAST(const Names & names, const substrait::Expression & rel)
{
    LOG_DEBUG(&Poco::Logger::get("ASTParser"), "substrait plan:\n{}", rel.DebugString());
    if (rel.has_scalar_function())
    {
        const auto & scalar_function = rel.scalar_function();
        auto function_signature = function_mapping.at(std::to_string(rel.scalar_function().function_reference()));

        auto substrait_name = function_signature.substr(0, function_signature.find(':'));
        auto func_parser = FunctionParserFactory::instance().tryGet(substrait_name, plan_parser);
        String function_name = func_parser ? func_parser->getCHFunctionName(scalar_function)
                                           : SerializedPlanParser::getFunctionName(function_signature, scalar_function);

        ASTs ast_args;
        parseFunctionArgumentsToAST(names, scalar_function, ast_args);

        return makeASTFunction(function_name, ast_args);
    }
    else
        return parseArgumentToAST(names, rel);
}

void ASTParser::parseFunctionArgumentsToAST(
    const Names & names,
    const substrait::Expression_ScalarFunction & scalar_function,
    ASTs & ast_args)
{
    const auto & args = scalar_function.arguments();

    for (const auto & arg : args)
    {
        if (arg.value().has_scalar_function())
        {
            ast_args.emplace_back(parseToAST(names, arg.value()));
        }
        else
        {
            ast_args.emplace_back(parseArgumentToAST(names, arg.value()));
        }
    }
}

ASTPtr ASTParser::parseArgumentToAST(const Names & names, const substrait::Expression & rel)
{
    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            DataTypePtr type;
            Field field;
            std::tie(std::ignore, field) = SerializedPlanParser::parseLiteral(rel.literal());
            return std::make_shared<ASTLiteral>(field);
        }
        case substrait::Expression::RexTypeCase::kSelection: {
            if (!rel.selection().has_direct_reference() || !rel.selection().direct_reference().has_struct_field())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can only have direct struct references in selections");

            const auto field = rel.selection().direct_reference().struct_field().field();
            return std::make_shared<ASTIdentifier>(names[field]);
        }
        case substrait::Expression::RexTypeCase::kCast: {
            if (!rel.cast().has_type() || !rel.cast().has_input())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Doesn't have type or input in cast node.");

            /// Append input to asts
            ASTs args;
            args.emplace_back(parseArgumentToAST(names, rel.cast().input()));

            /// Append destination type to asts
            const auto & substrait_type = rel.cast().type();
            /// Spark cast(x as BINARY) -> CH reinterpretAsStringSpark(x)
            if (substrait_type.has_binary())
                return makeASTFunction("reinterpretAsStringSpark", args);
            else
            {
                DataTypePtr ch_type = TypeParser::parseType(substrait_type);
                args.emplace_back(std::make_shared<ASTLiteral>(ch_type->getName()));

                return makeASTFunction("CAST", args);
            }
        }
        case substrait::Expression::RexTypeCase::kIfThen: {
            const auto & if_then = rel.if_then();
            auto condition_nums = if_then.ifs_size();
            std::string ch_function_name = condition_nums == 1 ? "if" : "multiIf";
            auto function_multi_if = FunctionFactory::instance().get(ch_function_name, context);
            ASTs args;

            for (int i = 0; i < condition_nums; ++i)
            {
                const auto & ifs = if_then.ifs(i);
                auto if_node = parseArgumentToAST(names, ifs.if_());
                args.emplace_back(if_node);

                auto then_node = parseArgumentToAST(names, ifs.then());
                args.emplace_back(then_node);
            }

            auto else_node = parseArgumentToAST(names, if_then.else_());
            args.emplace_back(std::move(else_node));
            return makeASTFunction(ch_function_name, args);
        }
        case substrait::Expression::RexTypeCase::kScalarFunction: {
            return parseToAST(names, rel);
        }
        case substrait::Expression::RexTypeCase::kSingularOrList: {
            const auto & options = rel.singular_or_list().options();
            /// options is empty always return false
            if (options.empty())
                return std::make_shared<ASTLiteral>(0);
            /// options should be literals
            if (!options[0].has_literal())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Options of SingularOrList must have literal type");

            ASTs args;
            args.emplace_back(parseArgumentToAST(names, rel.singular_or_list().value()));

            bool nullable = false;
            size_t options_len = options.size();
            ASTs in_args;
            in_args.reserve(options_len);

            for (int i = 0; i < static_cast<int>(options_len); ++i)
            {
                if (!options[i].has_literal())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "in expression values must be the literal!");
                if (!nullable)
                    nullable = options[i].literal().has_null();
            }

            auto elem_type_and_field = SerializedPlanParser::parseLiteral(options[0].literal());
            DataTypePtr elem_type = wrapNullableType(nullable, elem_type_and_field.first);
            for (int i = 0; i < static_cast<int>(options_len); ++i)
            {
                auto type_and_field = SerializedPlanParser::parseLiteral(options[i].literal());
                auto option_type = wrapNullableType(nullable, type_and_field.first);
                if (!elem_type->equals(*option_type))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "SingularOrList options type mismatch:{} and {}",
                        elem_type->getName(),
                        option_type->getName());

                in_args.emplace_back(std::make_shared<ASTLiteral>(type_and_field.second));
            }
            auto array_ast = makeASTFunction("array", in_args);
            args.emplace_back(array_ast);

            auto ast = makeASTFunction("in", args);
            if (nullable)
            {
                /// if sets has `null` and value not in sets
                /// In Spark: return `null`, is the standard behaviour from ANSI.(SPARK-37920)
                /// In CH: return `false`
                /// So we used if(a, b, c) cast `false` to `null` if sets has `null`
                ast = makeASTFunction("if", ast, std::make_shared<ASTLiteral>(true), std::make_shared<ASTLiteral>(Field()));
            }

            return ast;
        }
        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Join on condition error. Unsupported spark expression type {} : {}",
                magic_enum::enum_name(rel.rex_type_case()),
                rel.DebugString());
    }
}

void SerializedPlanParser::removeNullableForRequiredColumns(const std::set<String> & require_columns, ActionsDAGPtr actions_dag)
{
    for (const auto & item : require_columns)
    {
        const auto * require_node = actions_dag->tryFindInOutputs(item);
        if (require_node)
        {
            auto function_builder = FunctionFactory::instance().get("assumeNotNull", context);
            ActionsDAG::NodeRawConstPtrs args = {require_node};
            const auto & node = actions_dag->addFunction(function_builder, args, item);
            actions_dag->addOrReplaceInOutputs(node);
        }
    }
}

void SerializedPlanParser::wrapNullable(
    const std::vector<String> & columns,
    ActionsDAGPtr actions_dag,
    std::map<std::string, std::string> & nullable_measure_names)
{
    for (const auto & item : columns)
    {
        ActionsDAG::NodeRawConstPtrs args;
        args.emplace_back(&actions_dag->findInOutputs(item));
        const auto * node = toFunctionNode(actions_dag, "toNullable", args);
        actions_dag->addOrReplaceInOutputs(*node);
        nullable_measure_names[item] = node->result_name;
    }
}

SharedContextHolder SerializedPlanParser::shared_context;

LocalExecutor::~LocalExecutor()
{
    if (context->getConfigRef().getBool("dump_pipeline", false))
        LOG_INFO(&Poco::Logger::get("LocalExecutor"), "Dump pipeline:\n{}", dumpPipeline());
    if (spark_buffer)
    {
        ch_column_to_spark_row->freeMem(spark_buffer->address, spark_buffer->size);
        spark_buffer.reset();
    }
}


void LocalExecutor::execute(QueryPlanPtr query_plan)
{
    Stopwatch stopwatch;

    const Settings & settings = context->getSettingsRef();
    current_query_plan = std::move(query_plan);
    auto * logger = &Poco::Logger::get("LocalExecutor");

    QueryPriorities priorities;
    auto query_status = std::make_shared<QueryStatus>(
        context,
        "",
        context->getClientInfo(),
        priorities.insert(static_cast<int>(settings.priority)),
        CurrentThread::getGroup(),
        IAST::QueryKind::Select,
        settings,
        0);

    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = settings.query_plan_enable_optimizations};
    auto pipeline_builder = current_query_plan->buildQueryPipeline(
        optimization_settings,
        BuildQueryPipelineSettings{
            .actions_settings
            = ExpressionActionsSettings{.can_compile_expressions = true, .min_count_to_compile_expression = 3,
                                        .compile_expressions = CompileExpressions::yes},
            .process_list_element = query_status});

    LOG_DEBUG(logger, "clickhouse plan after optimization:\n{}", PlanUtil::explainPlan(*current_query_plan));
    query_pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));
    LOG_DEBUG(logger, "clickhouse pipeline:\n{}", QueryPipelineUtil::explainPipeline(query_pipeline));
    auto t_pipeline = stopwatch.elapsedMicroseconds();

    executor = std::make_unique<PullingPipelineExecutor>(query_pipeline);
    auto t_executor = stopwatch.elapsedMicroseconds() - t_pipeline;
    stopwatch.stop();
    LOG_INFO(
        logger,
        "build pipeline {} ms; create executor {} ms;",
        t_pipeline / 1000.0,
        t_executor / 1000.0);

    header = current_query_plan->getCurrentDataStream().header.cloneEmpty();
    ch_column_to_spark_row = std::make_unique<CHColumnToSparkRow>();
}

std::unique_ptr<SparkRowInfo> LocalExecutor::writeBlockToSparkRow(Block & block)
{
    return ch_column_to_spark_row->convertCHColumnToSparkRow(block);
}

bool LocalExecutor::hasNext()
{
    bool has_next;
    try
    {
        size_t columns = currentBlock().columns();
        if (columns == 0 || isConsumed())
        {
            auto empty_block = header.cloneEmpty();
            setCurrentBlock(empty_block);
            has_next = executor->pull(currentBlock());
            produce();
        }
        else
        {
            has_next = true;
        }
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("LocalExecutor"),
            "LocalExecutor run query plan failed with message: {}. Plan Explained: \n{}",
            e.message(),
            PlanUtil::explainPlan(*current_query_plan));
        throw;
    }
    return has_next;
}

SparkRowInfoPtr LocalExecutor::next()
{
    checkNextValid();
    SparkRowInfoPtr row_info = writeBlockToSparkRow(currentBlock());
    consume();
    if (spark_buffer)
    {
        ch_column_to_spark_row->freeMem(spark_buffer->address, spark_buffer->size);
        spark_buffer.reset();
    }
    spark_buffer = std::make_unique<SparkBuffer>();
    spark_buffer->address = row_info->getBufferAddress();
    spark_buffer->size = row_info->getTotalBytes();
    return row_info;
}

Block * LocalExecutor::nextColumnar()
{
    checkNextValid();
    Block * columnar_batch;
    if (currentBlock().columns() > 0)
    {
        columnar_batch = &currentBlock();
    }
    else
    {
        auto empty_block = header.cloneEmpty();
        setCurrentBlock(empty_block);
        columnar_batch = &currentBlock();
    }
    consume();
    return columnar_batch;
}

Block & LocalExecutor::getHeader()
{
    return header;
}

LocalExecutor::LocalExecutor(QueryContext & _query_context, ContextPtr context_)
    : query_context(_query_context)
    , context(context_)
{
}

std::string LocalExecutor::dumpPipeline()
{
    const auto & processors = query_pipeline.getProcessors();
    for (auto & processor : processors)
    {
        WriteBufferFromOwnString buffer;
        auto data_stats = processor->getProcessorDataStats();
        buffer << "(";
        buffer << "\nexcution time: " << processor->getElapsedUs() << " us.";
        buffer << "\ninput wait time: " << processor->getInputWaitElapsedUs() << " us.";
        buffer << "\noutput wait time: " << processor->getOutputWaitElapsedUs() << " us.";
        buffer << "\ninput rows: " << data_stats.input_rows;
        buffer << "\ninput bytes: " << data_stats.input_bytes;
        buffer << "\noutput rows: " << data_stats.output_rows;
        buffer << "\noutput bytes: " << data_stats.output_bytes;
        buffer << ")";
        processor->setDescription(buffer.str());
    }
    WriteBufferFromOwnString out;
    printPipeline(processors, out);
    return out.str();
}

NonNullableColumnsResolver::NonNullableColumnsResolver(
    const Block & header_,
    SerializedPlanParser & parser_,
    const substrait::Expression & cond_rel_)
    : header(header_)
    , parser(parser_)
    , cond_rel(cond_rel_)
{
}

// make it simple at present, if the condition contains or, return empty for both side.
std::set<String> NonNullableColumnsResolver::resolve()
{
    collected_columns.clear();
    visit(cond_rel);
    return collected_columns;
}

// TODO: make it the same as spark, it's too simple at present.
void NonNullableColumnsResolver::visit(const substrait::Expression & expr)
{
    if (!expr.has_scalar_function())
        return;

    const auto & scalar_function = expr.scalar_function();
    auto function_signature = parser.function_mapping.at(std::to_string(scalar_function.function_reference()));
    auto function_name = safeGetFunctionName(function_signature, scalar_function);

    // Only some special functions are used to judge whether the column is non-nullable.
    if (function_name == "and")
    {
        visit(scalar_function.arguments(0).value());
        visit(scalar_function.arguments(1).value());
    }
    else if (function_name == "greaterOrEquals" || function_name == "greater")
    {
        // If it's the case, a > x, what ever x or a is, a and x are non-nullable.
        // a or x may be a column, or a simple expression like plus etc.
        visitNonNullable(scalar_function.arguments(0).value());
        visitNonNullable(scalar_function.arguments(1).value());
    }
    else if (function_name == "lessOrEquals" || function_name == "less")
    {
        // same as gt, gte.
        visitNonNullable(scalar_function.arguments(0).value());
        visitNonNullable(scalar_function.arguments(1).value());
    }
    else if (function_name == "isNotNull")
    {
        visitNonNullable(scalar_function.arguments(0).value());
    }
    // else do nothing
}

void NonNullableColumnsResolver::visitNonNullable(const substrait::Expression & expr)
{
    if (expr.has_scalar_function())
    {
        const auto & scalar_function = expr.scalar_function();
        auto function_signature = parser.function_mapping.at(std::to_string(scalar_function.function_reference()));
        auto function_name = safeGetFunctionName(function_signature, scalar_function);
        if (function_name == "plus" || function_name == "minus" || function_name == "multiply" || function_name == "divide")
        {
            visitNonNullable(scalar_function.arguments(0).value());
            visitNonNullable(scalar_function.arguments(1).value());
        }
    }
    else if (expr.has_selection())
    {
        const auto & selection = expr.selection();
        auto column_pos = selection.direct_reference().struct_field().field();
        auto column_name = header.getByPosition(column_pos).name;
        collected_columns.insert(column_name);
    }
    // else, do nothing.
}

std::string NonNullableColumnsResolver::safeGetFunctionName(
    const std::string & function_signature,
    const substrait::Expression_ScalarFunction & function)
{
    try
    {
        return parser.getFunctionName(function_signature, function);
    }
    catch (const Exception &)
    {
        return "";
    }
}
}
