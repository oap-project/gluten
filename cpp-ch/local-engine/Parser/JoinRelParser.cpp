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
#include "JoinRelParser.h"
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Join/BroadCastJoinBuilder.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Parser/SerializedPlanParser.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <google/protobuf/wrappers.pb.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int BAD_ARGUMENTS;
}
}

struct JoinOptimizationInfo
{
    bool is_broadcast = false;
    bool is_smj = false;
    bool is_null_aware_anti_join = false;
    bool is_existence_join = false;
    std::string storage_join_key;
};

using namespace DB;

JoinOptimizationInfo parseJoinOptimizationInfo(const substrait::JoinRel & join)
{
    google::protobuf::StringValue optimization;
    optimization.ParseFromString(join.advanced_extension().optimization().value());
    JoinOptimizationInfo info;
    if (optimization.value().contains("isBHJ="))
    {
        ReadBufferFromString in(optimization.value());
        assertString("JoinParameters:", in);
        assertString("isBHJ=", in);
        readBoolText(info.is_broadcast, in);
        assertChar('\n', in);
        if (info.is_broadcast)
        {
            assertString("isNullAwareAntiJoin=", in);
            readBoolText(info.is_null_aware_anti_join, in);
            assertChar('\n', in);
            assertString("buildHashTableId=", in);
            readString(info.storage_join_key, in);
            assertChar('\n', in);
        }
    }
    else
    {
        ReadBufferFromString in(optimization.value());
        assertString("JoinParameters:", in);
        assertString("isSMJ=", in);
        readBoolText(info.is_smj, in);
        assertChar('\n', in);
        if (info.is_smj)
        {
            assertString("isNullAwareAntiJoin=", in);
            readBoolText(info.is_null_aware_anti_join, in);
            assertChar('\n', in);
            assertString("isExistenceJoin=", in);
            readBoolText(info.is_existence_join, in);
            assertChar('\n', in);
        }
    }
    return info;
}


void reorderJoinOutput(DB::QueryPlan & plan, DB::Names cols)
{
    ActionsDAGPtr project = std::make_shared<ActionsDAG>(plan.getCurrentDataStream().header.getNamesAndTypesList());
    NamesWithAliases project_cols;
    for (const auto & col : cols)
    {
        project_cols.emplace_back(NameWithAlias(col, col));
    }
    project->project(project_cols);
    QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), project);
    project_step->setStepDescription("Reorder Join Output");
    plan.addStep(std::move(project_step));
}

namespace local_engine
{

std::pair<DB::JoinKind, DB::JoinStrictness> getJoinKindAndStrictness(substrait::JoinRel_JoinType join_type)
{
    switch (join_type)
    {
        case substrait::JoinRel_JoinType_JOIN_TYPE_INNER:
            return {DB::JoinKind::Inner, DB::JoinStrictness::All};
        case substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
            return {DB::JoinKind::Left, DB::JoinStrictness::Semi};
        case substrait::JoinRel_JoinType_JOIN_TYPE_ANTI:
            return {DB::JoinKind::Left, DB::JoinStrictness::Anti};
        case substrait::JoinRel_JoinType_JOIN_TYPE_LEFT:
            return {DB::JoinKind::Left, DB::JoinStrictness::All};
        case substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT:
            return {DB::JoinKind::Right, DB::JoinStrictness::All};
        case substrait::JoinRel_JoinType_JOIN_TYPE_OUTER:
            return {DB::JoinKind::Full, DB::JoinStrictness::All};
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported join type {}.", magic_enum::enum_name(join_type));
    }
}
std::shared_ptr<DB::TableJoin> createDefaultTableJoin(substrait::JoinRel_JoinType join_type)
{
    auto & global_context = SerializedPlanParser::global_context;
    auto table_join = std::make_shared<TableJoin>(global_context->getSettings(), global_context->getGlobalTemporaryVolume());

    std::pair<DB::JoinKind, DB::JoinStrictness> kind_and_strictness = getJoinKindAndStrictness(join_type);
    table_join->setKind(kind_and_strictness.first);
    table_join->setStrictness(kind_and_strictness.second);
    return table_join;
}

JoinRelParser::JoinRelParser(SerializedPlanParser * plan_paser_)
    : RelParser(plan_paser_)
    , function_mapping(plan_paser_->function_mapping)
    , context(plan_paser_->context)
    , extra_plan_holder(plan_paser_->extra_plan_holder)
{
}

DB::QueryPlanPtr
JoinRelParser::parse(DB::QueryPlanPtr /*query_plan*/, const substrait::Rel & /*rel*/, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "join node has 2 inputs, can't call parse().");
}

const substrait::Rel & JoinRelParser::getSingleInput(const substrait::Rel & /*rel*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "join node has 2 inputs, can't call getSingleInput().");
}

DB::QueryPlanPtr JoinRelParser::parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    const auto & join = rel.join();
    if (!join.has_left() || !join.has_right())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "left table or right table is missing.");
    }

    rel_stack.push_back(&rel);
    auto left_plan = getPlanParser()->parseOp(join.left(), rel_stack);
    auto right_plan = getPlanParser()->parseOp(join.right(), rel_stack);
    rel_stack.pop_back();

    return parseJoin(join, std::move(left_plan), std::move(right_plan));
}

DB::QueryPlanPtr JoinRelParser::parseJoin(const substrait::JoinRel & join, DB::QueryPlanPtr left, DB::QueryPlanPtr right)
{
    auto join_opt_info = parseJoinOptimizationInfo(join);
    auto storage_join = join_opt_info.is_broadcast ? BroadCastJoinBuilder::getJoin(join_opt_info.storage_join_key) : nullptr;

    if (storage_join)
    {
        ActionsDAGPtr project = ActionsDAG::makeConvertingActions(
            right->getCurrentDataStream().header.getColumnsWithTypeAndName(),
            storage_join->getRightSampleBlock().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);

        if (project)
        {
            QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right->getCurrentDataStream(), project);
            project_step->setStepDescription("Rename Broadcast Table Name");
            steps.emplace_back(project_step.get());
            right->addStep(std::move(project_step));
        }
    }

    auto table_join = createDefaultTableJoin(join.type());
    addConvertStep(*table_join, *left, *right);
    Names after_join_names;
    auto left_names = left->getCurrentDataStream().header.getNames();
    after_join_names.insert(after_join_names.end(), left_names.begin(), left_names.end());
    auto right_name = table_join->columnsFromJoinedTable().getNames();
    after_join_names.insert(after_join_names.end(), right_name.begin(), right_name.end());
    bool add_filter_step = tryAddPushDownFilter(*table_join, join, *left, *right, table_join->columnsFromJoinedTable(), after_join_names);

    QueryPlanPtr query_plan;
    if (storage_join)
    {
        auto broadcast_hash_join = storage_join->getJoinLocked(table_join, context);
        QueryPlanStepPtr join_step = std::make_unique<FilledJoinStep>(left->getCurrentDataStream(), broadcast_hash_join, 8192);

        join_step->setStepDescription("STORAGE_JOIN");
        steps.emplace_back(join_step.get());
        left->addStep(std::move(join_step));
        query_plan = std::move(left);
        /// hold right plan for profile
        extra_plan_holder.emplace_back(std::move(right));
    }
    else if (join_opt_info.is_smj)
    {
        JoinPtr smj_join = std::make_shared<FullSortingMergeJoin>(table_join, right->getCurrentDataStream().header.cloneEmpty());
        MultiEnum<DB::JoinAlgorithm> join_algorithm = context->getSettingsRef().join_algorithm;
        QueryPlanStepPtr join_step
                 = std::make_unique<DB::JoinStep>(left->getCurrentDataStream(), right->getCurrentDataStream(), smj_join, 8192, 1, false);

        join_step->setStepDescription("SORT_MERGE_JOIN");
        steps.emplace_back(join_step.get());
        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::move(left));
        plans.emplace_back(std::move(right));

        query_plan = std::make_unique<QueryPlan>();
        query_plan->unitePlans(std::move(join_step), {std::move(plans)});
    }
    else
    {
        /// TODO: make grace hash join be the default hash join algorithm.
        ///
        /// Following is some configuration for grace hash join.
        /// - spark.gluten.sql.columnar.backend.ch.runtime_settings.join_algorithm=grace_hash. This will
        ///   enable grace hash join.
        /// - spark.gluten.sql.columnar.backend.ch.runtime_settings.max_bytes_in_join=3145728. This setup
        ///   the memory limitation fro grace hash join. If the memory consumption exceeds the limitation,
        ///   data will be spilled to disk. Don't set the limitation too small, otherwise the buckets number
        ///   will be too large and the performance will be bad.
        JoinPtr hash_join = nullptr;
        MultiEnum<DB::JoinAlgorithm> join_algorithm = context->getSettingsRef().join_algorithm;
        if (join_algorithm.isSet(DB::JoinAlgorithm::GRACE_HASH))
        {
            hash_join = std::make_shared<GraceHashJoin>(
                context,
                table_join,
                left->getCurrentDataStream().header,
                right->getCurrentDataStream().header,
                context->getTempDataOnDisk());
        }
        else
        {
            hash_join = std::make_shared<HashJoin>(table_join, right->getCurrentDataStream().header.cloneEmpty());
        }
        QueryPlanStepPtr join_step
            = std::make_unique<DB::JoinStep>(left->getCurrentDataStream(), right->getCurrentDataStream(), hash_join, 8192, 1, false);

        join_step->setStepDescription("HASH_JOIN");
        steps.emplace_back(join_step.get());
        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::move(left));
        plans.emplace_back(std::move(right));

        query_plan = std::make_unique<QueryPlan>();
        query_plan->unitePlans(std::move(join_step), {std::move(plans)});
    }
    reorderJoinOutput(*query_plan, after_join_names);

    if (add_filter_step)
    {
        addPostFilter(*query_plan, join);
    }
    return query_plan;
}

void JoinRelParser::addConvertStep(TableJoin & table_join, DB::QueryPlan & left, DB::QueryPlan & right)
{
    NameSet left_columns_set;
    for (const auto & col : left.getCurrentDataStream().header.getNames())
    {
        left_columns_set.emplace(col);
    }
    table_join.setColumnsFromJoinedTable(
        right.getCurrentDataStream().header.getNamesAndTypesList(), left_columns_set, getUniqueName("right") + ".");

    // fix right table key duplicate
    NamesWithAliases right_table_alias;
    for (size_t idx = 0; idx < table_join.columnsFromJoinedTable().size(); idx++)
    {
        auto origin_name = right.getCurrentDataStream().header.getByPosition(idx).name;
        auto dedup_name = table_join.columnsFromJoinedTable().getNames().at(idx);
        if (origin_name != dedup_name)
        {
            right_table_alias.emplace_back(NameWithAlias(origin_name, dedup_name));
        }
    }
    if (!right_table_alias.empty())
    {
        ActionsDAGPtr rename_dag = std::make_shared<ActionsDAG>(right.getCurrentDataStream().header.getNamesAndTypesList());
        auto original_right_columns = right.getCurrentDataStream().header;
        for (const auto & column_alias : right_table_alias)
        {
            if (original_right_columns.has(column_alias.first))
            {
                auto pos = original_right_columns.getPositionByName(column_alias.first);
                const auto & alias = rename_dag->addAlias(*rename_dag->getInputs()[pos], column_alias.second);
                rename_dag->getOutputs()[pos] = &alias;
            }
        }
        rename_dag->projectInput();
        QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right.getCurrentDataStream(), rename_dag);
        project_step->setStepDescription("Right Table Rename");
        steps.emplace_back(project_step.get());
        right.addStep(std::move(project_step));
    }

    for (const auto & column : table_join.columnsFromJoinedTable())
    {
        table_join.addJoinedColumn(column);
    }
    ActionsDAGPtr left_convert_actions = nullptr;
    ActionsDAGPtr right_convert_actions = nullptr;
    std::tie(left_convert_actions, right_convert_actions) = table_join.createConvertingActions(
        left.getCurrentDataStream().header.getColumnsWithTypeAndName(), right.getCurrentDataStream().header.getColumnsWithTypeAndName());

    if (right_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right.getCurrentDataStream(), right_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        steps.emplace_back(converting_step.get());
        right.addStep(std::move(converting_step));
    }

    if (left_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(left.getCurrentDataStream(), left_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        steps.emplace_back(converting_step.get());
        left.addStep(std::move(converting_step));
    }
}

void JoinRelParser::addPostFilter(DB::QueryPlan & query_plan, const substrait::JoinRel & join)
{
    std::string filter_name;
    auto actions_dag = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());
    if (!join.post_join_filter().has_scalar_function())
    {
	// It may be singular_or_list
        auto * in_node = getPlanParser()->parseExpression(actions_dag, join.post_join_filter());
        filter_name = in_node->result_name;
    }
    else
    {
        getPlanParser()->parseFunction(query_plan.getCurrentDataStream().header, join.post_join_filter(), filter_name, actions_dag, true);
    }
    auto filter_step = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(), actions_dag, filter_name, true);
    filter_step->setStepDescription("Post Join Filter");
    steps.emplace_back(filter_step.get());
    query_plan.addStep(std::move(filter_step));
}

bool JoinRelParser::tryAddPushDownFilter(
    TableJoin & table_join,
    const substrait::JoinRel & join,
    DB::QueryPlan & left,
    DB::QueryPlan & right,
    const NamesAndTypesList & alias_right,
    const Names & names)
{
    try
    {
        ASTParser astParser(context, function_mapping, getPlanParser());
        ASTs args;

        if (join.has_expression())
        {
            args.emplace_back(astParser.parseToAST(names, join.expression()));
        }

        if (join.has_post_join_filter())
        {
            args.emplace_back(astParser.parseToAST(names, join.post_join_filter()));
        }

        if (args.empty())
            return false;

        ASTPtr ast = args.size() == 1 ? args.back() : makeASTFunction("and", args);

        bool is_asof = (table_join.strictness() == JoinStrictness::Asof);

        Aliases aliases;
        DatabaseAndTableWithAlias left_table_name;
        DatabaseAndTableWithAlias right_table_name;
        TableWithColumnNamesAndTypes left_table(left_table_name, left.getCurrentDataStream().header.getNamesAndTypesList());
        TableWithColumnNamesAndTypes right_table(right_table_name, alias_right);

        CollectJoinOnKeysVisitor::Data data{table_join, left_table, right_table, aliases, is_asof};
        if (auto * or_func = ast->as<ASTFunction>(); or_func && or_func->name == "or")
        {
            for (auto & disjunct : or_func->arguments->children)
            {
                table_join.addDisjunct();
                CollectJoinOnKeysVisitor(data).visit(disjunct);
            }
            assert(table_join.getClauses().size() == or_func->arguments->children.size());
        }
        else
        {
            table_join.addDisjunct();
            CollectJoinOnKeysVisitor(data).visit(ast);
            assert(table_join.oneDisjunct());
        }

        if (join.has_post_join_filter())
        {
            auto left_keys = table_join.leftKeysList();
            auto right_keys = table_join.rightKeysList();
            if (!left_keys->children.empty())
            {
                auto actions = astParser.convertToActions(left.getCurrentDataStream().header.getNamesAndTypesList(), left_keys);
                QueryPlanStepPtr before_join_step = std::make_unique<ExpressionStep>(left.getCurrentDataStream(), actions);
                before_join_step->setStepDescription("Before JOIN LEFT");
                steps.emplace_back(before_join_step.get());
                left.addStep(std::move(before_join_step));
            }

            if (!right_keys->children.empty())
            {
                auto actions = astParser.convertToActions(right.getCurrentDataStream().header.getNamesAndTypesList(), right_keys);
                QueryPlanStepPtr before_join_step = std::make_unique<ExpressionStep>(right.getCurrentDataStream(), actions);
                before_join_step->setStepDescription("Before JOIN RIGHT");
                steps.emplace_back(before_join_step.get());
                right.addStep(std::move(before_join_step));
            }
        }
    }
    // if ch does not support the join type or join conditions, it will throw an exception like 'not support'.
    catch (Poco::Exception & e)
    {
        // CH not support join condition has 'or' and has different table in each side.
        // But in inner join, we could execute join condition after join. so we have add filter step
        if (e.code() == ErrorCodes::INVALID_JOIN_ON_EXPRESSION && table_join.kind() == DB::JoinKind::Inner)
        {
            return true;
        }
        else
        {
            throw;
        }
    }
    return false;
}

void registerJoinRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_paser) { return std::make_shared<JoinRelParser>(plan_paser); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kJoin, builder);
}

}
