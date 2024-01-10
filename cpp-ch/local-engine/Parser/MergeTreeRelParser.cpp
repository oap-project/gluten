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


#include <google/protobuf/wrappers.pb.h>

#include <Parser/FunctionParser.h>
#include <Parser/TypeParser.h>
#include <Storages/StorageMergeTreeFactory.h>
#include <Common/CHUtil.h>
#include <Common/MergeTreeTool.h>

#include "MergeTreeRelParser.h"

#include "Processors/QueryPlan/ExpressionStep.h"


namespace DB
{
namespace ErrorCodes
{
extern const int NO_SUCH_DATA_PART;
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FUNCTION;
extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
using namespace DB;

static NameToIndexMap fillNamesPositions(const Names & names)
{
    NameToIndexMap names_positions;

    for (size_t position = 0; position < names.size(); ++position)
    {
        const auto & name = names[position];
        names_positions[name] = position;
    }

    return names_positions;
}


/// Find minimal position of any of the column in primary key.
static Int64 findMinPosition(const NameSet & condition_table_columns, const NameToIndexMap & primary_key_positions)
{
    Int64 min_position = std::numeric_limits<Int64>::max() - 1;

    for (const auto & column : condition_table_columns)
    {
        auto it = primary_key_positions.find(column);
        if (it != primary_key_positions.end())
            min_position = std::min(min_position, static_cast<Int64>(it->second));
    }

    return min_position;
}

DB::QueryPlanPtr
MergeTreeRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel_, std::list<const substrait::Rel *> & rel_stack_)
{
    const auto & rel = rel_.read();
    assert(rel.has_extension_table());
    google::protobuf::StringValue table;
    table.ParseFromString(rel.extension_table().detail().value());
    auto merge_tree_table = local_engine::parseMergeTreeTableString(table.value());
    DB::Block header;
    if (rel.has_base_schema() && rel.base_schema().names_size())
    {
        header = TypeParser::buildBlockFromNamedStruct(rel.base_schema());
    }
    else
    {
        // For count(*) case, there will be an empty base_schema, so we try to read at least once column
        auto all_parts_dir = MergeTreeUtil::getAllMergeTreeParts(std::filesystem::path("/") / merge_tree_table.relative_path);
        if (all_parts_dir.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Empty mergetree directory: {}", merge_tree_table.relative_path);
        auto part_names_types_list = MergeTreeUtil::getSchemaFromMergeTreePart(all_parts_dir[0]);
        NamesAndTypesList one_column_name_type;
        one_column_name_type.push_back(part_names_types_list.front());
        header = BlockUtil::buildHeader(one_column_name_type);
        LOG_DEBUG(&Poco::Logger::get("SerializedPlanParser"), "Try to read ({}) instead of empty header", header.dumpNames());
    }
    const auto names_and_types_list = header.getNamesAndTypesList();
    auto storage_factory = StorageMergeTreeFactory::instance();

    auto storage = storage_factory.getStorage(
        StorageID(merge_tree_table.database, merge_tree_table.table),
        ColumnsDescription(),
        [&]() -> CustomStorageMergeTreePtr
        {
            auto custom_storage_merge_tree = std::make_shared<CustomStorageMergeTree>(
                StorageID(merge_tree_table.database, merge_tree_table.table),
                merge_tree_table.relative_path,
                *buildMetaData(names_and_types_list, context),
                false,
                global_context,
                "",
                MergeTreeData::MergingParams(),
                buildMergeTreeSettings());
            custom_storage_merge_tree->loadDataParts(false, std::nullopt);
            return custom_storage_merge_tree;
        });
    auto metadata = storage->getInMemoryMetadataPtr();
    query_context.metadata = metadata;
    primary_key_names_positions = fillNamesPositions(metadata->primary_key.column_names);

    for (const auto & [name, sizes] : storage->getColumnSizes())
    {
        auto name_and_type = names_and_types_list.tryGetByName(name);
        if (name_and_type.has_value() && name_and_type.value().type->getFamilyName() == "String")
            column_sizes[name] = sizes.data_compressed * 1; // punish string column as it's more costly to read & compare
        else
            column_sizes[name] = sizes.data_compressed;
    }

    query_context.storage_snapshot = std::make_shared<StorageSnapshot>(*storage, metadata);
    query_context.custom_storage_merge_tree = storage;
    auto query_info = buildQueryInfo(names_and_types_list);

    std::set<String> non_nullable_columns;
    if (rel.has_filter())
    {
        NonNullableColumnsResolver non_nullable_columns_resolver(header, *getPlanParser(), rel.filter());
        non_nullable_columns = non_nullable_columns_resolver.resolve();
        query_info->prewhere_info = parsePreWhereInfo(rel.filter(), header, rel_stack_);
    }
    // std::cout << "prewhere is:" << query_info->prewhere_info->dump() << std::endl;
    auto data_parts = query_context.custom_storage_merge_tree->getAllDataPartsVector();
    int min_block = merge_tree_table.min_block;
    int max_block = merge_tree_table.max_block;
    MergeTreeData::DataPartsVector selected_parts;
    std::copy_if(
        std::begin(data_parts),
        std::end(data_parts),
        std::inserter(selected_parts, std::begin(selected_parts)),
        [min_block, max_block](MergeTreeData::DataPartPtr part)
        {
            return part->info.min_block >= min_block && part->info.max_block < max_block;
        });
    if (selected_parts.empty())
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "part {} to {} not found.", min_block, max_block);
    auto read_step = query_context.custom_storage_merge_tree->reader.readFromParts(
        selected_parts,
        /* alter_conversions = */
        {},
        names_and_types_list.getNames(),
        query_context.storage_snapshot,
        *query_info,
        context,
        context->getSettingsRef().max_block_size,
        1,
        nullptr,
        nullptr,
        false);

    steps.emplace_back(read_step.get());
    query_plan->addStep(std::move(read_step));

    if (rel.has_filter())
    {
        ActionsDAGPtr project = std::make_shared<ActionsDAG>(query_plan->getCurrentDataStream().header.getNamesAndTypesList());
        NamesWithAliases project_cols;
        for (const auto & col : names_and_types_list)
        {
            project_cols.emplace_back(NameWithAlias(col.name, col.name));
        }
        project->project(project_cols);
        QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), project);
        project_step->setStepDescription("Reorder ReadFromMergeTree Output");
        query_plan->addStep(std::move(project_step));
    }


    if (!non_nullable_columns.empty())
    {
        auto input_header = query_plan->getCurrentDataStream().header;
        std::erase_if(non_nullable_columns, [input_header](auto item) -> bool { return !input_header.has(item); });
        auto * remove_null_step = getPlanParser()->addRemoveNullableStep(*query_plan, non_nullable_columns);
        if (remove_null_step)
            steps.emplace_back(remove_null_step);
    }
    return query_plan;
}

PrewhereInfoPtr MergeTreeRelParser::parsePreWhereInfo(
    const substrait::Expression & rel,
    Block & input,
    std::list<const substrait::Rel *> & rel_stack_)
{
    std::string filter_name;
    auto prewhere_info = std::make_shared<PrewhereInfo>();
    prewhere_info->prewhere_actions = optimizePrewhereAction(rel, filter_name, input);
    prewhere_info->prewhere_column_name = filter_name;
    prewhere_info->need_filter = true;
    prewhere_info->remove_prewhere_column = true;
    prewhere_info->prewhere_actions->projectInput(false);

    if (rel_stack_.size() > 0 && rel_stack_.back()->has_project())
    {
        std::set<int> used_cols;
        getUsedColumnsInBaseSchema(used_cols, *rel_stack_.back(), input.columns());
        auto name_and_types = input.getNamesAndTypes();
        for (size_t i = 0; i < input.columns(); i++)
        {
            if (used_cols.find(i) != used_cols.end())
            {
                prewhere_info->prewhere_actions->tryRestoreColumn(name_and_types.at(i).name);
            }
            else
            {
                auto & type = name_and_types.at(i).type;
                const auto & dummy_node = prewhere_info->prewhere_actions->addColumn(
                    ColumnWithTypeAndName(
                        type->createColumnConstWithDefaultValue(1),
                        type,
                        name_and_types.at(i).name));
                prewhere_info->prewhere_actions->addOrReplaceInOutputs(dummy_node);
            }
        }
    }
    else
    {
        for (const auto & name : input.getNames())
        {
            prewhere_info->prewhere_actions->tryRestoreColumn(name);
        }
    }

    // In substrait plan, columns are referenced by cardinal, not name.
    // So we must add dummy column to make sure the prewhere_actions outputs,
    // which will in turn affect ReadFromMergeTree's output in MergeTreeSelectProcessor::applyPrewhereActions,
    // has the same number of columns as the input.
//    for (const auto & name_and_type : input.getNamesAndTypes())
//    {
//        auto & name = name_and_type.name;
//        auto & type = name_and_type.type;

        // if (name == "l_shipdate" || name == "l_receiptdate")
        // {
        //     const auto & dummy_node = prewhere_info->prewhere_actions->addColumn(
        //         ColumnWithTypeAndName(
        //             type->createColumnConstWithDefaultValue(1),
        //             std::make_shared<DataTypeInt8>(),
        //             getUniqueName("_dummy_" + name)));
        //     prewhere_info->prewhere_actions->addOrReplaceInOutputs(dummy_node);
        // }
        // else
        //     prewhere_info->prewhere_actions->tryRestoreColumn(name);

        //
        // if (name == "l_receiptdate" || name == "l_receiptdate")
        // {
        //     const auto & dummy_node = prewhere_info->prewhere_actions->addColumn(
        //         ColumnWithTypeAndName(
        //             type->createColumnConstWithDefaultValue(1),
        //             std::make_shared<DataTypeInt8>(),
        //             getUniqueName("_dummy_" + name)));
        //     prewhere_info->prewhere_actions->addOrReplaceInOutputs(dummy_node);
        // }
        // else
        // {
        //     const auto & dummy_node = prewhere_info->prewhere_actions->addColumn(
        //        ColumnWithTypeAndName(
        //            type->createColumn(), type, name));
        //     prewhere_info->prewhere_actions->addOrReplaceInOutputs(dummy_node);
        // }

        // prewhere_info->prewhere_actions->tryRestoreColumn(name);
//    }

    return prewhere_info;
}

DB::ActionsDAGPtr MergeTreeRelParser::optimizePrewhereAction(const substrait::Expression & rel, std::string & filter_name, Block & block)
{
    Conditions res;
    std::set<Int64> pk_positions;
    analyzeExpressions(res, rel, pk_positions, block);

    Int64 min_valid_pk_pos = -1;
    for (auto pk_pos : pk_positions)
    {
        if (pk_pos != min_valid_pk_pos + 1)
            break;
        min_valid_pk_pos = pk_pos;
    }

    for (auto & cond : res)
        if (cond.min_position_in_primary_key > min_valid_pk_pos)
            cond.min_position_in_primary_key = std::numeric_limits<Int64>::max() - 1;

    // filter less size column first
    res.sort();
    auto filter_action = std::make_shared<ActionsDAG>(block.getNamesAndTypesList());

    if (res.size() == 1)
    {
        parseToAction(filter_action, res.back().node, filter_name);
    }
    else
    {
        DB::ActionsDAG::NodeRawConstPtrs args;

        for (Condition cond : res)
        {
            String ignore;
            parseToAction(filter_action, cond.node, ignore);
            args.emplace_back(&filter_action->getNodes().back());
        }

        auto function_builder = FunctionFactory::instance().get("and", context);
        std::string args_name = join(args, ',');
        filter_name = +"and(" + args_name + ")";
        const auto * and_function = &filter_action->addFunction(function_builder, args, filter_name);
        filter_action->addOrReplaceInOutputs(*and_function);
    }

    filter_action->removeUnusedActions(Names{filter_name}, true, true);
    return filter_action;
}

void MergeTreeRelParser::parseToAction(ActionsDAGPtr & filter_action, const substrait::Expression & rel, std::string & filter_name)
{
    if (rel.has_scalar_function())
        getPlanParser()->parseFunctionWithDAG(rel, filter_name, filter_action, true);
    else
    {
        const auto * in_node = parseExpression(filter_action, rel);
        filter_action->addOrReplaceInOutputs(*in_node);
        filter_name = in_node->result_name;
    }
}

void MergeTreeRelParser::analyzeExpressions(
    Conditions & res,
    const substrait::Expression & rel,
    std::set<Int64> & pk_positions,
    Block & block)
{
    if (rel.has_scalar_function() && getCHFunctionName(rel.scalar_function()) == "and")
    {
        int arguments_size = rel.scalar_function().arguments_size();

        for (int i = 0; i < arguments_size; ++i)
        {
            auto argument = rel.scalar_function().arguments(i);
            analyzeExpressions(res, argument.value(), pk_positions, block);
        }
    }
    else
    {
        Condition cond(rel);
        collectColumns(rel, cond.table_columns, block);
        cond.columns_size = getColumnsSize(cond.table_columns);
        cond.min_position_in_primary_key = findMinPosition(cond.table_columns, primary_key_names_positions);
        pk_positions.emplace(cond.min_position_in_primary_key);

        res.emplace_back(std::move(cond));
    }
}


UInt64 MergeTreeRelParser::getColumnsSize(const NameSet & columns)
{
    UInt64 size = 0;
    for (const auto & column : columns)
        if (column_sizes.contains(column))
            size += column_sizes[column];

    return size;
}

void MergeTreeRelParser::collectColumns(const substrait::Expression & rel, NameSet & columns, Block & block)
{
    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            return;
        }

        case substrait::Expression::RexTypeCase::kSelection: {
            const size_t idx = rel.selection().direct_reference().struct_field().field();
            if (const Names names = block.getNames(); names.size() > idx)
                columns.insert(names[idx]);

            return;
        }

        case substrait::Expression::RexTypeCase::kCast: {
            const auto & input = rel.cast().input();
            collectColumns(input, columns, block);
            return;
        }

        case substrait::Expression::RexTypeCase::kIfThen: {
            const auto & if_then = rel.if_then();

            auto condition_nums = if_then.ifs_size();
            for (int i = 0; i < condition_nums; ++i)
            {
                const auto & ifs = if_then.ifs(i);
                collectColumns(ifs.if_(), columns, block);
                collectColumns(ifs.then(), columns, block);
            }

            return;
        }

        case substrait::Expression::RexTypeCase::kScalarFunction: {
            for (const auto & arg : rel.scalar_function().arguments())
                collectColumns(arg.value(), columns, block);

            return;
        }

        case substrait::Expression::RexTypeCase::kSingularOrList: {
            const auto & options = rel.singular_or_list().options();
            /// options is empty always return false
            if (options.empty())
                return;

            collectColumns(rel.singular_or_list().value(), columns, block);
            return;
        }

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported spark expression type {} : {}",
                magic_enum::enum_name(rel.rex_type_case()),
                rel.DebugString());
    }
}


String MergeTreeRelParser::getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func)
{
    auto func_signature = getPlanParser()->function_mapping.at(std::to_string(substrait_func.function_reference()));
    auto pos = func_signature.find(':');
    auto func_name = func_signature.substr(0, pos);

    auto it = SCALAR_FUNCTIONS.find(func_name);
    if (it == SCALAR_FUNCTIONS.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unsupported substrait function on mergetree prewhere parser: {}", func_name);
    return it->second;
}
}
