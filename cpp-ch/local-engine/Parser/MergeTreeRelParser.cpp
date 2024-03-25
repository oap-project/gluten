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
#include <Storages/Mergetree/MetaDataHelper.h>

#include "MergeTreeRelParser.h"

#include <Poco/StringTokenizer.h>


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

CustomStorageMergeTreePtr MergeTreeRelParser::parseStorage(
    const substrait::ReadRel::ExtensionTable & extension_table,
    ContextMutablePtr context)
{
    google::protobuf::StringValue table;
    table.ParseFromString(extension_table.detail().value());
    auto merge_tree_table = local_engine::parseMergeTreeTableString(table.value());
    DB::Block header;
    header = TypeParser::buildBlockFromNamedStruct(merge_tree_table.schema, merge_tree_table.low_card_key);
    auto names_and_types_list = header.getNamesAndTypesList();
    auto storage_factory = StorageMergeTreeFactory::instance();
    auto metadata = buildMetaData(names_and_types_list, context, merge_tree_table);

    auto storage = storage_factory.getStorage(
        StorageID(merge_tree_table.database, merge_tree_table.table),
        metadata->getColumns(),
        [&]() -> CustomStorageMergeTreePtr
        {
            auto custom_storage_merge_tree = std::make_shared<CustomStorageMergeTree>(
                StorageID(merge_tree_table.database, merge_tree_table.table),
                merge_tree_table.relative_path,
                *metadata,
                false,
                context,
                "",
                MergeTreeData::MergingParams(),
                buildMergeTreeSettings(merge_tree_table.table_configs));
            return custom_storage_merge_tree;
        });
    return storage;
}

DB::QueryPlanPtr
MergeTreeRelParser::parseReadRel(
    DB::QueryPlanPtr query_plan,
    const substrait::ReadRel & rel,
    const substrait::ReadRel::ExtensionTable & extension_table,
    std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    google::protobuf::StringValue table;
    table.ParseFromString(extension_table.detail().value());
    auto merge_tree_table = local_engine::parseMergeTreeTableString(table.value());
    DB::Block header;
    header = TypeParser::buildBlockFromNamedStruct(merge_tree_table.schema, merge_tree_table.low_card_key);
    DB::Block input;
    if (rel.has_base_schema() && rel.base_schema().names_size())
    {
        input = TypeParser::buildBlockFromNamedStruct(rel.base_schema());
    }
    else
    {
        NamesAndTypesList one_column_name_type;
        one_column_name_type.push_back(header.getNamesAndTypesList().front());
        input = BlockUtil::buildHeader(one_column_name_type);
        LOG_DEBUG(&Poco::Logger::get("SerializedPlanParser"), "Try to read ({}) instead of empty header", header.dumpNames());
    }
    auto storage_factory = StorageMergeTreeFactory::instance();
    auto metadata = buildMetaData(header.getNamesAndTypesList(), context, merge_tree_table);
    query_context.metadata = metadata;
    StorageID table_id(merge_tree_table.database, merge_tree_table.table);
    auto storage = storage_factory.getStorage(
        table_id,
        metadata->getColumns(),
        [&]() -> CustomStorageMergeTreePtr
        {
            auto custom_storage_merge_tree = std::make_shared<CustomStorageMergeTree>(
                StorageID(merge_tree_table.database, merge_tree_table.table),
                merge_tree_table.relative_path,
                *metadata,
                false,
                global_context,
                "",
                MergeTreeData::MergingParams(),
                buildMergeTreeSettings(merge_tree_table.table_configs));
            return custom_storage_merge_tree;
        });

    restoreMetaData(storage, merge_tree_table, context);
    for (const auto & [name, sizes] : storage->getColumnSizes())
        column_sizes[name] = sizes.data_compressed;
    query_context.storage_snapshot = std::make_shared<StorageSnapshot>(*storage, metadata);
    query_context.custom_storage_merge_tree = storage;
    auto names_and_types_list = input.getNamesAndTypesList();

    auto query_info = buildQueryInfo(names_and_types_list);

    std::set<String> non_nullable_columns;
    if (rel.has_filter())
    {
        NonNullableColumnsResolver non_nullable_columns_resolver(input, *getPlanParser(), rel.filter());
        non_nullable_columns = non_nullable_columns_resolver.resolve();
        query_info->prewhere_info = parsePreWhereInfo(rel.filter(), input);
    }

    std::vector<DataPartPtr> selected_parts = storage_factory.getDataParts(table_id, merge_tree_table.getPartNames());
    auto ranges = merge_tree_table.extractRange(selected_parts);
    if (selected_parts.empty())
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "no data part found.");
    auto read_step = query_context.custom_storage_merge_tree->reader.readFromParts(
        selected_parts,
        /* alter_conversions = */ {},
        names_and_types_list.getNames(),
        query_context.storage_snapshot,
        *query_info,
        context,
        context->getSettingsRef().max_block_size,
        1);

    auto * source_step_with_filter = static_cast<SourceStepWithFilter *>(read_step.get());
    const auto & storage_prewhere_info = query_info->prewhere_info;
    if (storage_prewhere_info)
    {
        source_step_with_filter->addFilter(storage_prewhere_info->prewhere_actions, storage_prewhere_info->prewhere_column_name);
        source_step_with_filter->applyFilters();
    }
    query_context.custom_storage_merge_tree->wrapRangesInDataParts(*reinterpret_cast<ReadFromMergeTree *>(read_step.get()), ranges);
    steps.emplace_back(read_step.get());
    query_plan->addStep(std::move(read_step));
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

PrewhereInfoPtr MergeTreeRelParser::parsePreWhereInfo(const substrait::Expression & rel, Block & input)
{
    std::string filter_name;
    auto prewhere_info = std::make_shared<PrewhereInfo>();
    prewhere_info->prewhere_actions = optimizePrewhereAction(rel, filter_name, input);
    prewhere_info->prewhere_column_name = filter_name;
    prewhere_info->need_filter = true;
    prewhere_info->remove_prewhere_column = true;
    prewhere_info->prewhere_actions->projectInput(false);
    for (const auto & name : input.getNames())
        prewhere_info->prewhere_actions->tryRestoreColumn(name);
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

    // TODO need to test
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

    filter_action->removeUnusedActions(Names{filter_name}, false, true);
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
    Conditions & res, const substrait::Expression & rel, std::set<Int64> & pk_positions, Block & block)
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

        // TODO: get primary_key_names
        const NameToIndexMap primary_key_names_positions;
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
