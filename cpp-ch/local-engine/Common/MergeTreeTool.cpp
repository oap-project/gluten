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
#include "MergeTreeTool.h"

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <google/protobuf/util/json_util.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>

using namespace DB;

namespace local_engine
{
std::shared_ptr<DB::StorageInMemoryMetadata> buildMetaData(const DB::NamesAndTypesList &columns, ContextPtr context, const MergeTreeTable &table)
{
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    for (const auto & item : columns)
    {
        columns_description.add(ColumnDescription(item.name, item.type));
    }
    metadata->setColumns(std::move(columns_description));
    metadata->partition_key.expression_list_ast = std::make_shared<ASTExpressionList>();
    metadata->sorting_key = KeyDescription::parse(table.order_by_key, metadata->getColumns(), context);
    if (table.primary_key.empty())
    {
         if (table.order_by_key != MergeTreeTable::TUPLE)
             metadata->primary_key = KeyDescription::parse(table.order_by_key, metadata->getColumns(), context);
         else
            metadata->primary_key.expression = std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>());
    }
    else
    {
        metadata->primary_key = KeyDescription::parse(table.primary_key, metadata->getColumns(), context);
    }
    return metadata;
}

std::unique_ptr<MergeTreeSettings> buildMergeTreeSettings(const MergeTreeTableSettings & config)
{
    auto settings = std::make_unique<DB::MergeTreeSettings>();
    settings->set("allow_nullable_key", Field(1));
    if (!config.storage_policy.empty())
        settings->set("storage_policy", Field(config.storage_policy));
    return settings;
}

std::unique_ptr<SelectQueryInfo> buildQueryInfo(NamesAndTypesList & names_and_types_list)
{
    std::unique_ptr<SelectQueryInfo> query_info = std::make_unique<SelectQueryInfo>();
    query_info->query = std::make_shared<ASTSelectQuery>();
    auto syntax_analyzer_result = std::make_shared<TreeRewriterResult>(names_and_types_list);
    syntax_analyzer_result->analyzed_join = std::make_shared<TableJoin>();
    query_info->syntax_analyzer_result = syntax_analyzer_result;
    return query_info;
}


void parseTableConfig(MergeTreeTableSettings & settings, String config_json)
{
    rapidjson::Document doc;
    doc.Parse(config_json.c_str());
    if (doc.HasMember("storage_policy"))
        settings.storage_policy = doc["storage_policy"].GetString();

}

MergeTreeTable parseMergeTreeTableString(const std::string & info)
{

    ReadBufferFromString in(info);
    assertString("MergeTree;", in);
    MergeTreeTable table;
    readString(table.database, in);
    assertChar('\n', in);
    readString(table.table, in);
    assertChar('\n', in);
    String schema;
    readString(schema, in);
    google::protobuf::util::JsonStringToMessage(schema, &table.schema);
    assertChar('\n', in);
    readString(table.order_by_key, in);
    assertChar('\n', in);
    if (table.order_by_key != MergeTreeTable::TUPLE)
    {
        readString(table.primary_key, in);
        assertChar('\n', in);
    }
    readString(table.low_card_key, in);
    assertChar('\n', in);
    readString(table.relative_path, in);
    assertChar('\n', in);
    readString(table.absolute_path, in);
    assertChar('\n', in);
    String json;
    readString(json, in);
    parseTableConfig(table.table_configs, json);
    assertChar('\n', in);
    while (!in.eof())
    {
        MergeTreePart part;
        readString(part.name, in);
        assertChar('\n', in);
        readIntText(part.begin, in);
        assertChar('\n', in);
        readIntText(part.end, in);
        assertChar('\n', in);
        table.parts.emplace_back(part);
    }
    return table;
}

std::unordered_set<String> MergeTreeTable::getPartNames() const
{
    std::unordered_set<String> names;
    for (const auto & part : parts)
        names.emplace(part.name);
    return names;
}

RangesInDataParts MergeTreeTable::extractRange(DataPartsVector parts_vector) const
{
    std::unordered_map<String, DataPartPtr> name_index;
    std::ranges::for_each(parts_vector, [&](const DataPartPtr & part) {name_index.emplace(part->name, part);});
    RangesInDataParts ranges_in_data_parts;

    std::ranges::transform(
        parts,
        std::inserter(ranges_in_data_parts, ranges_in_data_parts.end()),
        [&](const MergeTreePart& part)
        {
            RangesInDataPart ranges_in_data_part;
            ranges_in_data_part.data_part = name_index.at(part.name);
            ranges_in_data_part.part_index_in_query = 0;
            ranges_in_data_part.ranges.emplace_back(MarkRange(part.begin, part.end));
            return ranges_in_data_part;
        });
    return ranges_in_data_parts;
}

}
