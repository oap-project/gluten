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
#include <filesystem>
#include <memory>
#include <optional>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Defines.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Disks/registerDisks.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Parser/RelParser.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/printPipeline.h>
#include <Storages/Output/WriteBufferBuilder.h>
#include <Storages/StorageMergeTreeFactory.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/wrappers.pb.h>
#include <Poco/Logger.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/BitHelpers.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/CurrentThread.h>
#include <Common/GlutenSignalHandler.h>
#include <Common/LoggerExtend.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "CHUtil.h"

#include <unistd.h>
#include <sys/resource.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
constexpr auto VIRTUAL_ROW_COUNT_COLUMN = "__VIRTUAL_ROW_COUNT_COLUMN__";

namespace fs = std::filesystem;

DB::Block BlockUtil::buildRowCountHeader()
{
    DB::Block header;
    auto type = std::make_shared<DB::DataTypeUInt8>();
    auto col = type->createColumn();
    DB::ColumnWithTypeAndName named_col(std::move(col), type, VIRTUAL_ROW_COUNT_COLUMN);
    header.insert(std::move(named_col));
    return header.cloneEmpty();
}

DB::Chunk BlockUtil::buildRowCountChunk(UInt64 rows)
{
    auto data_type = std::make_shared<DB::DataTypeUInt8>();
    auto col = data_type->createColumnConst(rows, 0);
    DB::Columns res_columns;
    res_columns.emplace_back(std::move(col));
    return DB::Chunk(std::move(res_columns), rows);
}

DB::Block BlockUtil::buildRowCountBlock(UInt64 rows)
{
    DB::Block block;
    auto uint8_ty = std::make_shared<DB::DataTypeUInt8>();
    auto col = uint8_ty->createColumnConst(rows, 0);
    DB::ColumnWithTypeAndName named_col(col, uint8_ty, VIRTUAL_ROW_COUNT_COLUMN);
    block.insert(named_col);
    return block;
}

DB::Block BlockUtil::buildHeader(const DB::NamesAndTypesList & names_types_list)
{
    DB::ColumnsWithTypeAndName cols;
    for (const auto & name_type : names_types_list)
    {
        DB::ColumnWithTypeAndName col(name_type.type->createColumn(), name_type.type, name_type.name);
        cols.emplace_back(col);
    }
    return DB::Block(cols);
}

/**
 * There is a special case with which we need be careful. In spark, struct/map/list are always
 * wrapped in Nullable, but this should not happen in clickhouse.
 */
DB::Block
BlockUtil::flattenBlock(const DB::Block & block, UInt64 flags, bool recursively, const std::unordered_set<size_t> & columns_to_skip_flatten)
{
    DB::Block res;

    for (size_t col_i = 0; col_i < block.columns(); ++col_i)
    {
        const auto & elem = block.getByPosition(col_i);

        if (columns_to_skip_flatten.contains(col_i))
        {
            res.insert(elem);
            continue;
        }

        DB::DataTypePtr nested_type = removeNullable(elem.type);
        DB::ColumnPtr nested_col = elem.column;
        DB::ColumnPtr null_map_col = nullptr;
        // A special case, const(Nullable(nothing))
        if (elem.type->isNullable() && typeid_cast<const DB::ColumnNullable *>(elem.column->getPtr().get()))
        {
            const auto * nullable_col = typeid_cast<const DB::ColumnNullable *>(elem.column->getPtr().get());
            nested_col = nullable_col->getNestedColumnPtr();
            null_map_col = nullable_col->getNullMapColumnPtr();
        }

        if (const DB::DataTypeArray * type_arr = typeid_cast<const DB::DataTypeArray *>(nested_type.get()))
        {
            const DB::DataTypeTuple * type_tuple = typeid_cast<const DB::DataTypeTuple *>(type_arr->getNestedType().get());
            if (type_tuple && type_tuple->haveExplicitNames() && (flags & FLAT_NESTED_TABLE))
            {
                const DB::DataTypes & element_types = type_tuple->getElements();
                const DB::Strings & names = type_tuple->getElementNames();
                size_t tuple_size = element_types.size();

                bool is_const = isColumnConst(*nested_col);
                const DB::ColumnArray * column_array;
                if (is_const)
                    column_array = typeid_cast<const DB::ColumnArray *>(&assert_cast<const DB::ColumnConst &>(*nested_col).getDataColumn());
                else
                    column_array = typeid_cast<const DB::ColumnArray *>(nested_col.get());

                const DB::ColumnPtr & column_offsets = column_array->getOffsetsPtr();

                const DB::ColumnTuple & column_tuple = typeid_cast<const DB::ColumnTuple &>(column_array->getData());
                const auto & element_columns = column_tuple.getColumns();

                for (size_t i = 0; i < tuple_size; ++i)
                {
                    String nested_name = DB::Nested::concatenateName(elem.name, names[i]);
                    DB::ColumnPtr column_array_of_element = DB::ColumnArray::create(element_columns[i], column_offsets);
                    auto named_column_array_of_element = DB::ColumnWithTypeAndName(
                        is_const ? DB::ColumnConst::create(std::move(column_array_of_element), block.rows()) : column_array_of_element,
                        std::make_shared<DB::DataTypeArray>(element_types[i]),
                        nested_name);

                    if (null_map_col)
                    {
                        // Should all field columns have the same null map ?
                        DB::DataTypePtr null_type = std::make_shared<DB::DataTypeNullable>(element_types[i]);
                        named_column_array_of_element.column
                            = DB::ColumnNullable::create(named_column_array_of_element.column, null_map_col);
                        named_column_array_of_element.type = null_type;
                    }

                    if (recursively)
                    {
                        auto flatten_one_col_block = flattenBlock({named_column_array_of_element}, flags, recursively);
                        for (const auto & named_col : flatten_one_col_block.getColumnsWithTypeAndName())
                            res.insert(named_col);
                    }
                    else
                        res.insert(named_column_array_of_element);
                }
            }
            else
                res.insert(elem);
        }
        else if (const DB::DataTypeTuple * type_tuple = typeid_cast<const DB::DataTypeTuple *>(nested_type.get()))
        {
            if ((flags & FLAT_STRUCT_FORCE) || (type_tuple->haveExplicitNames() && (flags & FLAT_STRUCT)))
            {
                const DB::DataTypes & element_types = type_tuple->getElements();
                DB::Strings element_names = type_tuple->getElementNames();
                if (element_names.empty())
                {
                    // This is a struct without named fields, we should flatten it.
                    // But we can't get the field names, so we use the field index as the field name.
                    for (size_t i = 0; i < element_types.size(); ++i)
                        element_names.push_back(elem.name + "_filed_" + std::to_string(i));
                }

                const DB::ColumnTuple * column_tuple;
                if (isColumnConst(*nested_col))
                    column_tuple = typeid_cast<const DB::ColumnTuple *>(&assert_cast<const DB::ColumnConst &>(*nested_col).getDataColumn());
                else
                    column_tuple = typeid_cast<const DB::ColumnTuple *>(nested_col.get());

                size_t tuple_size = column_tuple->tupleSize();
                for (size_t i = 0; i < tuple_size; ++i)
                {
                    const auto & element_column = column_tuple->getColumn(i);
                    String nested_name = DB::Nested::concatenateName(elem.name, element_names[i]);
                    auto new_element_col = DB::ColumnWithTypeAndName(element_column.getPtr(), element_types[i], nested_name);
                    if (null_map_col && !element_types[i]->isNullable())
                    {
                        // Should all field columns have the same null map ?
                        new_element_col.column = DB::ColumnNullable::create(new_element_col.column, null_map_col);
                        new_element_col.type = std::make_shared<DB::DataTypeNullable>(new_element_col.type);
                    }

                    if (recursively)
                    {
                        DB::Block one_col_block({new_element_col});
                        auto flatten_one_col_block = flattenBlock(one_col_block, flags, recursively);
                        for (const auto & named_col : flatten_one_col_block.getColumnsWithTypeAndName())
                            res.insert(named_col);
                    }
                    else
                        res.insert(std::move(new_element_col));
                }
            }
            else
                res.insert(elem);
        }
        else
            res.insert(elem);
    }

    return res;
}

DB::Block BlockUtil::concatenateBlocksMemoryEfficiently(std::vector<DB::Block> && blocks)
{
    if (blocks.empty())
        return {};

    size_t num_rows = 0;
    for (const auto & block : blocks)
        num_rows += block.rows();

    Block out = blocks[0].cloneEmpty();
    MutableColumns columns = out.mutateColumns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns[i]->reserve(num_rows);
        for (auto & block : blocks)
        {
            const auto & tmp_column = *block.getByPosition(0).column;
            columns[i]->insertRangeFrom(tmp_column, 0, block.rows());
            block.erase(0);
        }
    }
    blocks.clear();

    out.setColumns(std::move(columns));
    return out;
}


size_t PODArrayUtil::adjustMemoryEfficientSize(size_t n)
{
    /// According to definition of DEFUALT_BLOCK_SIZE
    size_t padding_n = 2 * PADDING_FOR_SIMD - 1;
    size_t rounded_n = roundUpToPowerOfTwoOrZero(n);
    size_t padded_n = n;
    if (rounded_n > n + n / 2)
    {
        size_t smaller_rounded_n = rounded_n / 2;
        padded_n = smaller_rounded_n < padding_n ? n : smaller_rounded_n - padding_n;
    }
    else
    {
        padded_n = rounded_n - padding_n;
    }
    return padded_n;
}

std::string PlanUtil::explainPlan(DB::QueryPlan & plan)
{
    std::string plan_str;
    DB::QueryPlan::ExplainPlanOptions buf_opt{
        .header = true,
        .actions = true,
        .indexes = true,
    };
    DB::WriteBufferFromOwnString buf;
    plan.explainPlan(buf, buf_opt);
    plan_str = buf.str();
    return plan_str;
}

std::vector<MergeTreeUtil::Path> MergeTreeUtil::getAllMergeTreeParts(const Path & storage_path)
{
    if (!fs::exists(storage_path))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid merge tree store path:{}", storage_path.string());

    // TODO: May need to check the storage format version
    std::vector<fs::path> res;
    for (const auto & entry : fs::directory_iterator(storage_path))
    {
        auto filename = entry.path().filename();
        if (filename == "format_version.txt" || filename == "detached" || filename == "_delta_log")
            continue;
        res.push_back(entry.path());
    }
    return res;
}

DB::NamesAndTypesList MergeTreeUtil::getSchemaFromMergeTreePart(const fs::path & part_path)
{
    DB::NamesAndTypesList names_types_list;
    if (!fs::exists(part_path))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid merge tree store path:{}", part_path.string());
    DB::ReadBufferFromFile readbuffer((part_path / "columns.txt").string());
    names_types_list.readText(readbuffer);
    return names_types_list;
}


NestedColumnExtractHelper::NestedColumnExtractHelper(const DB::Block & block_, bool case_insentive_)
    : block(block_), case_insentive(case_insentive_)
{
}

std::optional<DB::ColumnWithTypeAndName> NestedColumnExtractHelper::extractColumn(const String & column_name)
{
    if (const auto * col = findColumn(block, column_name))
        return {*col};

    auto nested_names = DB::Nested::splitName(column_name);
    if (case_insentive)
    {
        boost::to_lower(nested_names.first);
        boost::to_lower(nested_names.second);
    }
    if (!findColumn(block, nested_names.first))
        return {};

    if (!nested_tables.contains(nested_names.first))
    {
        DB::ColumnsWithTypeAndName columns = {*findColumn(block, nested_names.first)};
        nested_tables[nested_names.first] = std::make_shared<DB::Block>(BlockUtil::flattenBlock(columns));
    }

    return extractColumn(column_name, nested_names.first, nested_names.second);
}

std::optional<DB::ColumnWithTypeAndName> NestedColumnExtractHelper::extractColumn(
    const String & original_column_name, const String & column_name_prefix, const String & column_name_suffix)
{
    auto table_iter = nested_tables.find(column_name_prefix);
    if (table_iter == nested_tables.end())
        return {};

    auto & nested_table = table_iter->second;
    auto nested_names = DB::Nested::splitName(column_name_suffix);
    auto new_column_name_prefix = DB::Nested::concatenateName(column_name_prefix, nested_names.first);
    if (nested_names.second.empty())
    {
        if (const auto * column_ref = findColumn(*nested_table, new_column_name_prefix))
        {
            DB::ColumnWithTypeAndName column = *column_ref;
            if (case_insentive)
                column.name = original_column_name;
            return {std::move(column)};
        }
        else
        {
            return {};
        }
    }

    const auto * sub_col = findColumn(*nested_table, new_column_name_prefix);
    if (!sub_col)
        return {};

    DB::ColumnsWithTypeAndName columns = {*sub_col};
    DB::Block sub_block(columns);
    nested_tables[new_column_name_prefix] = std::make_shared<DB::Block>(BlockUtil::flattenBlock(sub_block));
    return extractColumn(original_column_name, new_column_name_prefix, nested_names.second);
}

const DB::ColumnWithTypeAndName * NestedColumnExtractHelper::findColumn(const DB::Block & in_block, const std::string & name) const
{
    if (case_insentive)
    {
        std::string final_name = name;
        boost::to_lower(final_name);
        const auto & cols = in_block.getColumnsWithTypeAndName();
        auto found = std::find_if(cols.begin(), cols.end(), [&](const auto & column) { return boost::iequals(column.name, name); });
        if (found == cols.end())
            return nullptr;
        return &*found;
    }

    const auto * col = in_block.findByName(name);
    if (col)
        return col;
    return nullptr;
}

const DB::ActionsDAG::Node * ActionsDAGUtil::convertNodeType(
    DB::ActionsDAGPtr & actions_dag,
    const DB::ActionsDAG::Node * node,
    const std::string & type_name,
    const std::string & result_name,
    CastType cast_type)
{
    DB::ColumnWithTypeAndName type_name_col;
    type_name_col.name = type_name;
    type_name_col.column = DB::DataTypeString().createColumnConst(0, type_name_col.name);
    type_name_col.type = std::make_shared<DB::DataTypeString>();
    const auto * right_arg = &actions_dag->addColumn(std::move(type_name_col));
    const auto * left_arg = node;
    DB::CastDiagnostic diagnostic = {node->result_name, node->result_name};
    DB::ActionsDAG::NodeRawConstPtrs children = {left_arg, right_arg};
    return &actions_dag->addFunction(
        DB::createInternalCastOverloadResolver(cast_type, std::move(diagnostic)), std::move(children), result_name);
}

String QueryPipelineUtil::explainPipeline(DB::QueryPipeline & pipeline)
{
    DB::WriteBufferFromOwnString buf;
    const auto & processors = pipeline.getProcessors();
    DB::printPipelineCompact(processors, buf, true);
    return buf.str();
}

using namespace DB;

std::map<std::string, std::string> BackendInitializerUtil::getBackendConfMap(std::string * plan)
{
    std::map<std::string, std::string> ch_backend_conf;
    if (plan == nullptr)
        return ch_backend_conf;

    /// Parse backend configs from plan extensions
    do
    {
        auto plan_ptr = std::make_unique<substrait::Plan>();
        auto success = plan_ptr->ParseFromString(*plan);
        if (!success)
            break;

        if (logger && logger->debug())
        {
            namespace pb_util = google::protobuf::util;
            pb_util::JsonOptions options;
            std::string json;
            auto s = pb_util::MessageToJsonString(*plan_ptr, &json, options);
            if (!s.ok())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not convert Substrait Plan to Json");
            LOG_DEBUG(&Poco::Logger::get("CHUtil"), "Update Config Map Plan:\n{}", json);
        }

        if (!plan_ptr->has_advanced_extensions() || !plan_ptr->advanced_extensions().has_enhancement())
            break;
        const auto & enhancement = plan_ptr->advanced_extensions().enhancement();

        if (!enhancement.Is<substrait::Expression>())
            break;

        substrait::Expression expression;
        if (!enhancement.UnpackTo(&expression) || !expression.has_literal() || !expression.literal().has_map())
            break;

        const auto & key_values = expression.literal().map().key_values();
        for (const auto & key_value : key_values)
        {
            if (!key_value.has_key() || !key_value.has_value())
                continue;

            const auto & key = key_value.key();
            const auto & value = key_value.value();
            if (!key.has_string() || !value.has_string())
                continue;

            ch_backend_conf[key.string()] = value.string();
        }
    } while (false);

    if (!ch_backend_conf.count(CH_RUNTIME_CONFIG_FILE))
    {
        /// Try to get config path from environment variable
        const char * config_path = std::getenv("CLICKHOUSE_BACKEND_CONFIG"); /// NOLINT
        if (config_path)
            ch_backend_conf[CH_RUNTIME_CONFIG_FILE] = config_path;
    }
    return ch_backend_conf;
}

DB::Context::ConfigurationPtr BackendInitializerUtil::initConfig(std::map<std::string, std::string> & backend_conf_map)
{
    DB::Context::ConfigurationPtr config;

    /// Parse input substrait plan, and get native conf map from it.
    if (backend_conf_map.count(CH_RUNTIME_CONFIG_FILE))
    {
        auto config_file = backend_conf_map[CH_RUNTIME_CONFIG_FILE];
        if (fs::exists(config_file) && fs::is_regular_file(config_file))
        {
            ConfigProcessor config_processor(config_file, false, true);
            config_processor.setConfigPath(fs::path(config_file).parent_path());
            auto loaded_config = config_processor.loadConfig(false);
            config = loaded_config.configuration;
        }
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "{} is not a valid configure file.", config_file);
    }
    else
        config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    for (const auto & kv : backend_conf_map)
    {
        const auto & key = kv.first;
        const auto & value = kv.second;
        // std::cout << "set config key:" << key << ", value:" << value << std::endl;

        if (key.starts_with(CH_RUNTIME_CONFIG_PREFIX) && key != CH_RUNTIME_CONFIG_FILE)
        {
            // Apply spark.gluten.sql.columnar.backend.ch.runtime_config.* to config
            config->setString(key.substr(CH_RUNTIME_CONFIG_PREFIX.size()), value);
        }
    }
    return config;
}


void BackendInitializerUtil::initLoggers(DB::Context::ConfigurationPtr config)
{
    auto level = config->getString("logger.level", "warning");
    if (config->has("logger.log"))
        local_engine::LoggerExtend::initFileLogger(*config, "ClickHouseBackend");
    else
        local_engine::LoggerExtend::initConsoleLogger(level);

    logger = &Poco::Logger::get("ClickHouseBackend");
}

void BackendInitializerUtil::initEnvs(DB::Context::ConfigurationPtr config)
{
    /// Set environment variable TZ if possible
    if (config->has("timezone"))
    {
        String timezone_name = config->getString("timezone");
        if (0 != setenv("TZ", timezone_name.data(), 1)) /// NOLINT
            throw Poco::Exception("Cannot setenv TZ variable");

        tzset();
        DateLUT::setDefaultTimezone(timezone_name);
    }

    /// Set environment variable LIBHDFS3_CONF if possible
    if (config->has(LIBHDFS3_CONF_KEY))
    {
        std::string libhdfs3_conf = config->getString(LIBHDFS3_CONF_KEY, "");
        setenv("LIBHDFS3_CONF", libhdfs3_conf.c_str(), true); /// NOLINT
    }

    /// Enable logging in libhdfs3, logs will be written to stderr
    setenv("HDFS_ENABLE_LOGGING", "true", true); /// NOLINT

    /// Get environment varaible SPARK_USER if possible
    const char * spark_user_c_str = std::getenv("SPARK_USER");
    if (spark_user_c_str)
        spark_user = spark_user_c_str;
}

void BackendInitializerUtil::initSettings(std::map<std::string, std::string> & backend_conf_map, DB::Settings & settings)
{
    /// Initialize default setting.
    settings.set("date_time_input_format", "best_effort");

    for (const auto & pair : backend_conf_map)
    {
        // Firstly apply spark.gluten.sql.columnar.backend.ch.runtime_config.local_engine.settings.* to settings
        if (pair.first.starts_with(CH_RUNTIME_CONFIG_PREFIX + SETTINGS_PATH + "."))
        {
            settings.set(pair.first.substr((CH_RUNTIME_CONFIG_PREFIX + SETTINGS_PATH + ".").size()), pair.second);
            LOG_DEBUG(&Poco::Logger::get("CHUtil"), "Set settings from config key:{} value:{}", pair.first, pair.second);
        }
        else if (pair.first.starts_with(CH_RUNTIME_SETTINGS_PREFIX))
        {
            settings.set(pair.first.substr(CH_RUNTIME_SETTINGS_PREFIX.size()), pair.second);
            LOG_DEBUG(&Poco::Logger::get("CHUtil"), "Set settings key:{} value:{}", pair.first, pair.second);
        }
        else if (pair.first.starts_with(SPARK_HADOOP_PREFIX + S3A_PREFIX))
        {
            // Apply general S3 configs, e.g. spark.hadoop.fs.s3a.access.key -> set in fs.s3a.access.key
            // deal with per bucket S3 configs, e.g. fs.s3a.bucket.bucket_name.assumed.role.arn
            // for gluten, we require first authenticate with AK/SK(or instance profile), then assume other roles with STS
            // so only the following per-bucket configs are supported:
            // 1. fs.s3a.bucket.bucket_name.assumed.role.arn
            // 2. fs.s3a.bucket.bucket_name.assumed.role.session.name
            // 3. fs.s3a.bucket.bucket_name.endpoint
            // 4. fs.s3a.bucket.bucket_name.assumed.role.externalId (non hadoop official)
            settings.set(pair.first.substr(SPARK_HADOOP_PREFIX.length()), pair.second);
        }
    }

    /// Finally apply some fixed kvs to settings.
    settings.set("join_use_nulls", true);
    settings.set("input_format_orc_allow_missing_columns", true);
    settings.set("input_format_orc_case_insensitive_column_matching", true);
    settings.set("input_format_orc_import_nested", true);
    settings.set("input_format_orc_skip_columns_with_unsupported_types_in_schema_inference", true);
    settings.set("input_format_parquet_allow_missing_columns", true);
    settings.set("input_format_parquet_case_insensitive_column_matching", true);
    settings.set("input_format_parquet_import_nested", true);
    settings.set("input_format_json_read_numbers_as_strings", true);
    settings.set("input_format_json_read_bools_as_numbers", false);
    settings.set("input_format_csv_trim_whitespaces", false);
    settings.set("input_format_csv_allow_cr_end_of_line", true);
    settings.set("output_format_orc_string_as_string", true);
    settings.set("output_format_parquet_version", "1.0");
    settings.set("output_format_parquet_compression_method", "snappy");
    settings.set("output_format_parquet_string_as_string", true);
    settings.set("output_format_parquet_fixed_string_as_fixed_byte_array", false);
    settings.set("output_format_json_quote_64bit_integers", false);
    settings.set("output_format_json_quote_denormals", true);
    settings.set("output_format_json_skip_null_value_in_named_tuples", true);
    settings.set("function_json_value_return_type_allow_complex", true);
    settings.set("function_json_value_return_type_allow_nullable", true);
    settings.set("precise_float_parsing", true);
}

void BackendInitializerUtil::initContexts(DB::Context::ConfigurationPtr config)
{
    /// Make sure global_context and shared_context are constructed only once.
    auto & shared_context = SerializedPlanParser::shared_context;
    if (!shared_context.get())
        shared_context = SharedContextHolder(Context::createShared());

    auto & global_context = SerializedPlanParser::global_context;
    if (!global_context)
    {
        global_context = Context::createGlobal(shared_context.get());
        global_context->makeGlobalContext();
        global_context->setConfig(config);

        auto getDefaultPath = [config] -> auto
        {
            bool use_current_directory_as_tmp = config->getBool("use_current_directory_as_tmp", false);
            char buffer[PATH_MAX];
            if (use_current_directory_as_tmp && getcwd(buffer, sizeof(buffer)) != nullptr)
                return std::string(buffer) + "/tmp/libch";
            else
                return std::string("/tmp/libch");
        };

        global_context->setTemporaryStoragePath(config->getString("tmp_path", getDefaultPath()), 0);
        global_context->setPath(config->getString("path", "/"));
    }
}

void BackendInitializerUtil::applyGlobalConfigAndSettings(DB::Context::ConfigurationPtr config, DB::Settings & settings)
{
    auto & global_context = SerializedPlanParser::global_context;
    global_context->setConfig(config);
    global_context->setSettings(settings);
}

void BackendInitializerUtil::updateNewSettings(DB::ContextMutablePtr context, DB::Settings & settings)
{
    context->setSettings(settings);
}

extern void registerAggregateFunctionCombinatorPartialMerge(AggregateFunctionCombinatorFactory &);
extern void registerAggregateFunctionsBloomFilter(AggregateFunctionFactory &);
extern void registerFunctions(FunctionFactory &);

void registerAllFunctions()
{
    DB::registerFunctions();

    DB::registerAggregateFunctions();
    auto & agg_factory = AggregateFunctionFactory::instance();
    registerAggregateFunctionsBloomFilter(agg_factory);

    {
        /// register aggregate function combinators from local_engine
        auto & factory = AggregateFunctionCombinatorFactory::instance();
        registerAggregateFunctionCombinatorPartialMerge(factory);
    }
    registerDisks(true);
}

void BackendInitializerUtil::registerAllFactories()
{
    registerReadBufferBuilders();
    registerWriteBufferBuilders();

    LOG_INFO(logger, "Register read buffer builders.");

    registerRelParsers();
    LOG_INFO(logger, "Register relation parsers.");

    registerAllFunctions();
    LOG_INFO(logger, "Register all functions.");
}

void BackendInitializerUtil::initCompiledExpressionCache(DB::Context::ConfigurationPtr config)
{
#if USE_EMBEDDED_COMPILER
    /// 128 MB
    constexpr size_t compiled_expression_cache_size_default = 1024 * 1024 * 128;
    size_t compiled_expression_cache_size = config->getUInt64("compiled_expression_cache_size", compiled_expression_cache_size_default);

    constexpr size_t compiled_expression_cache_elements_size_default = 10000;
    size_t compiled_expression_cache_elements_size
        = config->getUInt64("compiled_expression_cache_elements_size", compiled_expression_cache_elements_size_default);

    CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_size, compiled_expression_cache_elements_size);
#endif
}

void BackendInitializerUtil::init_json(std::string * plan_json)
{
    auto plan_ptr = std::make_unique<substrait::Plan>();
    google::protobuf::util::JsonStringToMessage(plan_json->c_str(), plan_ptr.get());
    return init(new String(plan_ptr->SerializeAsString()));
}

void BackendInitializerUtil::init(std::string * plan)
{
    std::map<std::string, std::string> backend_conf_map = getBackendConfMap(plan);
    DB::Context::ConfigurationPtr config = initConfig(backend_conf_map);

    initLoggers(config);

    initEnvs(config);
    LOG_INFO(logger, "Init environment variables.");

    DB::Settings settings;
    initSettings(backend_conf_map, settings);
    LOG_INFO(logger, "Init settings.");

    initContexts(config);
    LOG_INFO(logger, "Init shared context and global context.");

    applyGlobalConfigAndSettings(config, settings);
    LOG_INFO(logger, "Apply configuration and setting for global context.");

    // clean static per_bucket_clients and shared_client before running local engine,
    // in case of running the multiple gluten ut in one process
    ReadBufferBuilderFactory::instance().clean();

    std::call_once(
        init_flag,
        [&]
        {
            SignalHandler::instance().init();

            registerAllFactories();
            LOG_INFO(logger, "Register all factories.");

            initCompiledExpressionCache(config);
            LOG_INFO(logger, "Init compiled expressions cache factory.");

            GlobalThreadPool::initialize();

            const size_t active_parts_loading_threads = config->getUInt("max_active_parts_loading_thread_pool_size", 64);
            DB::getActivePartsLoadingThreadPool().initialize(
                active_parts_loading_threads,
                0, // We don't need any threads one all the parts will be loaded
                active_parts_loading_threads);
        });
}

void BackendInitializerUtil::updateConfig(DB::ContextMutablePtr context, std::string * plan)
{
    std::map<std::string, std::string> backend_conf_map = getBackendConfMap(plan);

    // configs cannot be updated per query
    // settings can be updated per query

    auto ctx = context->getSettings(); // make a copy
    initSettings(backend_conf_map, ctx);
    updateNewSettings(context, ctx);
}

void BackendFinalizerUtil::finalizeGlobally()
{
    // Make sure client caches release before ClientCacheRegistry
    ReadBufferBuilderFactory::instance().clean();
    StorageMergeTreeFactory::clear();
    auto & global_context = SerializedPlanParser::global_context;
    auto & shared_context = SerializedPlanParser::shared_context;
    if (global_context)
    {
        global_context->shutdown();
        global_context.reset();
        shared_context.reset();
    }
}

void BackendFinalizerUtil::finalizeSessionally()
{
}

Int64 DateTimeUtil::currentTimeMillis()
{
    return timeInMilliseconds(std::chrono::system_clock::now());
}

UInt64 MemoryUtil::getCurrentMemoryUsage(size_t depth)
{
    Int64 current_memory_usage = 0;
    auto * current_mem_tracker = DB::CurrentThread::getMemoryTracker();
    for (size_t i = 0; i < depth && current_mem_tracker; ++i)
        current_mem_tracker = current_mem_tracker->getParent();
    if (current_mem_tracker)
        current_memory_usage = current_mem_tracker->get();
    return current_memory_usage < 0 ? 0 : current_memory_usage;
}

UInt64 MemoryUtil::getMemoryRSS()
{
    long rss = 0L;
    FILE * fp = NULL;
    char buf[4096];
    sprintf(buf, "/proc/%d/statm", getpid());
    if ((fp = fopen(buf, "r")) == NULL)
        return 0;
    fscanf(fp, "%*s%ld", &rss);
    fclose(fp);
    return rss * sysconf(_SC_PAGESIZE);
}

}
