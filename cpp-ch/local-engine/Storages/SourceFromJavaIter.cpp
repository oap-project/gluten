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
#include "SourceFromJavaIter.h"
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <jni/jni_common.h>
#include <Common/assert_cast.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>
#include <Common/Exception.h>
#include <Common/JNIUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>

namespace local_engine
{
jclass SourceFromJavaIter::serialized_record_batch_iterator_class = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_hasNext = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_next = nullptr;


static DB::Block getRealHeader(const DB::Block & header)
{
    if (header.columns())
        return header;
    return BlockUtil::buildRowCountHeader();
}


DB::Block * SourceFromJavaIter::peekBlock(JNIEnv * env, jobject java_iter)
{
    jboolean has_next = safeCallBooleanMethod(env, java_iter, serialized_record_batch_iterator_hasNext);
    if (has_next)
    {
        jbyteArray block = static_cast<jbyteArray>(safeCallObjectMethod(env, java_iter, serialized_record_batch_iterator_next));
        return reinterpret_cast<DB::Block *>(byteArrayToLong(env, block));
    }
    return nullptr;
}


SourceFromJavaIter::SourceFromJavaIter(
    DB::ContextPtr context_, DB::Block header, jobject java_iter_, bool materialize_input_, DB::Block * first_block_)
    : DB::ISource(getRealHeader(header))
    , context(context_)
    , original_header(header)
    , java_iter(java_iter_)
    , materialize_input(materialize_input_)
    , first_block(first_block_)
{
}

DB::Chunk SourceFromJavaIter::generate()
{
    GET_JNIENV(env)
    SCOPE_EXIT({CLEAN_JNIENV});

    DB::Chunk result;
    DB::Block * data = nullptr;
    if (first_block) [[unlikely]]
    {
        data = first_block;
        first_block = nullptr;
    }
    else if (jboolean has_next = safeCallBooleanMethod(env, java_iter, serialized_record_batch_iterator_hasNext))
    {
        jbyteArray block = static_cast<jbyteArray>(safeCallObjectMethod(env, java_iter, serialized_record_batch_iterator_next));
        data = reinterpret_cast<DB::Block *>(byteArrayToLong(env, block));
    }
    else
        return {};

    /// Post-processing
    if (materialize_input)
        materializeBlockInplace(*data);

    if (data->rows() > 0)
    {
        size_t rows = data->rows();
        if (original_header.columns())
        {
            result.setColumns(data->mutateColumns(), rows);
            convertNullable(result);
            auto info = std::make_shared<DB::AggregatedChunkInfo>();
            info->is_overflows = data->info.is_overflows;
            info->bucket_num = data->info.bucket_num;
            result.setChunkInfo(info);
        }
        else
        {
            result = BlockUtil::buildRowCountChunk(rows);
            auto info = std::make_shared<DB::AggregatedChunkInfo>();
            result.setChunkInfo(info);
        }
    }
    return result;
}

SourceFromJavaIter::~SourceFromJavaIter()
{
    GET_JNIENV(env)
    env->DeleteGlobalRef(java_iter);
    CLEAN_JNIENV
}

Int64 SourceFromJavaIter::byteArrayToLong(JNIEnv * env, jbyteArray arr)
{
    jsize len = env->GetArrayLength(arr);
    assert(len == sizeof(Int64));
    char * c_arr = new char[len];
    env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte *>(c_arr));
    std::reverse(c_arr, c_arr + 8);
    Int64 result = reinterpret_cast<Int64 *>(c_arr)[0];
    delete[] c_arr;
    return result;
}

void SourceFromJavaIter::convertNullable(DB::Chunk & chunk)
{
    auto output = this->getOutputs().front().getHeader();
    auto rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto & column = columns.at(i);
        const auto & type = output.getByPosition(i).type;
        columns[i] = convertNestedNullable(column, type);
    }
    chunk.setColumns(columns, rows);
}

DB::ColumnPtr SourceFromJavaIter::convertNestedNullable(const DB::ColumnPtr & column, const DB::DataTypePtr & target_type)
{
    DB::WhichDataType column_type(column->getDataType());
    if (column_type.isAggregateFunction())
        return column;

    if (DB::isColumnConst(*column))
    {
        const auto & data_column = assert_cast<const DB::ColumnConst &>(*column).getDataColumnPtr();
        const auto & result_column = convertNestedNullable(data_column, target_type);
        return DB::ColumnConst::create(result_column, column->size());
    }

    // if target type is non-nullable, the column type must be also non-nullable, recursively converting it's nested type
    // if target type is nullable, the column type may be nullable or non-nullable, converting it and then recursively converting it's nested type
    DB::ColumnPtr new_column = column;
    if (!column_type.isNullable() && target_type->isNullable())
        new_column = DB::makeNullable(column);

    DB::ColumnPtr nested_column = new_column;
    DB::DataTypePtr nested_target_type = removeNullable(target_type);
    if (new_column->isNullable())
    {
        const auto & nullable_col = typeid_cast<const DB::ColumnNullable *>(new_column->getPtr().get());
        nested_column = nullable_col->getNestedColumnPtr();
        const auto & result_column = convertNestedNullable(nested_column, nested_target_type);
        return DB::ColumnNullable::create(result_column, nullable_col->getNullMapColumnPtr());
    }

    DB::WhichDataType nested_column_type(nested_column->getDataType());
    if (nested_column_type.isMap())
    {
        // header: Map(String, Nullable(String))
        // chunk:  Map(String, String)
        const auto & array_column = assert_cast<const DB::ColumnMap &>(*nested_column).getNestedColumn();
        const auto & map_type = assert_cast<const DB::DataTypeMap &>(*nested_target_type);
        auto tuple_columns = assert_cast<const DB::ColumnTuple *>(array_column.getDataPtr().get())->getColumns();
        // only convert for value column as key is always non-nullable
        const auto & value_column = convertNestedNullable(tuple_columns[1],  map_type.getValueType());
        auto result_column = DB::ColumnArray::create(DB::ColumnTuple::create(DB::Columns{tuple_columns[0], value_column}), array_column.getOffsetsPtr());
        return DB::ColumnMap::create(std::move(result_column));
    }

    if (nested_column_type.isArray())
    {
        // header: Array(Nullable(String))
        // chunk:  Array(String)
        const auto & list_column = assert_cast<const DB::ColumnArray &>(*nested_column);
        auto nested_type = assert_cast<const DB::DataTypeArray &>(*nested_target_type).getNestedType();
        const auto & result_column = convertNestedNullable(list_column.getDataPtr(), nested_type);
        return DB::ColumnArray::create(result_column, list_column.getOffsetsPtr());
    }

    if (nested_column_type.isTuple())
    {
        // header: Tuple(Nullable(String), Nullable(String))
        // chunk:  Tuple(String, Nullable(String))
        const auto & tuple_column = assert_cast<const DB::ColumnTuple &>(*nested_column);
        auto nested_types = assert_cast<const DB::DataTypeTuple &>(*nested_target_type).getElements();
        DB::Columns columns;
        for (size_t i = 0; i != tuple_column.tupleSize(); ++i)
            columns.push_back(convertNestedNullable(tuple_column.getColumnPtr(i), nested_types[i]));
        return DB::ColumnTuple::create(std::move(columns));
    }

    return new_column;
}

}
