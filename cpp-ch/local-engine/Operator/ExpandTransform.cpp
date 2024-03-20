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
#include <memory>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/IProcessor.h>
#include "ExpandTransorm.h"

#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
ExpandTransform::ExpandTransform(const DB::Block & input_, const DB::Block & output_, const ExpandField & project_set_exprs_)
    : DB::IProcessor({input_}, {output_}), project_set_exprs(project_set_exprs_)
{
}

ExpandTransform::Status ExpandTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;
        return Status::PortFull;
    }

    if (!has_input)
    {
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        input_chunk = input.pull(true);
        has_input = true;
        expand_expr_iterator = 0;
    }

    return Status::Ready;
}

void ExpandTransform::work()
{
    if (expand_expr_iterator >= project_set_exprs.getExpandRows())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "expand_expr_iterator >= project_set_exprs.getExpandRows()");
    const auto & original_cols = input_chunk.getColumns();
    size_t rows = input_chunk.getNumRows();
    DB::Columns cols;
    for (size_t j = 0; j < project_set_exprs.getExpandCols(); ++j)
    {
        const auto & type = project_set_exprs.getTypes()[j];
        const auto & kind = project_set_exprs.getKinds()[expand_expr_iterator][j];
        const auto & field = project_set_exprs.getFields()[expand_expr_iterator][j];

        if (kind == EXPAND_FIELD_KIND_SELECTION)
        {
            const auto & original_col = original_cols[field.get<Int32>()];
            if (type->isNullable() == original_col->isNullable())
            {
                cols.push_back(original_col);
            }
            else if (type->isNullable() && !original_col->isNullable())
            {
                auto null_map = DB::ColumnUInt8::create(rows, 0);
                auto col = DB::ColumnNullable::create(original_col, std::move(null_map));
                cols.push_back(std::move(col));
            }
            else
            {
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "Miss match nullable, column {} is nullable, but type {} is not nullable",
                    original_col->getName(),
                    type->getName());
            }
        }
        else if (field.isNull())
        {
            // Add null column
            auto null_map = DB::ColumnUInt8::create(rows, 1);
            auto nested_type = DB::removeNullable(type);
            auto col = DB::ColumnNullable::create(nested_type->createColumn()->cloneResized(rows), std::move(null_map));
            cols.push_back(std::move(col));
        }
        else
        {
            // Add constant column: gid, gpos, etc.
            auto col = type->createColumnConst(rows, field);
            cols.push_back(col->convertToFullColumnIfConst());
        }
    }
    output_chunk = DB::Chunk(cols, rows);
    expand_expr_iterator += 1;
    has_output = expand_expr_iterator <= project_set_exprs.getExpandRows();
    has_input = expand_expr_iterator < project_set_exprs.getExpandRows();
}
}
