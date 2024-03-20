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
#include "DebugUtils.h"
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionHelpers.h>

namespace debug
{
void headBlock(const DB::Block & block, size_t count)
{
    std::cerr << "============Block============" << std::endl;
    std::cerr << block.dumpStructure() << std::endl;
    // print header
    for (const auto & name : block.getNames())
        std::cerr << name << "\t";
    std::cerr << std::endl;

    // print rows
    for (size_t row = 0; row < std::min(count, block.rows()); ++row)
    {
        for (size_t column = 0; column < block.columns(); ++column)
        {
            const auto type = block.getByPosition(column).type;
            auto col = block.getByPosition(column).column;

            if (column > 0)
                std::cerr << "\t";
            DB::WhichDataType which(type);
            if (which.isAggregateFunction())
                std::cerr << "Nan";
            else if (col->isNullAt(row))
                std::cerr << "null";
            else
                std::cerr << toString((*col)[row]);
        }
        std::cerr << std::endl;
    }
}

void headColumn(const DB::ColumnPtr & column, size_t count)
{
    std::cerr << "============Column============" << std::endl;

    // print header
    std::cerr << column->getName() << "\t";
    std::cerr << std::endl;

    // print rows
    for (size_t row = 0; row < std::min(count, column->size()); ++row)
        std::cerr << toString((*column)[row]) << std::endl;
}

}
