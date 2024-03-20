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

#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace local_engine
{
class SparkFunctionConv : public DB::IFunction
{
public:
    static constexpr uint32_t CONV_MAX_LENGTH = 64U + 1U;
    static constexpr auto name = "sparkConv";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionConv>(); }
    SparkFunctionConv() = default;
    ~SparkFunctionConv() override = default;

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & /*arguments*/) const override;

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t /*input_rows_count*/) const override;

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    static DB::DataTypePtr getNestedResultType(DB::DataTypePtr from_arg_type) { return DB::removeNullable(from_arg_type); }
};
}
