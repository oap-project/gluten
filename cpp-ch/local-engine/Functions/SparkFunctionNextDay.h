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
class SparkFunctionNextDay : public DB::IFunction
{
public:
    static constexpr auto name = "spark_next_day";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionNextDay>(); }
    SparkFunctionNextDay() = default;
    ~SparkFunctionNextDay() override = default;

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override;

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t /*input_rows_count*/) const override;

    static Int8 getDayOfWeek(const String & abbr);

private:
    DB::ColumnPtr executeConst(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type) const;
    DB::ColumnPtr executeGeneral(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type) const;


    static DB::DataTypePtr getNestedResultType(DB::DataTypePtr from_arg_type) { return DB::removeNullable(from_arg_type); }
};
}
