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

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/LocalDateTime.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

using namespace DB;

namespace local_eingine
{

class SparkFunctionDateToUnixTimestamp : public IFunction
{
public:
    static constexpr auto name = "sparkDateToUnixTimestamp";
    static FunctionPtr create(ContextPtr) { return std::make_shared<SparkFunctionDateToUnixTimestamp>(); }
    SparkFunctionDateToUnixTimestamp()
    {
        const DateLUTImpl * date_lut = &DateLUT::instance("UTC");
        UInt32 utc_timestamp = static_cast<UInt32>(0);
        LocalDateTime date_time(utc_timestamp, *date_lut);
        UInt32 unix_timestamp = date_time.to_time_t();
        delta_timestamp_from_utc = unix_timestamp - utc_timestamp;
    }
    ~SparkFunctionDateToUnixTimestamp() override = default;
    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo &) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return std::make_shared<DataTypeUInt32>(); }

    ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} argument size must be 1 or 2", name);

        ColumnWithTypeAndName first_arg = arguments[0];
        if (isDate(first_arg.type))
            return executeInternal<UInt16>(first_arg.column, input_rows);
        else
            return executeInternal<Int32>(first_arg.column, input_rows);
    }

    template <typename T>
    ColumnPtr NO_SANITIZE_UNDEFINED executeInternal(const ColumnPtr & col, size_t input_rows) const
    {
        const ColumnVector<T> * col_src = checkAndGetColumn<ColumnVector<T>>(col.get());
        MutableColumnPtr res = ColumnVector<UInt32>::create(col->size());
        PaddedPODArray<UInt32> & data = assert_cast<ColumnVector<UInt32> *>(res.get())->getData();
        if (col->size() == 0)
            return res;

        for (size_t i = 0; i < input_rows; ++i)
        {
            const T t = col_src->getElement(i);
            data[i] = static_cast<UInt32>(t * DATE_SECONDS_PER_DAY) + delta_timestamp_from_utc;
        }
        return res;
    }

private:
    UInt32 delta_timestamp_from_utc;
};

}
