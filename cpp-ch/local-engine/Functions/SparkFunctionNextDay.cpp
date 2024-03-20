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
#include "SparkFunctionNextDay.h"
#include <unordered_map>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/TransformDateTime64.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{
DB::DataTypePtr SparkFunctionNextDay::getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() != 2)
    {
        throw DB::Exception(
            DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} requires exactly 2 arguments, {} provided", getName(), arguments.size());
    }
    auto arg0_type = DB::removeNullable(arguments[0].type);
    auto arg0_which_type = DB::WhichDataType(arg0_type);
    if (!arg0_which_type.isDate() && !arg0_which_type.isDate32() && !arg0_which_type.isDateTime() && !arg0_which_type.isDateTime64())
    {
        throw DB::Exception(
            DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument[0] of function {}",
            arguments[0].type->getName(),
            getName());
    }

    auto arg1_type = DB::removeNullable(arguments[1].type);
    if (!DB::WhichDataType(arg1_type).isString())
    {
        throw DB::Exception(
            DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument[1] of function {}",
            arguments[1].type->getName(),
            getName());
    }

    return DB::makeNullableSafe(arg0_type);
}

DB::ColumnPtr SparkFunctionNextDay::executeImpl(
    const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t /*input_rows_count*/) const
{
    if (DB::isColumnConst(*arguments[1].column))
        return executeConst(arguments, result_type);
    else
        return executeGeneral(arguments, result_type);
}

struct NextDayConstTransformer
{
    static constexpr auto name = "NextDay";
    static const UInt8 week_mode = 0;

    static inline Int64 execute(Int64 day, UInt8 next_weekday, const DateLUTImpl & time_zone)
    {
        auto from_week_day = DB::ToDayOfWeekImpl::execute(day, week_mode, time_zone);
        return day + static_cast<Int64>(calDayDelta(from_week_day, next_weekday));
    }

    static inline UInt32 execute(UInt32 day, UInt8 next_weekday, const DateLUTImpl & time_zone)
    {
        auto from_week_day = DB::ToDayOfWeekImpl::execute(day, week_mode, time_zone);
        return day + static_cast<UInt32>(calDayDelta(from_week_day, next_weekday));
    }

    static inline Int32 execute(Int32 day, UInt8 next_weekday, const DateLUTImpl & time_zone)
    {
        auto from_week_day = DB::ToDayOfWeekImpl::execute(day, week_mode, time_zone);
        return day + static_cast<Int32>(calDayDelta(from_week_day, next_weekday));
    }

    static inline UInt16 execute(UInt16 day, UInt8 next_weekday, const DateLUTImpl & time_zone)
    {
        auto from_week_day = DB::ToDayOfWeekImpl::execute(day, week_mode, time_zone);
        return day + static_cast<UInt16>(calDayDelta(from_week_day, next_weekday));
    }

    static UInt16 calDayDelta(UInt16 from_week_day, UInt16 next_weekday)
    {
        return next_weekday > from_week_day ? next_weekday - from_week_day : 7 + next_weekday - from_week_day;
    }
};


template <typename DateType>
class NextDayConstImpl
{
public:
    template <typename Transformer>
    static void execute(const DB::IColumn & src_col, UInt16 next_weekday, DB::IColumn * dst_col, Transformer transformer = {})
    {
        const auto & time_zone = DateLUT::instance();
        size_t rows = src_col.size();
        for (size_t i = 0; i < rows; ++i)
        {
            DB::Field from_day_value;
            src_col.get(i, from_day_value);
            if (from_day_value.isNull())
            {
                dst_col->insertDefault();
            }
            else
            {
                dst_col->insert(transformer.execute(
                    static_cast<typename DateType::FieldType>(from_day_value.safeGet<typename DateType::FieldType>()),
                    next_weekday,
                    time_zone));
            }
        }
    }

    template <typename Transformer>
    static void execute(const DB::IColumn & src_col, const DB::IColumn & weekday_col, DB::IColumn * dst_col, Transformer transformer = {})
    {
        size_t rows = src_col.size();
        const auto & time_zone = DateLUT::instance();
        for (size_t i = 0; i < rows; ++i)
        {
            DB::Field from_day_value;
            DB::Field weekday_value;
            src_col.get(i, from_day_value);
            weekday_col.get(i, weekday_value);
            if (from_day_value.isNull() || weekday_value.isNull())
            {
                dst_col->insertDefault();
            }
            else
            {
                auto next_weekday = SparkFunctionNextDay::getDayOfWeek(weekday_value.safeGet<String>());
                if (next_weekday < 0) [[unlikely]]
                {
                    dst_col->insertDefault();
                }
                else
                {
                    dst_col->insert(transformer.execute(
                        static_cast<typename DateType::FieldType>(from_day_value.safeGet<typename DateType::FieldType>()),
                        next_weekday,
                        time_zone));
                }
            }
        }
    }
};

DB::ColumnPtr SparkFunctionNextDay::executeConst(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type) const
{
    const auto * from_date_col = arguments[0].column.get();

    auto nested_result_type = DB::removeNullable(result_type);
    auto next_week_day = getDayOfWeek((*arguments[1].column)[0].safeGet<String>());
    if (next_week_day == -1) [[unlikely]]
    {
        auto default_date_col = nested_result_type->createColumnConst(from_date_col->size(), 0);
        auto null_map_col = DB::DataTypeUInt8().createColumnConst(from_date_col->size(), 1)->convertToFullColumnIfConst();
        return DB::ColumnNullable::create(default_date_col, null_map_col);
    }

    auto to_date_col = result_type->createColumn();
    to_date_col->reserve(from_date_col->size());
    DB::WhichDataType ty_which(nested_result_type);
    if (ty_which.isDate())
    {
        NextDayConstImpl<DB::DataTypeDate>::execute(*from_date_col, next_week_day, to_date_col.get(), NextDayConstTransformer{});
    }
    else if (ty_which.isDate32())
    {
        NextDayConstImpl<DB::DataTypeDate32>::execute(*from_date_col, next_week_day, to_date_col.get(), NextDayConstTransformer{});
    }
    else if (ty_which.isDateTime())
    {
        NextDayConstImpl<DB::DataTypeDateTime>::execute(*from_date_col, next_week_day, to_date_col.get(), NextDayConstTransformer{});
    }
    else if (ty_which.isDateTime64())
    {
        NextDayConstImpl<DB::DataTypeDateTime64>::execute(
            *from_date_col,
            next_week_day,
            to_date_col.get(),
            DB::TransformDateTime64<NextDayConstTransformer>{
                assert_cast<const DB::DataTypeDateTime64 *>(nested_result_type.get())->getScale()});
    }
    return std::move(to_date_col);
}

DB::ColumnPtr SparkFunctionNextDay::executeGeneral(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type) const
{
    const auto * from_date_col = arguments[0].column.get();
    const auto * weekday_col = arguments[1].column.get();
    auto nested_result_type = DB::removeNullable(result_type);
    auto to_date_col = result_type->createColumn();
    to_date_col->reserve(from_date_col->size());
    DB::WhichDataType ty_which(nested_result_type);
    if (ty_which.isDate())
    {
        NextDayConstImpl<DB::DataTypeDate>::execute(*from_date_col, *weekday_col, to_date_col.get(), NextDayConstTransformer{});
    }
    else if (ty_which.isDate32())
    {
        NextDayConstImpl<DB::DataTypeDate32>::execute(*from_date_col, *weekday_col, to_date_col.get(), NextDayConstTransformer{});
    }
    else if (ty_which.isDateTime())
    {
        NextDayConstImpl<DB::DataTypeDateTime>::execute(*from_date_col, *weekday_col, to_date_col.get(), NextDayConstTransformer{});
    }
    else if (ty_which.isDateTime64())
    {
        NextDayConstImpl<DB::DataTypeDateTime64>::execute(
            *from_date_col,
            *weekday_col,
            to_date_col.get(),
            DB::TransformDateTime64<NextDayConstTransformer>{
                assert_cast<const DB::DataTypeDateTime64 *>(nested_result_type.get())->getScale()});
    }
    return std::move(to_date_col);
}

Int8 SparkFunctionNextDay::getDayOfWeek(const String & abbr)
{
    String lower_abbr = abbr;
    boost::to_lower(lower_abbr);
    // week mode must be 0
    static const std::unordered_map<String, Int8> abbr2day
        = {{"mo", 1},     {"mon", 1},       {"monday", 1}, {"tu", 2},       {"tue", 2},      {"tuesday", 2}, {"we", 3},
           {"wed", 3},    {"wednesday", 3}, {"th", 4},     {"thu", 4},      {"thursday", 4}, {"fr", 5},      {"fri", 5},
           {"friday", 5}, {"sa", 6},        {"sat", 6},    {"saturday", 6}, {"su", 7},       {"sun", 7},     {"sunday", 7}};
    auto it = abbr2day.find(lower_abbr);
    if (it != abbr2day.end()) [[likely]]
        return it->second;
    return -1;
}

REGISTER_FUNCTION(SparkFunctionNextDay)
{
    factory.registerFunction<SparkFunctionNextDay>();
}
}
