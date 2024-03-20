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

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDecimalArithmetics.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
struct DivideDecimalsImpl
{
    static constexpr auto name = "sparkDivideDecimal";
    template <typename FirstType, typename SecondType>
    static inline Decimal256 execute(FirstType a, SecondType b, UInt16 scale_a, UInt16 scale_b, UInt16 result_scale)
    {
        if (a.value == 0 || b.value == 0)
            return Decimal256(0);

        Int256 sign_a = a.value < 0 ? -1 : 1;
        Int256 sign_b = b.value < 0 ? -1 : 1;

        std::vector<UInt8> a_digits = DecimalOpHelpers::toDigits(a.value * sign_a);

        while (scale_a < scale_b + result_scale)
        {
            a_digits.push_back(0);
            ++scale_a;
        }

        while (scale_a > scale_b + result_scale && !a_digits.empty())
        {
            a_digits.pop_back();
            --scale_a;
        }

        if (a_digits.empty())
            return Decimal256(0);

        std::vector<UInt8> divided = DecimalOpHelpers::divide(a_digits, b.value * sign_b);

        if (divided.size() > DecimalUtils::max_precision<Decimal256>)
            throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow: result bigger that Decimal256");
        return Decimal256(sign_a * sign_b * DecimalOpHelpers::fromDigits(divided));
    }
};

template <typename Transform>
class SparkFunctionDecimalDivide : public FunctionsDecimalArithmetics<Transform>
{
public:
    static constexpr auto name = Transform::name;
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionDecimalDivide>(); }
    SparkFunctionDecimalDivide() = default;
    ~SparkFunctionDecimalDivide() override = default;
    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return makeNullable(FunctionsDecimalArithmetics<Transform>::getReturnTypeImpl(arguments));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must have 2 arugments.", name);

        ColumnPtr res_col = nullptr;
        MutableColumnPtr null_map_col = ColumnUInt8::create(input_rows, 0);
        auto getNonNullableColumn = [&](const ColumnPtr & col) -> const ColumnPtr
        {
            if (col->isNullable())
            {
                auto * nullable_col = checkAndGetColumn<const ColumnNullable>(col.get());
                return nullable_col->getNestedColumnPtr();
            }
            else
                return col;
        };

        ColumnWithTypeAndName new_arg0{getNonNullableColumn(arguments[0].column), removeNullable(arguments[0].type), arguments[0].name};
        ColumnWithTypeAndName new_arg1{getNonNullableColumn(arguments[1].column), removeNullable(arguments[1].type), arguments[0].name};
        ColumnsWithTypeAndName new_args{new_arg0, new_arg1};
        bool arg_type_valid = true;

        if (isDecimal(new_arg1.type))
        {
            using Types = TypeList<DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128, DataTypeDecimal256>;
            arg_type_valid = castTypeToEither(
                Types{},
                new_arg1.type.get(),
                [&](const auto & right_)
                {
                    using R = typename std::decay_t<decltype(right_)>::FieldType;
                    const ColumnDecimal<R> * const_col_right = checkAndGetColumnConstData<ColumnDecimal<R>>(new_arg1.column.get());
                    if (const_col_right && const_col_right->getElement(0).value == 0)
                    {
                        null_map_col = ColumnUInt8::create(input_rows, 1);
                        res_col = ColumnDecimal<Decimal256>::create(input_rows, 0);
                    }
                    else
                        res_col = FunctionsDecimalArithmetics<Transform>::executeImpl(new_args, removeNullable(result_type), input_rows);

                    if (!const_col_right)
                    {
                        const ColumnDecimal<R> * col_right = assert_cast<const ColumnDecimal<R> *>(new_arg1.column.get());
                        PaddedPODArray<UInt8> & null_map = assert_cast<ColumnVector<UInt8> *>(null_map_col.get())->getData();
                        for (size_t i = 0; i < col_right->size(); ++i)
                            null_map[i] = (col_right->getElement(i).value == 0 || arguments[1].column->isNullAt(i));
                    }
                    return true;
                });
        }
        else if (isNumber(new_arg1.type))
        {
            using Types = TypeList<
                DataTypeFloat32,
                DataTypeFloat64,
                DataTypeUInt8,
                DataTypeUInt16,
                DataTypeUInt32,
                DataTypeUInt64,
                DataTypeUInt128,
                DataTypeUInt256,
                DataTypeInt8,
                DataTypeInt16,
                DataTypeInt32,
                DataTypeInt64,
                DataTypeInt128,
                DataTypeInt256>;
            arg_type_valid = castTypeToEither(
                Types{},
                new_arg1.type.get(),
                [&](const auto & right_)
                {
                    using R = typename std::decay_t<decltype(right_)>::FieldType;
                    const ColumnVector<R> * const_col_right = checkAndGetColumnConstData<ColumnVector<R>>(new_arg1.column.get());
                    if (const_col_right && const_col_right->getElement(0) == 0)
                    {
                        null_map_col = ColumnUInt8::create(input_rows, 1);
                        res_col = ColumnDecimal<Decimal256>::create(input_rows, 0);
                    }
                    else
                        res_col = FunctionsDecimalArithmetics<Transform>::executeImpl(new_args, removeNullable(result_type), input_rows);

                    if (!const_col_right)
                    {
                        const ColumnVector<R> * col_right = assert_cast<const ColumnVector<R> *>(new_arg1.column.get());
                        PaddedPODArray<UInt8> & null_map = assert_cast<ColumnVector<UInt8> *>(null_map_col.get())->getData();
                        for (size_t i = 0; i < col_right->size(); ++i)
                            null_map[i] = (col_right->getElement(i) == 0 || arguments[1].column->isNullAt(i));
                    }
                    return true;
                });
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s arguments type must be numbeic", name);

        if (!arg_type_valid)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s arguments type is not valid.", name);

        return ColumnNullable::create(res_col, std::move(null_map_col));
    }
};
}
