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

#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/LocalDate.h>
#include <Common/StringUtils/StringUtils.h>
#include "ExcelNumberReader.h"

namespace local_engine
{

inline bool checkDate(const UInt16 & year, const UInt8 & month_, const UInt8 & day_)
{
    auto is_leap_year_ = (year % 400 == 0) || (year % 100 != 0 && year % 4 == 0);

    if ((month_ == 1 || month_ == 3 || month_ == 5 || month_ == 7 || month_ == 8 || month_ == 10 || month_ == 12) && day_ >= 1
        && day_ <= 31)
        return true;
    else if (month_ == 2 && ((is_leap_year_ && day_ >= 1 && day_ <= 29) || (!is_leap_year_ && day_ >= 1 && day_ <= 28)))
        return true;
    else if ((month_ == 4 || month_ == 6 || month_ == 9 || month_ == 11) && day_ >= 1 && day_ <= 30)
        return true;
    return false;
}

inline size_t readDigits(char * res, size_t max_chars, DB::ReadBuffer & in)
{
    size_t num_chars = 0;
    while (!in.eof() && isNumericASCII(*in.position()) && num_chars < max_chars)
    {
        res[num_chars] = *in.position() - '0';
        ++num_chars;
        ++in.position();
    }
    return num_chars;
}

template <size_t digit, size_t power_of_ten, typename T>
inline void readDecimalNumberImpl(T & res, const char * src)
{
    res += src[digit] * power_of_ten;
    if constexpr (digit > 0)
        readDecimalNumberImpl<digit - 1, power_of_ten * 10>(res, src);
}

template <size_t num_digits, typename T>
inline void readDecimalNumber(T & res, const char * src)
{
    readDecimalNumberImpl<num_digits - 1, 1>(res, src);
}

template <size_t num_digits, typename T>
inline bool readNumber(DB::ReadBuffer & buf, T & res)
{
    char digits[std::numeric_limits<UInt64>::digits10];
    size_t read_num_digits = readDigits(digits, sizeof(digits), buf);

    if (read_num_digits != num_digits)
        return false;

    readDecimalNumber<num_digits>(res, digits);
    return true;
}


bool readDatetime64TextWithExcel(
    DB::DateTime64 & datetime64,
    UInt32 scale,
    DB::ReadBuffer & buf,
    const DateLUTImpl & time_zone,
    const DB::FormatSettings::CSV & settings,
    bool quote);
bool readDateTime64Text(
    DB::DateTime64 & x,
    DB::ReadBuffer & buf,
    const DB::FormatSettings & settings,
    const DateLUTImpl & time_zone,
    const DateLUTImpl & utc_time_zone,
    bool quote);

bool readDateTextWithExcel(LocalDate & date, DB::ReadBuffer & buf, bool is_us_style);
bool readDateText(LocalDate & date, DB::ReadBuffer & buf, const DB::FormatSettings & settings);


template <typename T>
inline bool readExcelIntegerText(T & x, DB::ReadBuffer & buf, bool has_quote, const DB::FormatSettings & settings)
{
    if constexpr (std::is_same_v<decltype(x), bool &>)
    {
        readBoolText(x, buf);
        return true;
    }
    else
        return readExcelIntTextImpl(x, buf, has_quote, settings);
}

inline bool readExcelText(is_floating_point auto & x, DB::ReadBuffer & buf, bool has_quote, const DB::FormatSettings & settings)
{
    return readExcelFloatTextFastImpl(x, buf, has_quote, settings);
}

inline bool readExcelText(is_integer auto & x, DB::ReadBuffer & buf, bool has_quote, const DB::FormatSettings & settings)
{
    return readExcelIntegerText(x, buf, has_quote, settings);
}

inline bool readExcelText(LocalDate & x, DB::ReadBuffer & buf, bool /*has_quote*/, const DB::FormatSettings & settings)
{
    return readDateText(x, buf, settings);
}

/// CSV, for numbers, dates: quotes are optional, no special escaping rules.
template <typename T>
bool readCSVSimple(T & x, DB::ReadBuffer & buf, const DB::FormatSettings & settings)
{
    if (buf.eof())
        DB::throwReadAfterEOF();

    char maybe_quote = *buf.position();
    bool has_quote = false;
    if ((settings.csv.allow_single_quotes && maybe_quote == '\'') || (settings.csv.allow_double_quotes && maybe_quote == '\"'))
    {
        has_quote = true;
        ++buf.position();
    }

    /// deal empty string ""
    if ((has_quote && !buf.eof() && *buf.position() == maybe_quote)
        || (!has_quote && !buf.eof() && (*buf.position() == settings.csv.delimiter || *buf.position() == '\n' || *buf.position() == '\r')))
        return false;

    bool result = readExcelText(x, buf, has_quote, settings);
    if (!result)
        return false;

    if (has_quote)
        assertChar(maybe_quote, buf);

    while (!buf.eof() && *buf.position() == ' ')
    {
        //ignore end whitespace
        ++buf.position();
    }


    if (!buf.eof() && (*buf.position() != settings.csv.delimiter && *buf.position() != '\n' && *buf.position() != '\r'))
        return false;

    return true;
}


template <typename T>
requires is_arithmetic_v<T>
inline bool readCSV(T & x, DB::ReadBuffer & buf, const DB::FormatSettings & settings)
{
    return readCSVSimple(x, buf, settings);
}

inline bool readCSV(LocalDate & x, DB::ReadBuffer & buf, const DB::FormatSettings & settings)
{
    return readCSVSimple(x, buf, settings);
}

}
