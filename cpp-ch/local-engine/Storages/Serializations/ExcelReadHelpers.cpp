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
#include "ExcelReadHelpers.h"
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/parseDateTimeBestEffort.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PARSE_DATETIME;
}
}

namespace local_engine
{

bool readDateText(LocalDate & date, DB::ReadBuffer & buf, const DB::FormatSettings & settings)
{
    bool is_us_style = settings.date_time_input_format == DB::FormatSettings::DateTimeInputFormat::BestEffortUS;
    return readDateTextWithExcel(date, buf, is_us_style);
}

bool readDateTime64Text(
    DB::DateTime64 & x,
    DB::ReadBuffer & buf,
    const DB::FormatSettings & settings,
    const DateLUTImpl & time_zone,
    const DateLUTImpl & utc_time_zone,
    bool quote)
{
    // scale is set 6, maybe a bug
    int scale = 6;
    auto pr = static_cast<DB::PeekableReadBuffer>(buf);
    DB::PeekableReadBufferCheckpoint checkpoint{pr, false};
    if (readDatetime64TextWithExcel(x, scale, pr, time_zone, settings.csv, quote))
        return true;
    else
        pr.rollbackToCheckpoint();

    switch (settings.date_time_input_format)
    {
        case DB::FormatSettings::DateTimeInputFormat::Basic:
            return tryReadDateTime64Text(x, scale, pr, time_zone);
        case DB::FormatSettings::DateTimeInputFormat::BestEffort:
            return tryParseDateTime64BestEffort(x, scale, pr, time_zone, utc_time_zone);
        case DB::FormatSettings::DateTimeInputFormat::BestEffortUS:
            return tryParseDateTime64BestEffortUS(x, scale, pr, time_zone, utc_time_zone);
    }

    return false;
}

bool readDatetime64TextWithExcel(
    DB::DateTime64 & datetime64,
    UInt32 scale,
    DB::ReadBuffer & buf,
    const DateLUTImpl & time_zone,
    const DB::FormatSettings::CSV & settings,
    bool quote)
{
    /// Support more format.
    /// Only parser for below:
    ///           yyyy-MM-dd HH:mm:ss,SSS
    ///           yyyy-MM-dd'T'HH:mm:ss
    ///           yyyy-MM-dd'T'HH:mm:ss.SSS
    ///           yyyy-MM-dd'T'HH:mm:ss'Z'
    ///           yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    /// Other will fallback to ch read.
    /// The whole value is in buffer.

    UInt16 year = 0;
    UInt8 month = 0;
    UInt8 day = 0;
    UInt8 hour = 0;
    UInt8 minute = 0;
    UInt8 second = 0;

    char year_digits[std::numeric_limits<UInt64>::digits10];
    size_t num_year_digits = readDigits(year_digits, sizeof(year_digits), buf);

    if (num_year_digits != 4) // yyyyMM
        return false;

    readDecimalNumber<4>(year, year_digits);

    char delimiter_after_year = *buf.position();
    ++buf.position();

    char month_digits[std::numeric_limits<UInt64>::digits10];
    size_t num_month_digits = readDigits(month_digits, sizeof(month_digits), buf);

    if (num_month_digits != 2)
        return false;

    readDecimalNumber<2>(month, month_digits);

    if (*buf.position() != delimiter_after_year) // delimiter must same char
        return false;

    ++buf.position();
    char day_digits[std::numeric_limits<UInt64>::digits10];
    size_t num_day_digits = readDigits(day_digits, sizeof(day_digits), buf);

    if (num_day_digits != 2)
        return false;

    readDecimalNumber<2>(day, day_digits);

    char delimiter_after_day = *buf.position();

    if (delimiter_after_day != ' ' && delimiter_after_day != '\'')
        return false;

    ++buf.position();

    /// 'T'
    if (*buf.position() == 'T')
    {
        ++buf.position();
        if (delimiter_after_day != *buf.position())
            return false;

        ++buf.position();
    }

    if (!readNumber<2>(buf, hour))
        return false;

    if (*buf.position() != ':')
        return false;

    ++buf.position();
    if (!readNumber<2>(buf, minute))
        return false;

    if (*buf.position() != ':')
        return false;

    ++buf.position();
    if (!readNumber<2>(buf, second))
        return false;

    /// .SSS'Z'
    /// if not has quote, not allow ',' after 'ss'
    DB::DateTime64::NativeType fractional = 0;
    bool allow_comma = (settings.delimiter == ',' && quote) || (!quote && settings.delimiter != ',');
    if (!buf.eof() && (*buf.position() == '.' || (allow_comma && *buf.position() == ',')))
    {
        ++buf.position();

        /// Read digits, up to 'scale' positions.
        for (size_t i = 0; i < scale; ++i)
            if (!buf.eof() && isNumericASCII(*buf.position()))
            {
                fractional *= 10;
                fractional += *buf.position() - '0';
                ++buf.position();
            }
            else
                fractional *= 10;
    }

    if (!buf.eof() && buf.position() + 3 <= buf.buffer().end())
    {
        /// ignore 'Z'
        if (buf.position()[0] == '\'' && buf.position()[1] == 'Z' && buf.position()[2] == '\'')
            buf.position() = buf.position() + 3;
    }

    if (!day)
        day = 1;

    if (!checkDate(year, month, day))
        return false;

    time_t datetime = time_zone.makeDateTime(year, month, day, hour, minute, second);
    return DB::DecimalUtils::tryGetDecimalFromComponents<DB::DateTime64>(datetime, fractional, scale, datetime64);
}

inline bool readDateTextWithExcel(LocalDate & date, DB::ReadBuffer & buf, bool is_us_style)
{
    if (buf.eof())
        return false;

    /// Support more format include MM/dd/yyyy, yyyyMM, yyyy-MM, yyyy/MM.
    /// The whole value is in buffer.
    /// The delimiters can be arbitrary characters, like YYYY!MM, but obviously not digits.
    UInt16 year = 0;
    UInt8 month = 0;
    UInt8 day = 0;

    char first_digits[std::numeric_limits<UInt64>::digits10];
    size_t num_first_digits = readDigits(first_digits, sizeof(first_digits), buf);

    if (num_first_digits == 8) // yyyyMMdd
    {
        readDecimalNumber<4>(year, first_digits);
        readDecimalNumber<2>(month, first_digits + 4);
        readDecimalNumber<2>(day, first_digits + 6);
    }
    else if (num_first_digits == 6) // yyyyMM
    {
        readDecimalNumber<4>(year, first_digits);
        readDecimalNumber<2>(month, first_digits + 4);
    }
    else if (num_first_digits == 4) // yyyy-MM, yyyy/MM, yyyy.M, yyyyMMdd, yyyy.M.d
    {
        readDecimalNumber<4>(year, first_digits);
        char delimiter_after_year = *buf.position();
        ++buf.position();

        char month_digits[std::numeric_limits<UInt64>::digits10];
        size_t num_month_digits = readDigits(month_digits, sizeof(month_digits), buf);

        /// incorrect: yyyy-MMM
        if (num_month_digits == 1)
            readDecimalNumber<1>(month, month_digits);
        else if (num_month_digits == 2)
            readDecimalNumber<2>(month, month_digits);
        else
            return false;

        /// yyyy-MM-xx fallback to ch parser
        if (!buf.eof() && *buf.position() == delimiter_after_year)
        {
            ++buf.position();

            char day_digits[std::numeric_limits<UInt64>::digits10];
            size_t num_day_digits = readDigits(day_digits, sizeof(day_digits), buf);
            /// incorrect: yyyy-MM-ddd
            if (num_day_digits == 1)
                readDecimalNumber<1>(day, day_digits);
            else if (num_day_digits == 2)
                readDecimalNumber<2>(day, day_digits);
            else
                return false;
        }
    }
    else if (is_us_style)
    {
        /// MM/dd/yyyy, M.d.yyyy
        if (num_first_digits != 1 && num_first_digits != 2)
            return false;

        /// MM/dd/yyyy: 01/01/2023 or 1/1/2023
        if (num_first_digits == 1)
            readDecimalNumber<1>(month, first_digits);
        else
            readDecimalNumber<2>(month, first_digits);

        char delimiter_after_year = *buf.position();
        ++buf.position();


        char day_digits[std::numeric_limits<UInt64>::digits10];
        size_t num_day_digits = readDigits(day_digits, sizeof(day_digits), buf);

        if (num_day_digits == 1)
            readDecimalNumber<1>(day, day_digits);
        else if (num_day_digits == 2)
            readDecimalNumber<2>(day, day_digits);
        else
            return false;

        // incorrect: MM/dd-yyyy, MM/dd
        if (buf.eof() || delimiter_after_year != *buf.position())
            return false;

        ++buf.position();

        char year_digits[std::numeric_limits<UInt64>::digits10];
        size_t num_year_digits = readDigits(year_digits, sizeof(year_digits), buf);

        if (num_year_digits != 4)
            return false;

        readDecimalNumber<4>(year, year_digits);
    }
    else
        return false;

    if (!day)
        day = 1;

    date = LocalDate(year, month, day);
    return true;
}

}
