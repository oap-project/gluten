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
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>


namespace local_engine
{
using namespace DB;


template <typename Reader>
static inline void excelRead(IColumn & column, Reader && reader)
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try
    {
        reader(data);
        data.push_back(0);
        offsets.push_back(data.size());
    }
    catch (...)
    {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}

template <typename Vector, bool include_quotes = false>
void readExcelCSVQuoteString(Vector & s, ReadBuffer & buf, const char delimiter, const String & escape_value, const char & quote);
template <typename Vector>
void readExcelCSVStringInto(Vector & s, ReadBuffer & buf, const FormatSettings::CSV & settings, const String & escape_value);


void deserializeExcelStringTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const String & escape_value);


}
