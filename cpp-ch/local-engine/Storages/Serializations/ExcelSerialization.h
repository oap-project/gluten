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

#include <DataTypes/Serializations/ISerialization.h>
#include <base/extended_types.h>
#include <Common/DateLUTImpl.h>
namespace DB
{
namespace ErrorCodes
{
extern const int ATTEMPT_TO_READ_AFTER_EOF;
}
}

namespace local_engine
{
using namespace DB;


class ExcelSerialization final : public DB::ISerialization
{
public:
    explicit ExcelSerialization(const SerializationPtr & nested_, String escape_) : nested_ptr(nested_), escape(escape_) { }

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeWholeText(IColumn &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeTextEscaped(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeTextEscaped(IColumn &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeTextQuoted(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeTextQuoted(IColumn &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeTextJSON(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeTextJSON(IColumn &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeTextCSV(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

private:
    void deserializeDate32TextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const;

    template <typename T>
    requires is_arithmetic_v<T>
    void deserializeNumberTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    template <typename T>
    void deserializeDatetimeTextCSV(
        IColumn & column,
        ReadBuffer & istr,
        const FormatSettings & settings,
        const DateLUTImpl & time_zone,
        const DateLUTImpl & utc_time_zone) const;

private:
    SerializationPtr nested_ptr;
    String escape;
};
}
