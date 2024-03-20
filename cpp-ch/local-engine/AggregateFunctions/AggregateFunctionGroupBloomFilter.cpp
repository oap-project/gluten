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
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>

#include <AggregateFunctions/AggregateFunctionGroupBloomFilter.h>
#include "Core/TypeId.h"
#include "base/types.h"


namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
using namespace DB;

AggregateFunctionPtr
createAggregateFunctionBloomFilter(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);
    auto arg_type = argument_types[0]->getTypeId();
    if (arg_type != TypeIndex::Int64 && arg_type != TypeIndex::UInt64)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument for aggregate function {} must be Int64 or UInt64, but it has type {}",
            name,
            argument_types[0]->getName());

    UInt64 filter_size = 100;
    UInt64 filter_hashes = 2;
    UInt64 seed = 0;

    if (parameters.size() == 3)
    {
        auto get_parameter = [&](size_t i)
        {
            auto type = parameters[i].getType();
            if (type != Field::Types::Int64 && type != Field::Types::UInt64)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be Int64 or UInt64", name);

            if ((type == Field::Types::Int64 && parameters[i].get<Int64>() < 0))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be non-negative number", name);

            return parameters[i].get<UInt64>();
        };

        filter_size = get_parameter(0);
        filter_hashes = get_parameter(1);
        seed = get_parameter(2);
    }
    else if (parameters.empty())
    {
        // No parameter is specified, this is the case in INTERMEDIATE_TO_RESULT phase.
    }
    else
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of parameters for aggregate function {}, should either be 3 or 0",
            name);
    }


    if (arg_type == TypeIndex::Int64)
        return AggregateFunctionPtr(new AggregateFunctionGroupBloomFilter<Int64, AggregateFunctionGroupBloomFilterData>(
            argument_types, parameters, filter_size, filter_hashes, seed));
    else
        return AggregateFunctionPtr(new AggregateFunctionGroupBloomFilter<UInt64, AggregateFunctionGroupBloomFilterData>(
            argument_types, parameters, filter_size, filter_hashes, seed));
}

void registerAggregateFunctionsBloomFilter(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupBloomFilter", createAggregateFunctionBloomFilter);
}

}
