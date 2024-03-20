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
#include <unordered_map>
#include <Functions/SparkFunctionGetJsonObject.h>
#include <Interpreters/Context_fwd.h>
#include <Parser/SerializedPlanParser.h>
#include <substrait/algebra.pb.h>


namespace local_engine
{
class RelRewriter
{
public:
    RelRewriter(SerializedPlanParser * parser_) : parser(parser_) { }
    virtual ~RelRewriter() = default;
    virtual void rewrite(substrait::Rel & rel) = 0;

protected:
    SerializedPlanParser * parser;

    inline DB::ContextPtr getContext() { return parser->context; }
    inline std::unordered_map<std::string, std::string> & getFunctionMapping() { return parser->function_mapping; }
};

}
