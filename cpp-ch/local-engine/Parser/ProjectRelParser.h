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
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Parser/RelParser.h>
#include <Poco/Logger.h>

namespace local_engine
{
class ProjectRelParser : public RelParser
{
public:
    struct SplittedActionsDAGs
    {
        ActionsDAGPtr before_array_join; /// Optional
        ActionsDAGPtr array_join;
        ActionsDAGPtr after_array_join; /// Optional
    };

    explicit ProjectRelParser(SerializedPlanParser * plan_paser_);
    ~ProjectRelParser() override = default;

    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;

private:
    Poco::Logger * logger = &Poco::Logger::get("ProjectRelParser");

    DB::QueryPlanPtr parseProject(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_);
    DB::QueryPlanPtr parseGenerate(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_);

    static const DB::ActionsDAG::Node * findArrayJoinNode(ActionsDAGPtr actions_dag);

    /// Split actions_dag of generate rel into 3 parts: before array join + during array join + after array join
    static SplittedActionsDAGs splitActionsDAGInGenerate(ActionsDAGPtr actions_dag);


    const substrait::Rel & getSingleInput(const substrait::Rel & rel) override
    {
        if (rel.has_generate())
            return rel.generate().input();
        else
            return rel.project().input();
    }
};
}
