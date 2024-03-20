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

#include <functional>
#include <memory>
#include <Interpreters/Context_fwd.h>
#include <boost/core/noncopyable.hpp>
namespace local_engine
{
class WriteBufferBuilder
{
public:
    explicit WriteBufferBuilder(DB::ContextPtr context_) : context(context_) { }
    virtual ~WriteBufferBuilder() = default;
    /// build a new write buffer
    virtual std::unique_ptr<DB::WriteBuffer> build(const std::string & file_uri_) = 0;

protected:
    DB::ContextPtr context;
};

using WriteBufferBuilderPtr = std::shared_ptr<WriteBufferBuilder>;

class WriteBufferBuilderFactory : public boost::noncopyable
{
public:
    using NewBuilder = std::function<WriteBufferBuilderPtr(DB::ContextPtr)>;
    static WriteBufferBuilderFactory & instance();
    WriteBufferBuilderPtr createBuilder(const String & schema, DB::ContextPtr context);

    void registerBuilder(const String & schema, NewBuilder newer);

private:
    std::map<String, NewBuilder> builders;
};

void registerWriteBufferBuilders();
}
