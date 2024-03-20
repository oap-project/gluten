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

#include <jni.h>

namespace local_engine
{
template <typename T>
class SharedPointerWrapper
{
    std::shared_ptr<T> pointer;

public:
    template <typename... ARGS>
    explicit SharedPointerWrapper(ARGS... a)
    {
        pointer = std::make_shared<T>(a...);
    }

    explicit SharedPointerWrapper(std::shared_ptr<T> obj) { pointer = obj; }

    virtual ~SharedPointerWrapper() noexcept = default;

    jlong instance() const { return reinterpret_cast<jlong>(this); }

    std::shared_ptr<T> get() const { return pointer; }

    static std::shared_ptr<T> sharedPtr(jlong handle) { return cast(handle)->get(); }

    static void dispose(jlong handle)
    {
        auto obj = cast(handle);
        delete obj;
    }

private:
    static SharedPointerWrapper<T> * cast(jlong handle) { return reinterpret_cast<SharedPointerWrapper<T> *>(handle); }
};
template <typename T>
SharedPointerWrapper<T> * make_wrapper(std::shared_ptr<T> obj)
{
    return new SharedPointerWrapper<T>(obj);
}
}
