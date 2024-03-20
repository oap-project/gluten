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

#include <exception>
#include <jni.h>
#include <boost/core/noncopyable.hpp>
#include <boost/stacktrace.hpp>
#include <Common/Exception.h>

#include <sstream>
namespace local_engine
{
class JniErrorsGlobalState : boost::noncopyable
{
protected:
    JniErrorsGlobalState() = default;

public:
    ~JniErrorsGlobalState() = default;

    static JniErrorsGlobalState & instance();
    void initialize(JNIEnv * env_);
    void destroy(JNIEnv * env);

    inline jclass getIOExceptionClass() { return io_exception_class; }
    inline jclass getRuntimeExceptionClass() { return runtime_exception_class; }
    inline jclass getUnsupportedOperationExceptionClass() { return unsupportedoperation_exception_class; }
    inline jclass getIllegalAccessExceptionClass() { return illegal_access_exception_class; }
    inline jclass getIllegalArgumentExceptionClass() { return illegal_argument_exception_class; }

    void throwException(JNIEnv * env, const DB::Exception & e);
    void throwException(JNIEnv * env, const std::exception & e);
    static void throwException(JNIEnv * env, jclass exception_class, const std::string & message, const std::string & stack_trace = "");
    void throwRuntimeException(JNIEnv * env, const std::string & message, const std::string & stack_trace = "");


private:
    jclass io_exception_class = nullptr;
    jclass runtime_exception_class = nullptr;
    jclass unsupportedoperation_exception_class = nullptr;
    jclass illegal_access_exception_class = nullptr;
    jclass illegal_argument_exception_class = nullptr;
};
//

#define LOCAL_ENGINE_JNI_METHOD_START \
    try \
    {
#define LOCAL_ENGINE_JNI_METHOD_END(env, ret) \
    } \
    catch (DB::Exception & e) \
    { \
        local_engine::JniErrorsGlobalState::instance().throwException(env, e); \
        return ret; \
    } \
    catch (std::exception & e) \
    { \
        local_engine::JniErrorsGlobalState::instance().throwException(env, e); \
        return ret; \
    } \
    catch (...) \
    { \
        DB::WriteBufferFromOwnString ostr; \
        auto trace = boost::stacktrace::stacktrace(); \
        boost::stacktrace::detail::to_string(&trace.as_vector()[0], trace.size()); \
        local_engine::JniErrorsGlobalState::instance().throwRuntimeException(env, "Unknown Exception", ostr.str().c_str()); \
        return ret; \
    }
}
