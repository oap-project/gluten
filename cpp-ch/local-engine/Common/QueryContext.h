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
#include <Interpreters/Context_fwd.h>
#include <jni/ReservationListenerWrapper.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

namespace local_engine
{
int64_t initializeQuery(ReservationListenerWrapperPtr listener);

void releaseAllocator(int64_t allocator_id);

int64_t allocatorMemoryUsage(int64_t allocator_id);

struct NativeAllocatorContext
{
    std::shared_ptr<DB::CurrentThread::QueryScope> query_scope;
    std::shared_ptr<DB::ThreadStatus> thread_status;
    DB::ContextMutablePtr query_context;
    std::shared_ptr<DB::ThreadGroup> group;
    ReservationListenerWrapperPtr listener;
};

using NativeAllocatorContextPtr = std::shared_ptr<NativeAllocatorContext>;

NativeAllocatorContextPtr getAllocator(int64_t allocator);
}
