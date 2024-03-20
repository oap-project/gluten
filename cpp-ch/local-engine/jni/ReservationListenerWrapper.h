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
#include <memory>
#include <jni.h>

namespace local_engine
{
class ReservationListenerWrapper
{
public:
    static jclass reservation_listener_class;
    static jmethodID reservation_listener_reserve;
    static jmethodID reservation_listener_reserve_or_throw;
    static jmethodID reservation_listener_unreserve;

    explicit ReservationListenerWrapper(jobject listener);
    ~ReservationListenerWrapper();
    void reserve(int64_t size);
    void reserveOrThrow(int64_t size);
    void free(int64_t size);

private:
    jobject listener;
};
using ReservationListenerWrapperPtr = std::shared_ptr<ReservationListenerWrapper>;
}
