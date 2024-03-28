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
package org.apache.gluten.memory.alloc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Built-in toolkit for managing native memory allocations. To use the facility, one should import
 * Gluten's C++ library then create the c++ instance using following example code:
 *
 * <p>```c++ auto* allocator = reinterpret_cast<gluten::memory::MemoryAllocator*>(allocator_id); ```
 *
 * <p>The ID "allocator_id" can be retrieved from Java API {@link
 * NativeMemoryAllocator#getNativeInstanceId()}.
 *
 * <p>FIXME: to export the native APIs in a standard way
 */
public final class NativeMemoryAllocators {
  private static final Map<NativeMemoryAllocator.Type, NativeMemoryAllocators> INSTANCES =
      new ConcurrentHashMap<>();

  private final NativeMemoryAllocator allocator;

  private NativeMemoryAllocators(NativeMemoryAllocator.Type type) {
    allocator = NativeMemoryAllocator.create(type);
  }

  public static NativeMemoryAllocators getDefault() {
    return forType(NativeMemoryAllocator.Type.DEFAULT);
  }

  private static NativeMemoryAllocators forType(NativeMemoryAllocator.Type type) {
    return INSTANCES.computeIfAbsent(type, NativeMemoryAllocators::new);
  }

  public NativeMemoryAllocator get() {
    return allocator;
  }
}
