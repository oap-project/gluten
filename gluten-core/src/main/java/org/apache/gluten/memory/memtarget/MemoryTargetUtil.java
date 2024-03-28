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
package org.apache.gluten.memory.memtarget;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class MemoryTargetUtil {
  private MemoryTargetUtil() {}

  private static final Map<String, Integer> UNIQUE_NAME_LOOKUP = new ConcurrentHashMap<>();

  public static String toUniqueName(String name) {
    int nextId =
        UNIQUE_NAME_LOOKUP.compute(
            name, (s, integer) -> Optional.ofNullable(integer).map(id -> id + 1).orElse(0));
    return String.format("%s.%d", name, nextId);
  }
}
