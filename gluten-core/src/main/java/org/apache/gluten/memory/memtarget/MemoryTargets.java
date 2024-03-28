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

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.memory.MemoryUsageStatsBuilder;
import org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumers;

import org.apache.spark.memory.TaskMemoryManager;

import java.util.List;
import java.util.Map;

public final class MemoryTargets {

  private MemoryTargets() {
    // enclose factory ctor
  }

  public static MemoryTarget throwOnOom(MemoryTarget target) {
    return new ThrowOnOomMemoryTarget(target);
  }

  public static MemoryTarget overAcquire(
      MemoryTarget target, MemoryTarget overTarget, double overAcquiredRatio) {
    if (overAcquiredRatio == 0.0D) {
      return target;
    }
    return new OverAcquire(target, overTarget, overAcquiredRatio);
  }

  public static MemoryTarget newConsumer(
      TaskMemoryManager tmm,
      String name,
      List<Spiller> spillers,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    final TreeMemoryConsumers.Factory factory;
    if (GlutenConfig.getConf().memoryIsolation()) {
      factory = TreeMemoryConsumers.isolated();
    } else {
      factory = TreeMemoryConsumers.shared();
    }
    return factory.newConsumer(tmm, name, spillers, virtualChildren);
  }
}
