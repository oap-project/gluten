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
package org.apache.gluten.memory.memtarget.spark;

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.memory.MemoryUsageStatsBuilder;
import org.apache.gluten.memory.memtarget.Spiller;
import org.apache.gluten.memory.memtarget.TreeMemoryTarget;

import org.apache.commons.collections.map.ReferenceMap;
import org.apache.spark.memory.TaskMemoryManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TreeMemoryConsumers {

  private static final Map<Long, Factory> FACTORIES = new ConcurrentHashMap<>();

  private TreeMemoryConsumers() {}

  private static Factory createOrGetFactory(long perTaskCapacity) {
    return FACTORIES.computeIfAbsent(perTaskCapacity, Factory::new);
  }

  /**
   * A hub to provide memory target instances whose shared size (in the same task) is limited to X,
   * X = executor memory / task slots.
   *
   * <p>Using this to prevent OOMs if the delegated memory target could possibly hold large memory
   * blocks that are not spillable.
   *
   * <p>See <a href="https://github.com/oap-project/gluten/issues/3030">GLUTEN-3030</a>
   */
  public static Factory isolated() {
    return createOrGetFactory(GlutenConfig.getConf().conservativeTaskOffHeapMemorySize());
  }

  /**
   * This works as a legacy Spark memory consumer which grants as much as possible of memory
   * capacity to each task.
   */
  public static Factory shared() {
    return createOrGetFactory(TreeMemoryTarget.CAPACITY_UNLIMITED);
  }

  public static class Factory {

    private static final ReferenceMap MAP = new ReferenceMap(ReferenceMap.WEAK, ReferenceMap.WEAK);
    private final long perTaskCapacity;

    private Factory(long perTaskCapacity) {
      this.perTaskCapacity = perTaskCapacity;
    }

    @SuppressWarnings("unchecked")
    private TreeMemoryTarget getSharedAccount(TaskMemoryManager tmm) {
      synchronized (MAP) {
        return (TreeMemoryTarget)
            MAP.computeIfAbsent(
                tmm,
                m -> {
                  TreeMemoryTarget tmc = new TreeMemoryConsumer((TaskMemoryManager) m);
                  return tmc.newChild(
                      "root", perTaskCapacity, Collections.emptyList(), Collections.emptyMap());
                });
      }
    }

    public TreeMemoryTarget newConsumer(
        TaskMemoryManager tmm,
        String name,
        List<Spiller> spillers,
        Map<String, MemoryUsageStatsBuilder> virtualChildren) {
      TreeMemoryTarget account = getSharedAccount(tmm);
      return account.newChild(
          name, TreeMemoryConsumer.CAPACITY_UNLIMITED, spillers, virtualChildren);
    }
  }
}
