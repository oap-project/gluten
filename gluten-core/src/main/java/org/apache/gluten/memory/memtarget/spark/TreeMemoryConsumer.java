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

import org.apache.gluten.memory.MemoryUsageStatsBuilder;
import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.memory.memtarget.MemoryTargetUtil;
import org.apache.gluten.memory.memtarget.MemoryTargetVisitor;
import org.apache.gluten.memory.memtarget.Spiller;
import org.apache.gluten.memory.memtarget.TreeMemoryTarget;
import org.apache.gluten.memory.memtarget.TreeMemoryTargets;
import org.apache.gluten.proto.MemoryUsageStats;

import com.google.common.base.Preconditions;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is a Spark memory consumer and at the same time a factory to create sub-targets that share
 * one fixed memory capacity.
 *
 * <p>Once full (size > capacity), spillers will be called by the consumer with order. If failed to
 * free out enough memory, throw OOM to caller.
 *
 * <p>Spark's memory manager could either trigger spilling on the children spillers since this was
 * registered as a Spark memory consumer.
 *
 * <p>Typically used by utility class {@link
 * org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumers}.
 */
public class TreeMemoryConsumer extends MemoryConsumer implements TreeMemoryTarget {

  private final SimpleMemoryUsageRecorder recorder = new SimpleMemoryUsageRecorder();
  private final Map<String, TreeMemoryTarget> children = new HashMap<>();
  private final String name = MemoryTargetUtil.toUniqueName("Gluten.Tree");

  TreeMemoryConsumer(TaskMemoryManager taskMemoryManager) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.OFF_HEAP);
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      // or Spark complains about the zero size by throwing an error
      return 0;
    }
    long acquired = acquireMemory(size);
    recorder.inc(acquired);
    return acquired;
  }

  @Override
  public long repay(long size) {
    if (size == 0) {
      return 0;
    }
    long toFree = Math.min(getUsed(), size);
    freeMemory(toFree);
    Preconditions.checkArgument(getUsed() >= 0);
    recorder.inc(-toFree);
    return toFree;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public long usedBytes() {
    return getUsed();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public MemoryUsageStats stats() {
    Set<Map.Entry<String, TreeMemoryTarget>> entries = children.entrySet();
    Map<String, MemoryUsageStats> childrenStats =
        entries.stream()
            .collect(Collectors.toMap(e -> e.getValue().name(), e -> e.getValue().stats()));

    Preconditions.checkState(childrenStats.size() == children.size());
    MemoryUsageStats stats = recorder.toStats(childrenStats);
    Preconditions.checkState(
        stats.getCurrent() == getUsed(),
        "Used bytes mismatch between gluten memory consumer and Spark task memory manager");
    return stats;
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    // subject to the regular Spark spill calls
    return TreeMemoryTargets.spillTree(this, size);
  }

  @Override
  public TreeMemoryTarget newChild(
      String name,
      long capacity,
      List<Spiller> spillers,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    final TreeMemoryTarget child =
        TreeMemoryTargets.newChild(this, name, capacity, spillers, virtualChildren);
    if (children.containsKey(child.name())) {
      throw new IllegalArgumentException("Child already registered: " + child.name());
    }
    children.put(child.name(), child);
    return child;
  }

  @Override
  public Map<String, TreeMemoryTarget> children() {
    return Collections.unmodifiableMap(children);
  }

  @Override
  public TreeMemoryTarget parent() {
    // we are root
    throw new IllegalStateException("Unreachable code");
  }

  @Override
  public List<Spiller> getNodeSpillers() {
    // root doesn't spill
    return Collections.emptyList();
  }

  public TaskMemoryManager getTaskMemoryManager() {
    return taskMemoryManager;
  }
}
