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
package org.apache.gluten.memory.nmm;

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.memory.MemoryUsageRecorder;
import org.apache.gluten.memory.memtarget.MemoryTarget;
import org.apache.gluten.memory.memtarget.MemoryTargets;
import org.apache.gluten.memory.memtarget.Spiller;
import org.apache.gluten.memory.memtarget.Spillers;
import org.apache.gluten.proto.MemoryUsageStats;

import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskResources;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class NativeMemoryManagers {

  // TODO: Let all caller support spill.
  public static NativeMemoryManager contextInstance(String name) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.");
    }
    String id = "NativeMemoryManager:" + name;
    return TaskResources.addResourceIfNotRegistered(
        id, () -> createNativeMemoryManager(name, Collections.emptyList()));
  }

  public static NativeMemoryManager create(String name, Spiller... spillers) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("Spiller must be used in a Spark task.");
    }

    final NativeMemoryManager manager = createNativeMemoryManager(name, Arrays.asList(spillers));
    return TaskResources.addAnonymousResource(manager);
  }

  private static NativeMemoryManager createNativeMemoryManager(
      String name, List<Spiller> spillers) {
    final AtomicReference<NativeMemoryManager> out = new AtomicReference<>();
    // memory target
    final double overAcquiredRatio = GlutenConfig.getConf().memoryOverAcquiredRatio();
    final long reservationBlockSize = GlutenConfig.getConf().memoryReservationBlockSize();
    final TaskMemoryManager tmm = TaskResources.getLocalTaskContext().taskMemoryManager();
    final MemoryTarget target =
        MemoryTargets.throwOnOom(
            MemoryTargets.overAcquire(
                MemoryTargets.newConsumer(
                    tmm,
                    name,
                    // call memory manager's shrink API, if no good then call the spiller
                    Stream.concat(
                            Stream.of(
                                new Spiller() {
                                  @Override
                                  public long spill(MemoryTarget self, long size) {
                                    return Optional.of(out.get())
                                        .map(nmm -> nmm.shrink(size))
                                        .orElseThrow(
                                            () ->
                                                new IllegalStateException(
                                                    ""
                                                        + "Shrink is requested before native "
                                                        + "memory manager is created. Try moving "
                                                        + "any actions about memory allocation out "
                                                        + "from the memory manager constructor."));
                                  }

                                  @Override
                                  public Set<Phase> applicablePhases() {
                                    return Spillers.PHASE_SET_SHRINK_ONLY;
                                  }
                                }),
                            spillers.stream())
                        .map(spiller -> Spillers.withMinSpillSize(spiller, reservationBlockSize))
                        .collect(Collectors.toList()),
                    Collections.singletonMap(
                        "single",
                        new MemoryUsageRecorder() {
                          @Override
                          public void inc(long bytes) {
                            // no-op
                          }

                          @Override
                          public long peak() {
                            throw new UnsupportedOperationException("Not implemented");
                          }

                          @Override
                          public long current() {
                            throw new UnsupportedOperationException("Not implemented");
                          }

                          @Override
                          public MemoryUsageStats toStats() {
                            return getNativeMemoryManager().collectMemoryUsage();
                          }

                          private NativeMemoryManager getNativeMemoryManager() {
                            return Optional.of(out.get())
                                .orElseThrow(
                                    () ->
                                        new IllegalStateException(
                                            ""
                                                + "Memory usage stats are requested before native "
                                                + "memory manager is created. Try moving any "
                                                + "actions about memory allocation out from the "
                                                + "memory manager constructor."));
                          }
                        })),
                MemoryTargets.newConsumer(
                    tmm,
                    "OverAcquire.DummyTarget",
                    Collections.singletonList(
                        new Spiller() {
                          @Override
                          public long spill(MemoryTarget self, long size) {
                            return self.repay(size);
                          }

                          @Override
                          public Set<Phase> applicablePhases() {
                            return Spillers.PHASE_SET_ALL;
                          }
                        }),
                    Collections.emptyMap()),
                overAcquiredRatio));
    // listener
    ManagedReservationListener rl =
        new ManagedReservationListener(target, TaskResources.getSharedUsage());
    // native memory manager
    out.set(NativeMemoryManager.create(name, rl));
    return out.get();
  }
}
