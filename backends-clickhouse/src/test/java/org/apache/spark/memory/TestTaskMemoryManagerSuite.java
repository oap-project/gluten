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
package org.apache.spark.memory;

import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.memory.alloc.CHManagedCHReservationListener;
import org.apache.gluten.memory.alloc.CHNativeMemoryAllocator;
import org.apache.gluten.memory.alloc.CHNativeMemoryAllocatorManagerImpl;
import org.apache.gluten.memory.memtarget.MemoryTargets;

import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.package$;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class TestTaskMemoryManagerSuite {
  static {
    // for skip loading lib in NativeMemoryAllocator
    System.setProperty("spark.sql.testkey", "true");
  }

  protected TaskMemoryManager taskMemoryManager;
  protected CHManagedCHReservationListener listener;
  protected CHNativeMemoryAllocatorManagerImpl manager;

  @Before
  public void initMemoryManager() {
    final SparkConf conf =
        new SparkConf()
            .set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), true)
            .set(package$.MODULE$.MEMORY_OFFHEAP_SIZE(), 1000L);
    taskMemoryManager = new TaskMemoryManager(new UnifiedMemoryManager(conf, 1000L, 500L, 1), 0);

    listener =
        new CHManagedCHReservationListener(
            MemoryTargets.newConsumer(
                taskMemoryManager, "test", Collections.emptyList(), Collections.emptyMap()),
            new SimpleMemoryUsageRecorder());

    manager = new CHNativeMemoryAllocatorManagerImpl(new CHNativeMemoryAllocator(-1L, listener));
  }

  @After
  public void destroyMemoryManager() {
    taskMemoryManager = null;
    listener = null;
    manager = null;
  }

  @Test
  public void testCHNativeMemoryManager() {
    listener.reserveOrThrow(100L);
    Assert.assertEquals(100L, taskMemoryManager.getMemoryConsumptionForThisTask());

    listener.unreserve(100L);
    Assert.assertEquals(0L, taskMemoryManager.getMemoryConsumptionForThisTask());
  }

  @Test
  public void testMemoryFreeLessThanMalloc() {
    listener.reserveOrThrow(100L);
    Assert.assertEquals(100L, taskMemoryManager.getMemoryConsumptionForThisTask());

    listener.unreserve(200L);
    Assert.assertEquals(0L, taskMemoryManager.getMemoryConsumptionForThisTask());
  }

  @Test
  public void testMemoryLeak() {
    listener.reserveOrThrow(100L);
    Assert.assertEquals(100L, taskMemoryManager.getMemoryConsumptionForThisTask());

    listener.unreserve(100L);
    Assert.assertEquals(0L, taskMemoryManager.getMemoryConsumptionForThisTask());

    listener.reserveOrThrow(100L);
    Assert.assertEquals(100L, taskMemoryManager.getMemoryConsumptionForThisTask());

    listener.reserveOrThrow(100L);
    Assert.assertEquals(200L, taskMemoryManager.getMemoryConsumptionForThisTask());

    try {
      manager.release();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnsupportedOperationException);
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testAcquireLessMemory() {
    listener.reserveOrThrow(100L);
    Assert.assertEquals(100L, taskMemoryManager.getMemoryConsumptionForThisTask());

    listener.reserveOrThrow(1000L);
  }
}
