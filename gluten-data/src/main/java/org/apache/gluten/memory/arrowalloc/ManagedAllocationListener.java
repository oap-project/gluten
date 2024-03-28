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
package org.apache.gluten.memory.arrowalloc;

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.memory.memtarget.MemoryTarget;

import org.apache.arrow.memory.AllocationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class ManagedAllocationListener implements AllocationListener, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedAllocationListener.class);

  public static long BLOCK_SIZE = GlutenConfig.getConf().memoryReservationBlockSize();

  private final MemoryTarget target;
  private final SimpleMemoryUsageRecorder sharedUsage;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private long bytesReserved = 0L;
  private long blocksReserved = 0L;

  public ManagedAllocationListener(MemoryTarget target, SimpleMemoryUsageRecorder sharedUsage) {
    this.target = target;
    this.sharedUsage = sharedUsage;
  }

  @Override
  public void onPreAllocation(long size) {
    if (closed.get()) {
      LOG.warn("ManagedAllocationListener: already closed");
      return;
    }
    long requiredBlocks = updateReservation(size);
    if (requiredBlocks < 0) {
      throw new IllegalStateException();
    }
    if (requiredBlocks == 0) {
      return;
    }
    long toBeAcquired = requiredBlocks * BLOCK_SIZE;
    long granted = target.borrow(toBeAcquired);
    sharedUsage.inc(granted);
  }

  @Override
  public void onRelease(long size) {
    if (closed.get()) {
      LOG.warn("ManagedAllocationListener: already closed");
      return;
    }
    long requiredBlocks = updateReservation(-size);
    if (requiredBlocks > 0) {
      throw new IllegalStateException();
    }
    if (requiredBlocks == 0) {
      return;
    }
    long toBeReleased = -requiredBlocks * BLOCK_SIZE;
    long freed = target.repay(toBeReleased);
    sharedUsage.inc(-freed);
  }

  public long updateReservation(long bytesToAdd) {
    synchronized (this) {
      long newBytesReserved = bytesReserved + bytesToAdd;
      final long newBlocksReserved;
      // ceiling
      if (newBytesReserved == 0L) {
        // 0 is the special case in ceiling algorithm
        newBlocksReserved = 0L;
      } else {
        newBlocksReserved = (newBytesReserved - 1L) / BLOCK_SIZE + 1L;
      }
      long requiredBlocks = newBlocksReserved - blocksReserved;
      bytesReserved = newBytesReserved;
      blocksReserved = newBlocksReserved;
      return requiredBlocks;
    }
  }

  @Override
  public void close() throws Exception {
    closed.set(true);
  }
}
