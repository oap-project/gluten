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

package io.glutenproject.memory;

import org.apache.arrow.memory.AllocationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class SparkManagedAllocationListener implements AllocationListener, AutoCloseable {
  private static final Logger LOG =
      LoggerFactory.getLogger(SparkManagedAllocationListener.class);

  public static long BLOCK_SIZE = 8L * 1024 * 1024; // 8MB per block

  private final NativeSQLMemoryConsumer consumer;
  private final NativeSQLMemoryMetrics metrics;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private long bytesReserved = 0L;
  private long blocksReserved = 0L;

  public SparkManagedAllocationListener(NativeSQLMemoryConsumer consumer,
                                        NativeSQLMemoryMetrics metrics) {
    this.consumer = consumer;
    this.metrics = metrics;
  }

  @Override
  public void onPreAllocation(long size) {
    if (closed.get()) {
      LOG.warn("SparkManagedAllocationListener: already closed");
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
    consumer.acquire(toBeAcquired);
    metrics.inc(toBeAcquired);
  }

  @Override
  public void onRelease(long size) {
    if (closed.get()) {
      LOG.warn("SparkManagedAllocationListener: already closed");
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
    consumer.free(toBeReleased);
    metrics.inc(-toBeReleased);
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
