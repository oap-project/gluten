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

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.memory.memtarget.MemoryTarget;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class CHManagedCHReservationListener implements CHReservationListener {

  private static final Logger LOG = LoggerFactory.getLogger(CHManagedCHReservationListener.class);

  private MemoryTarget target;
  private final SimpleMemoryUsageRecorder usage;
  private final boolean throwIfMemoryExceed =
      GlutenConfig.getConf().chColumnarThrowIfMemoryExceed();
  private volatile boolean open = true;

  private final AtomicLong currentMemory = new AtomicLong(0L);

  public CHManagedCHReservationListener(MemoryTarget target, SimpleMemoryUsageRecorder usage) {
    this.target = target;
    this.usage = usage;
  }

  @Override
  public void reserveOrThrow(long size) {
    if (!throwIfMemoryExceed) {
      reserve(size);
      return;
    }

    synchronized (this) {
      if (!open) {
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("reserve memory size from native: %d", size));
      }
      long granted = target.borrow(size);
      if (granted < size) {
        target.repay(granted);
        throw new UnsupportedOperationException(
            "Not enough spark off-heap execution memory. "
                + "Acquired: "
                + size
                + ", granted: "
                + granted
                + ". "
                + "Try tweaking config option spark.memory.offHeap.size to "
                + "get larger space to run this application. ");
      }
      currentMemory.addAndGet(size);
      usage.inc(size);
    }
  }

  @Override
  public long reserve(long size) {
    synchronized (this) {
      if (!open) {
        return 0L;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("reserve memory (without exception) size from native: %d", size));
      }
      long granted = target.borrow(size);
      if (granted < size && (LOG.isWarnEnabled())) {
        LOG.warn(
            String.format(
                "Not enough spark off-heap execution memory. "
                    + "Acquired: %d, granted: %d. Try tweaking config option "
                    + "spark.memory.offHeap.size to get larger space "
                    + "to run this application.",
                size, granted));
      }
      currentMemory.addAndGet(granted);
      usage.inc(size);
      return granted;
    }
  }

  @Override
  public long unreserve(long size) {
    synchronized (this) {
      if (!open) {
        return 0L;
      }
      long memoryToFree = size;
      if ((currentMemory.get() - size) < 0L) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              String.format(
                  "The current used memory' %d will be less than 0(%d) after free %d",
                  currentMemory.get(), currentMemory.get() - size, size));
        }
        memoryToFree = currentMemory.get();
      }
      if (memoryToFree == 0L) {
        return memoryToFree;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("unreserve memory size from native: %d", memoryToFree));
      }
      target.repay(memoryToFree);
      currentMemory.addAndGet(-memoryToFree);
      usage.inc(-size);
      return memoryToFree;
    }
  }

  @Override
  public void inactivate() {
    synchronized (this) {
      // for some reasons, memory audit in the native code may not be 100% accurate
      // we'll allow the inaccuracy
      if (currentMemory.get() > 0) {
        unreserve(currentMemory.get());
      } else if (currentMemory.get() < 0) {
        reserve(currentMemory.get());
      }
      currentMemory.set(0L);

      target = null; // make it gc reachable
      open = false;
    }
  }

  @Override
  public long currentMemory() {
    return currentMemory.get();
  }
}
