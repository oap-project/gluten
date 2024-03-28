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
import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.memory.alloc.NativeMemoryAllocators;
import org.apache.gluten.memory.memtarget.KnownNameAndStats;
import org.apache.gluten.proto.MemoryUsageStats;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.spark.memory.SparkMemoryUtil;
import org.apache.spark.util.TaskResource;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeMemoryManager implements TaskResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeMemoryManager.class);

  private final long nativeInstanceHandle;
  private final String name;
  private final ReservationListener listener;

  private NativeMemoryManager(
      String name, long nativeInstanceHandle, ReservationListener listener) {
    this.name = name;
    this.nativeInstanceHandle = nativeInstanceHandle;
    this.listener = listener;
  }

  public static NativeMemoryManager create(String name, ReservationListener listener) {
    long allocatorId = NativeMemoryAllocators.getDefault().get().getNativeInstanceId();
    long reservationBlockSize = GlutenConfig.getConf().memoryReservationBlockSize();
    return new NativeMemoryManager(
        name,
        create(
            BackendsApiManager.getBackendName(), name, allocatorId, reservationBlockSize, listener),
        listener);
  }

  public long getNativeInstanceHandle() {
    return this.nativeInstanceHandle;
  }

  public MemoryUsageStats collectMemoryUsage() {
    try {
      return MemoryUsageStats.parseFrom(collectMemoryUsage(nativeInstanceHandle));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public long shrink(long size) {
    return shrink(nativeInstanceHandle, size);
  }

  // Hold this memory manager. The underlying memory pools will be released as lately as this
  // memory manager gets destroyed. Which means, a call to this function would make sure the
  // memory blocks directly or indirectly managed by this manager, be guaranteed safe to
  // access during the period that this manager is alive.
  public void hold() {
    hold(nativeInstanceHandle);
  }

  private static native long shrink(long memoryManagerId, long size);

  private static native long create(
      String backendType,
      String name,
      long allocatorId,
      long reservationBlockSize,
      ReservationListener listener);

  private static native void release(long memoryManagerId);

  private static native byte[] collectMemoryUsage(long memoryManagerId);

  private static native void hold(long memoryManagerId);

  @Override
  public void release() throws Exception {
    LOGGER.debug(
        SparkMemoryUtil.prettyPrintStats(
            "About to release memory manager, usage dump:",
            new KnownNameAndStats() {
              @Override
              public String name() {
                return name;
              }

              @Override
              public MemoryUsageStats stats() {
                return collectMemoryUsage();
              }
            }));
    release(nativeInstanceHandle);
    if (listener.getUsedBytes() != 0) {
      LOGGER.warn(
          String.format(
              "%s Reservation listener %s still reserved non-zero bytes, "
                  + "which may cause memory leak, size: %s. ",
              name, listener.toString(), Utils.bytesToString(listener.getUsedBytes())));
    }
  }

  @Override
  public int priority() {
    return 0; // lowest release priority
  }

  @Override
  public String resourceName() {
    return name + "_mem";
  }
}
