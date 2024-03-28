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
package org.apache.gluten.utils;

import org.apache.gluten.exception.GlutenException;

import org.apache.spark.SparkEnv;
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig;

/**
 * An object that generates IDs. This is broken into a separate class in case we ever want to
 * support multiple worker threads per process. Refer to twitter-archive Snowflake.
 */
public class SnowflakeIdWorker {

  // ==============================Singleton=====================================
  private static volatile SnowflakeIdWorker INSTANCE;
  // ==============================Fields===========================================
  private final long twepoch = 1640966400L;
  private final long workerIdBits = 6L;
  private final long maxWorkerId = -1L ^ (-1L << workerIdBits);
  private final long sequenceBits = 16L;
  private final long workerIdShift = sequenceBits;
  private final long timestampLeftShift = sequenceBits + workerIdBits;
  private final long sequenceMask = -1L ^ (-1L << sequenceBits);
  private long workerId;
  private long sequence = 0L;
  private long lastTimestamp = -1L;

  public SnowflakeIdWorker(long workerId) {
    if (workerId > maxWorkerId || workerId < 0) {
      throw new IllegalArgumentException(
          String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
    }
    this.workerId = workerId;
  }

  // ==============================Constructors=====================================

  public static SnowflakeIdWorker getInstance() {
    if (INSTANCE == null) {
      synchronized (SnowflakeIdWorker.class) {
        if (INSTANCE == null) {
          if (!SparkEnv.get().conf().contains(ClickHouseConfig.CLICKHOUSE_WORKER_ID())) {
            throw new IllegalArgumentException(
                "Please set an unique value to " + ClickHouseConfig.CLICKHOUSE_WORKER_ID());
          }
          INSTANCE =
              new SnowflakeIdWorker(
                  SparkEnv.get().conf().getLong(ClickHouseConfig.CLICKHOUSE_WORKER_ID(), 0));
        }
      }
    }
    return INSTANCE;
  }

  // ==============================Methods==========================================
  public synchronized long nextId() {
    long timestamp = timeGen();

    if (timestamp < lastTimestamp) {
      throw new GlutenException(
          String.format(
              "Clock moved backwards.  Refusing to generate id for %d milliseconds",
              lastTimestamp - timestamp));
    }

    if (lastTimestamp == timestamp) {
      sequence = (sequence + 1) & sequenceMask;
      if (sequence == 0) {
        timestamp = tilNextMillis(lastTimestamp);
      }
    } else {
      sequence = 0L;
    }

    lastTimestamp = timestamp;

    return ((timestamp - twepoch) << timestampLeftShift) //
        | (workerId << workerIdShift) //
        | sequence;
  }

  protected long tilNextMillis(long lastTimestamp) {
    long timestamp = timeGen();
    while (timestamp <= lastTimestamp) {
      timestamp = timeGen();
    }
    return timestamp;
  }

  protected long timeGen() {
    return System.currentTimeMillis() / 1000L;
  }
}
