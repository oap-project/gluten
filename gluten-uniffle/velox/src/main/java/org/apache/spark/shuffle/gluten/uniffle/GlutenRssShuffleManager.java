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
package org.apache.spark.shuffle.gluten.uniffle;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.shuffle.ColumnarShuffleDependency;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.sort.ColumnarShuffleManager;
import org.apache.spark.shuffle.writer.VeloxUniffleColumnarShuffleWriter;
import org.apache.uniffle.common.exception.RssException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

public class GlutenRssShuffleManager extends RssShuffleManager {
  private static final Logger LOG = LoggerFactory.getLogger(GlutenRssShuffleManager.class);
  private static final String GLUTEN_SHUFFLE_MANAGER_NAME =
      "org.apache.spark.shuffle.sort.ColumnarShuffleManager";

  private static final String VANILLA_UNIFFLE_SHUFFLE_MANAGER_NAME =
      "org.apache.spark.shuffle.celeborn.SparkShuffleManager";
  private volatile ColumnarShuffleManager _columnarShuffleManager;
  private volatile RssShuffleManager _vanillaUniffleShuffleManager;

  private ColumnarShuffleManager columnarShuffleManager() {
    if (_columnarShuffleManager == null) {
      synchronized (this) {
        if (_columnarShuffleManager == null) {
          _columnarShuffleManager =
              initShuffleManager(GLUTEN_SHUFFLE_MANAGER_NAME, sparkConf, isDriver());
        }
      }
    }
    return _columnarShuffleManager;
  }

  private RssShuffleManager vanillaUniffleShuffleManager() {
    if (_vanillaUniffleShuffleManager == null) {
      synchronized (this) {
        if (_vanillaUniffleShuffleManager == null) {
          initShuffleManager(VANILLA_UNIFFLE_SHUFFLE_MANAGER_NAME, sparkConf, isDriver());
        }
      }
    }
    return _vanillaUniffleShuffleManager;
  }

  private boolean isDriver() {
    return "driver".equals(SparkEnv.get().executorId());
  }

  private ColumnarShuffleManager initShuffleManager(String name, SparkConf conf, boolean isDriver) {
    Constructor constructor;
    ColumnarShuffleManager instance;
    try {
      Class klass = Class.forName(name);
      try {
        constructor = klass.getConstructor(conf.getClass(), Boolean.TYPE);
        instance = (ColumnarShuffleManager) constructor.newInstance(conf, isDriver);
      } catch (NoSuchMethodException var7) {
        constructor = klass.getConstructor(conf.getClass());
        instance = (ColumnarShuffleManager) constructor.newInstance(conf);
      }
    } catch (Exception e) {
      throw new RuntimeException("initColumnManager fail");
    }
    return instance;
  }

  public GlutenRssShuffleManager(SparkConf conf, boolean isDriver) {
    super(conf, isDriver);
    conf.set("spark.rss.row.based", "false");
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {
    return super.registerShuffle(shuffleId, dependency);
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    if (!(handle instanceof RssShuffleHandle)) {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
    sparkConf.setIfMissing(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssSparkConfig.RSS_ROW_BASED, "false");
    RssShuffleHandle<K, V, V> rssHandle = (RssShuffleHandle<K, V, V>) handle;
    if (rssHandle.getDependency() instanceof ColumnarShuffleDependency) {
      setPusherAppId(rssHandle);
      String taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
      ShuffleWriteMetrics writeMetrics;
      if (metrics != null) {
        writeMetrics = new WriteMetrics(metrics);
      } else {
        writeMetrics = context.taskMetrics().shuffleWriteMetrics();
      }
      return new VeloxUniffleColumnarShuffleWriter<>(
          context.partitionId(),
          rssHandle.getAppId(),
          rssHandle.getShuffleId(),
          taskId,
          context.taskAttemptId(),
          writeMetrics,
          this,
          sparkConf,
          shuffleWriteClient,
          rssHandle,
          this::markFailedTask,
          context);
    } else {
      return super.getWriter(handle, mapId, context, metrics);
    }
  }
}
