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
package io.glutenproject.backendsapi.clickhouse

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.ListenerApi
import io.glutenproject.execution.CHBroadcastBuildSideCache
import io.glutenproject.execution.datasource.{GlutenOrcWriterInjects, GlutenParquetWriterInjects, GlutenRowSplitter}
import io.glutenproject.expression.UDFMappings
import io.glutenproject.vectorized.{CHNativeExpressionEvaluator, JniLibLoader}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.execution.datasources.v1._

import org.apache.commons.lang3.StringUtils

import java.util.TimeZone

class CHListenerApi extends ListenerApi with Logging {

  override def onDriverStart(conf: SparkConf): Unit = initialize(conf, isDriver = true)

  override def onDriverShutdown(): Unit = shutdown()

  override def onExecutorStart(conf: SparkConf): Unit = initialize(conf, isDriver = false)

  override def onExecutorShutdown(): Unit = shutdown()

  private def initialize(conf: SparkConf, isDriver: Boolean): Unit = {
    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isBlank(libPath)) {
      throw new IllegalArgumentException(
        "Please set spark.gluten.sql.columnar.libpath to enable clickhouse backend")
    }
    if (isDriver) {
      JniLibLoader.loadFromPath(libPath, true)
    } else {
      val executorLibPath = conf.get(GlutenConfig.GLUTEN_EXECUTOR_LIB_PATH, libPath)
      JniLibLoader.loadFromPath(executorLibPath, true)
    }
    // Add configs
    conf.set(
      s"${CHBackendSettings.getBackendConfigPrefix}.runtime_config.timezone",
      conf.get("spark.sql.session.timeZone", TimeZone.getDefault.getID))
    conf.set(
      s"${CHBackendSettings.getBackendConfigPrefix}.runtime_config" +
        s".local_engine.settings.log_processors_profiles",
      "true")

    // add memory limit for external sort
    val externalSortKey = s"${CHBackendSettings.getBackendConfigPrefix}.runtime_settings" +
      s".max_bytes_before_external_sort"
    if (conf.getInt(externalSortKey, -1) < 0) {
      if (conf.getBoolean("spark.memory.offHeap.enabled", false)) {
        val memSize = JavaUtils.byteStringAsBytes(conf.get("spark.memory.offHeap.size")).toInt
        if (memSize > 0) {
          val cores = conf.getInt("spark.executor.cores", 1)
          val sortMemLimit = ((memSize / cores) * 0.8).toInt
          logInfo(s"max memory for sorting: $sortMemLimit")
          conf.set(externalSortKey, sortMemLimit.toString)
        }
      }
    }

    // Load supported hive/python/scala udfs
    UDFMappings.loadFromSparkConf(conf)

    val initKernel = new CHNativeExpressionEvaluator()
    initKernel.initNative(conf)

    // inject backend-specific implementations to override spark classes
    // FIXME: The following set instances twice in local mode?
    GlutenParquetWriterInjects.setInstance(new CHParquetWriterInjects())
    GlutenOrcWriterInjects.setInstance(new CHOrcWriterInjects())
    GlutenMergeTreeWriterInjects.setInstance(new CHMergeTreeWriterInjects())
    GlutenRowSplitter.setInstance(new CHRowSplitter())
  }

  private def shutdown(): Unit = {
    CHBroadcastBuildSideCache.cleanAll()
    val kernel = new CHNativeExpressionEvaluator()
    kernel.finalizeNative()
  }
}
