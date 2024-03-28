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
package org.apache.gluten

import org.apache.gluten.GlutenConfig.GLUTEN_DEFAULT_SESSION_TIMEZONE_KEY
import org.apache.gluten.GlutenPlugin.{GLUTEN_SESSION_EXTENSION_NAME, SPARK_SESSION_EXTS_KEY}
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.events.GlutenBuildInfoEvent
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.expression.ExpressionMappings
import org.apache.gluten.extension.{ColumnarOverrides, OthersExtensionOverrides, QueryStagePrepOverrides, StrategyOverrides}
import org.apache.gluten.test.TestStats
import org.apache.gluten.utils.TaskListener

import org.apache.spark.{SparkConf, SparkContext, TaskFailedReason}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.listener.GlutenListenerFactory
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.{GlutenDriverEndpoint, GlutenExecutorEndpoint}
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.execution.ui.GlutenEventUtils
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.utils.ExpressionUtil
import org.apache.spark.util.{SparkResourceUtil, TaskResources}

import java.util
import java.util.{Collections, Objects}

import scala.collection.mutable
import scala.language.implicitConversions

class GlutenPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new GlutenDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new GlutenExecutorPlugin()
  }
}

private[gluten] class GlutenDriverPlugin extends DriverPlugin with Logging {
  private var _sc: Option[SparkContext] = None

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    _sc = Some(sc)
    GlutenEventUtils.registerListener(sc)
    postBuildInfoEvent(sc)

    val conf = pluginContext.conf()
    if (conf.getBoolean(GlutenConfig.UT_STATISTIC.key, defaultValue = false)) {
      // Only statistic in UT, not thread safe
      TestStats.beginStatistic()
    }

    setPredefinedConfigs(sc, conf)
    // Initialize Backends API
    BackendsApiManager.initialize()
    BackendsApiManager.getListenerApiInstance.onDriverStart(conf)
    GlutenDriverEndpoint.glutenDriverEndpointRef = (new GlutenDriverEndpoint).self
    GlutenListenerFactory.addToSparkListenerBus(sc)
    ExpressionMappings.expressionExtensionTransformer =
      ExpressionUtil.extendedExpressionTransformer(
        conf.get(GlutenConfig.GLUTEN_EXTENDED_EXPRESSION_TRAN_CONF, "")
      )
    Collections.emptyMap()
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    if (pluginContext.conf().getBoolean(GlutenConfig.GLUTEN_UI_ENABLED, true)) {
      _sc.foreach {
        sc =>
          GlutenEventUtils.attachUI(sc)
          logInfo("Gluten SQL Tab has attached.")
      }
    }
  }

  override def shutdown(): Unit = {
    BackendsApiManager.getListenerApiInstance.onDriverShutdown()
  }

  private def postBuildInfoEvent(sc: SparkContext): Unit = {
    val buildInfo = BackendsApiManager.getBuildInfo

    // export gluten version to property to spark
    System.setProperty("gluten.version", VERSION)

    val glutenBuildInfo = new mutable.HashMap[String, String]()
    glutenBuildInfo.put("Gluten Version", VERSION)
    glutenBuildInfo.put("GCC Version", GCC_VERSION)
    glutenBuildInfo.put("Java Version", JAVA_COMPILE_VERSION)
    glutenBuildInfo.put("Scala Version", SCALA_COMPILE_VERSION)
    glutenBuildInfo.put("Spark Version", SPARK_COMPILE_VERSION)
    glutenBuildInfo.put("Hadoop Version", HADOOP_COMPILE_VERSION)
    glutenBuildInfo.put("Gluten Branch", BRANCH)
    glutenBuildInfo.put("Gluten Revision", REVISION)
    glutenBuildInfo.put("Gluten Revision Time", REVISION_TIME)
    glutenBuildInfo.put("Gluten Build Time", BUILD_DATE)
    glutenBuildInfo.put("Gluten Repo URL", REPO_URL)
    glutenBuildInfo.put("Backend", buildInfo.backend)
    glutenBuildInfo.put("Backend Branch", buildInfo.backendBranch)
    glutenBuildInfo.put("Backend Revision", buildInfo.backendRevision)
    glutenBuildInfo.put("Backend Revision Time", buildInfo.backendRevisionTime)
    val infoMap = glutenBuildInfo.toMap
    val loggingInfo = infoMap.toSeq
      .sortBy(_._1)
      .map {
        case (name, value) =>
          s"$name: $value"
      }
      .mkString(
        "Gluten build info:\n==============================================================\n",
        "\n",
        "\n=============================================================="
      )
    logInfo(loggingInfo)
    val event = GlutenBuildInfoEvent(infoMap)
    GlutenEventUtils.post(sc, event)
  }

  private def setPredefinedConfigs(sc: SparkContext, conf: SparkConf): Unit = {
    // sql extensions
    val extensions = if (conf.contains(SPARK_SESSION_EXTS_KEY)) {
      s"${conf.get(SPARK_SESSION_EXTS_KEY)},$GLUTEN_SESSION_EXTENSION_NAME"
    } else {
      s"$GLUTEN_SESSION_EXTENSION_NAME"
    }
    conf.set(SPARK_SESSION_EXTS_KEY, extensions)

    // off-heap bytes
    if (!conf.contains(GlutenConfig.GLUTEN_OFFHEAP_SIZE_KEY)) {
      throw new GlutenException(s"${GlutenConfig.GLUTEN_OFFHEAP_SIZE_KEY} is not set")
    }
    // Session's local time zone must be set. If not explicitly set by user, its default
    // value (detected for the platform) is used, consistent with spark.
    conf.set(GLUTEN_DEFAULT_SESSION_TIMEZONE_KEY, SQLConf.SESSION_LOCAL_TIMEZONE.defaultValueString)

    // task slots
    val taskSlots = SparkResourceUtil.getTaskSlots(conf)

    // Optimistic off-heap sizes, assuming all storage memory can be borrowed into execution memory
    // pool, regardless of Spark option spark.memory.storageFraction.
    val offHeapSize = conf.getSizeAsBytes(GlutenConfig.GLUTEN_OFFHEAP_SIZE_KEY)
    conf.set(GlutenConfig.GLUTEN_OFFHEAP_SIZE_IN_BYTES_KEY, offHeapSize.toString)
    val offHeapPerTask = offHeapSize / taskSlots
    conf.set(GlutenConfig.GLUTEN_TASK_OFFHEAP_SIZE_IN_BYTES_KEY, offHeapPerTask.toString)

    // Pessimistic off-heap sizes, with the assumption that all non-borrowable storage memory
    // determined by spark.memory.storageFraction was used.
    val fraction = 1.0d - conf.getDouble("spark.memory.storageFraction", 0.5d)
    val conservativeOffHeapPerTask = (offHeapSize * fraction).toLong / taskSlots
    conf.set(
      GlutenConfig.GLUTEN_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES_KEY,
      conservativeOffHeapPerTask.toString)

    // disable vanilla columnar readers, to prevent columnar-to-columnar conversions
    if (BackendsApiManager.getSettings.disableVanillaColumnarReaders(conf)) {
      // FIXME Hongze 22/12/06
      //  BatchScan.scala in shim was not always loaded by class loader.
      //  The file should be removed and the "ClassCastException" issue caused by
      //  spark.sql.<format>.enableVectorizedReader=true should be fixed in another way.
      //  Before the issue was fixed we force the use of vanilla row reader by using
      //  the following statement.
      conf.set("spark.sql.parquet.enableVectorizedReader", "false")
      conf.set("spark.sql.orc.enableVectorizedReader", "false")
      conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "false")
    }
    // When the Velox cache is enabled, the Velox file handle cache should also be enabled.
    // Otherwise, a 'reference id not found' error may occur.
    if (
      conf.getBoolean(GlutenConfig.COLUMNAR_VELOX_CACHE_ENABLED.key, false) && !conf.getBoolean(
        GlutenConfig.COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED.key,
        false)
    ) {
      throw new IllegalArgumentException(s"${GlutenConfig.COLUMNAR_VELOX_CACHE_ENABLED.key} and " +
        s"${GlutenConfig.COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED.key} should be enabled together.")
    }
  }
}

private[gluten] class GlutenExecutorPlugin extends ExecutorPlugin {
  private var executorEndpoint: GlutenExecutorEndpoint = _
  private val taskListeners: Seq[TaskListener] = Array(TaskResources)

  /** Initialize the executor plugin. */
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    val conf = ctx.conf()

    // Must set the 'spark.memory.offHeap.size' value to native memory malloc
    if (
      !conf.getBoolean("spark.memory.offHeap.enabled", false) ||
      (JavaUtils.byteStringAsBytes(
        conf.get("spark.memory.offHeap.size").toString) / 1024 / 1024).toInt <= 0
    ) {
      throw new IllegalArgumentException(
        s"Must set 'spark.memory.offHeap.enabled' to true" +
          s" and set off heap memory size by option 'spark.memory.offHeap.size'")
    }
    // Initialize Backends API
    // TODO categorize the APIs by driver's or executor's
    BackendsApiManager.initialize()
    BackendsApiManager.getListenerApiInstance.onExecutorStart(conf)
    executorEndpoint = new GlutenExecutorEndpoint(ctx.executorID(), conf)
  }

  /** Clean up and terminate this plugin. For example: close the native engine. */
  override def shutdown(): Unit = {
    BackendsApiManager.getListenerApiInstance.onExecutorShutdown()
    super.shutdown()
  }

  override def onTaskStart(): Unit = {
    taskListeners.foreach(_.onTaskStart())
  }

  override def onTaskSucceeded(): Unit = {
    taskListeners.reverse.foreach(_.onTaskSucceeded())
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    taskListeners.reverse.foreach(_.onTaskFailed(failureReason))
  }
}

private[gluten] class GlutenSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(exts: SparkSessionExtensions): Unit = {
    GlutenPlugin.DEFAULT_INJECTORS.foreach(injector => injector.inject(exts))
  }
}

private[gluten] class SparkConfImplicits(conf: SparkConf) {
  def enableGlutenPlugin(): SparkConf = {
    if (conf.contains(GlutenPlugin.SPARK_SQL_PLUGINS_KEY)) {
      throw new IllegalArgumentException(
        "A Spark plugin is already specified before enabling " +
          "Gluten plugin: " + conf.get(GlutenPlugin.SPARK_SQL_PLUGINS_KEY))
    }
    conf.set(GlutenPlugin.SPARK_SQL_PLUGINS_KEY, GlutenPlugin.GLUTEN_PLUGIN_NAME)
  }
}

private[gluten] trait GlutenSparkExtensionsInjector {
  def inject(extensions: SparkSessionExtensions)
}

private[gluten] object GlutenPlugin {
  // To enable GlutenPlugin in production, set "spark.plugins=org.apache.gluten.GlutenPlugin"
  val SPARK_SQL_PLUGINS_KEY: String = "spark.plugins"
  val GLUTEN_PLUGIN_NAME: String = Objects.requireNonNull(classOf[GlutenPlugin].getCanonicalName)
  val SPARK_SESSION_EXTS_KEY: String = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
  val GLUTEN_SESSION_EXTENSION_NAME: String =
    Objects.requireNonNull(classOf[GlutenSessionExtensions].getCanonicalName)

  /** Specify all injectors that Gluten is using in following list. */
  val DEFAULT_INJECTORS: List[GlutenSparkExtensionsInjector] = List(
    QueryStagePrepOverrides,
    ColumnarOverrides,
    StrategyOverrides,
    OthersExtensionOverrides
  )

  implicit def sparkConfImplicit(conf: SparkConf): SparkConfImplicits = {
    new SparkConfImplicits(conf)
  }
}
