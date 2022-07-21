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

package io.glutenproject

import java.util.{Collections, Objects}

import scala.language.implicitConversions
import com.google.protobuf.Any
import io.glutenproject.GlutenPlugin.{GLUTEN_SESSION_EXTENSION_NAME, SPARK_SESSION_EXTS_KEY}
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.{ColumnarOverrides, ColumnarQueryStagePreparations, OthersExtensionOverrides, StrategyOverrides}
import io.glutenproject.substrait.expression.ExpressionBuilder
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.{PlanBuilder, PlanNode}
import io.glutenproject.vectorized.ExpressionEvaluator
import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.internal.StaticSQLConf

class GlutenPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new GlutenDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new GlutenExecutorPlugin()
  }
}

private[glutenproject] class GlutenDriverPlugin extends DriverPlugin {
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    val conf = pluginContext.conf()
    GlutenPlugin.initNative(conf)
    setPredefinedConfigs(conf)
    // Initialize Backends API
    BackendsApiManager.initialize(pluginContext.conf()
      .get(GlutenConfig.GLUTEN_BACKEND_LIB, ""))
    Collections.emptyMap()
  }

  def setPredefinedConfigs(conf: SparkConf): Unit = {
    if (conf.contains(SPARK_SESSION_EXTS_KEY)) {
      throw new IllegalArgumentException("Spark extensions are already specified before " +
        "enabling Gluten plugin: " + conf.get(GlutenPlugin.SPARK_SESSION_EXTS_KEY))
    }
    conf.set(SPARK_SESSION_EXTS_KEY,
      String.format("%s", GLUTEN_SESSION_EXTENSION_NAME))
  }
}

private[glutenproject] class GlutenExecutorPlugin extends ExecutorPlugin {
  /**
   * Initialize the executor plugin.
   */
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    GlutenPlugin.initNative(ctx.conf())
    BackendsApiManager.initialize(ctx.conf().get(GlutenConfig.GLUTEN_BACKEND_LIB, ""))
  }

  /**
   * Clean up and terminate this plugin.
   * For example: close the native engine.
   */
  override def shutdown(): Unit = {
    super.shutdown()
  }
}

private[glutenproject] class GlutenSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(exts: SparkSessionExtensions): Unit = {
    GlutenPlugin.DEFAULT_INJECTORS.foreach(injector => injector.inject(exts))
  }
}

private[glutenproject] class SparkConfImplicits(conf: SparkConf) {
  def enableGlutenPlugin(): SparkConf = {
    if (conf.contains(GlutenPlugin.SPARK_SQL_PLUGINS_KEY)) {
      throw new IllegalArgumentException("A Spark plugin is already specified before enabling " +
        "Gluten plugin: " + conf.get(GlutenPlugin.SPARK_SQL_PLUGINS_KEY))
    }
    conf.set(GlutenPlugin.SPARK_SQL_PLUGINS_KEY, GlutenPlugin.GLUTEN_PLUGIN_NAME)
  }
}

private[glutenproject] trait GlutenSparkExtensionsInjector {
  def inject(extensions: SparkSessionExtensions)
}

private[glutenproject] object GlutenPlugin {
  // To enable GlutenPlugin in production, set "spark.plugins=io.glutenproject.GlutenPlugin"
  val SPARK_SQL_PLUGINS_KEY: String = "spark.plugins"
  val GLUTEN_PLUGIN_NAME: String = Objects.requireNonNull(classOf[GlutenPlugin]
    .getCanonicalName)
  val SPARK_SESSION_EXTS_KEY: String = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
  val GLUTEN_SESSION_EXTENSION_NAME: String = Objects.requireNonNull(
    classOf[GlutenSessionExtensions].getCanonicalName)

  /**
   * Specify all injectors that Gluten is using in following list.
   */
  val DEFAULT_INJECTORS: List[GlutenSparkExtensionsInjector] = List(
    ColumnarQueryStagePreparations,
    ColumnarOverrides,
    StrategyOverrides,
    OthersExtensionOverrides
  )

  implicit def sparkConfImplicit(conf: SparkConf): SparkConfImplicits = {
    new SparkConfImplicits(conf)
  }

  def buildNativeConfNode(conf: SparkConf): PlanNode = {
    val nativeConfMap = new util.HashMap[String, String]()

    if (conf.contains(GlutenConfig.SPARK_HIVE_EXEC_ORC_STRIPE_SIZE)) {
      nativeConfMap.put(
        GlutenConfig.HIVE_EXEC_ORC_STRIPE_SIZE,
        conf.get(GlutenConfig.SPARK_HIVE_EXEC_ORC_STRIPE_SIZE))
    }
    if (conf.contains(GlutenConfig.SPARK_HIVE_EXEC_ORC_ROW_INDEX_STRIDE)) {
      nativeConfMap.put(
        GlutenConfig.HIVE_EXEC_ORC_ROW_INDEX_STRIDE,
        conf.get(GlutenConfig.SPARK_HIVE_EXEC_ORC_ROW_INDEX_STRIDE))
    }
    if (conf.contains(GlutenConfig.SPARK_HIVE_EXEC_ORC_COMPRESS)) {
      nativeConfMap.put(
        GlutenConfig.HIVE_EXEC_ORC_COMPRESS, conf.get(GlutenConfig.SPARK_HIVE_EXEC_ORC_COMPRESS))
    }

    nativeConfMap.put(
      GlutenConfig.SPARK_BATCH_SIZE, conf.get(GlutenConfig.SPARK_BATCH_SIZE, "32768"))

    val stringMapNode = ExpressionBuilder.makeStringMap(nativeConfMap)
    val extensionNode = ExtensionBuilder
      .makeAdvancedExtension(Any.pack(stringMapNode.toProtobuf))
    PlanBuilder.makePlan(extensionNode)
  }

  def initNative(conf: SparkConf): Unit = {
    // SQLConf is not initialed here, so it can not use 'GlutenConfig.getConf' to get conf.
    if (conf.getBoolean(GlutenConfig.GLUTEN_LOAD_NATIVE, defaultValue = true)) {
      val customGlutenLib = conf.get(GlutenConfig.GLUTEN_LIB_PATH, "")
      val customBackendLib = conf.get(GlutenConfig.GLUTEN_BACKEND_LIB, "")
      val initKernel = new ExpressionEvaluator(java.util.Collections.emptyList[String],
        conf.get(GlutenConfig.GLUTEN_LIB_NAME, "spark_columnar_jni"),
        customGlutenLib,
        customBackendLib,
        conf.getBoolean(GlutenConfig.GLUTEN_LOAD_ARROW, defaultValue = true))
      if (customGlutenLib.nonEmpty || customBackendLib.nonEmpty) {
        // Initialize the native backend with spark confs.
        initKernel.initNative(buildNativeConfNode(conf).toProtobuf.toByteArray)
      }
    }
  }
}
