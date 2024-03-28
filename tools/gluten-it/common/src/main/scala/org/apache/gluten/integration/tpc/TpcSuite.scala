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
package org.apache.gluten.integration.tpc

import org.apache.gluten.integration.tpc.action.Action

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerHelper
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.ConfUtils.ConfImplicits._
import org.apache.spark.sql.SparkSessionSwitcher

import org.apache.log4j.{Level, LogManager}

import java.io.File
import java.util.Scanner

abstract class TpcSuite(
    private val masterUrl: String,
    private val actions: Array[Action],
    private val testConf: SparkConf,
    private val baselineConf: SparkConf,
    private val extraSparkConf: Map[String, String],
    private val logLevel: Level,
    private val errorOnMemLeak: Boolean,
    private val enableUi: Boolean,
    private val enableHsUi: Boolean,
    private val hsUiPort: Int,
    private val offHeapSize: String,
    private val disableAqe: Boolean,
    private val disableBhj: Boolean,
    private val disableWscg: Boolean,
    private val shufflePartitions: Int,
    private val minimumScanPartitions: Boolean) {

  resetLogLevel()

  private[tpc] val sessionSwitcher: SparkSessionSwitcher =
    new SparkSessionSwitcher(masterUrl, logLevel.toString)

  // define initial configs
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.sources.useV1SourceList", "")
  sessionSwitcher
    .defaultConf()
    .setWarningOnOverriding("spark.sql.shuffle.partitions", s"$shufflePartitions")
  sessionSwitcher
    .defaultConf()
    .setWarningOnOverriding("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
  sessionSwitcher
    .defaultConf()
    .setWarningOnOverriding("spark.executor.heartbeatInterval", "1s") // for keeping metrics updated
  sessionSwitcher
    .defaultConf()
    .setWarningOnOverriding("spark.executor.metrics.pollingInterval", "0")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.network.timeout", "3601s")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.broadcastTimeout", "1800")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.network.io.preferDirectBufs", "false")
  sessionSwitcher
    .defaultConf()
    .setWarningOnOverriding("spark.unsafe.exceptionOnMemoryLeak", s"$errorOnMemLeak")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.memory.offHeap.enabled", "true")
  sessionSwitcher.defaultConf().setWarningOnOverriding("spark.memory.offHeap.size", offHeapSize)

  if (!enableUi) {
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.ui.enabled", "false")
  }

  if (enableHsUi) {
    if (!new File(historyWritePath()).exists() && !new File(historyWritePath()).mkdirs()) {
      throw new RuntimeException(
        "Unable to create history directory: " +
          historyWritePath())
    }
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.eventLog.enabled", "true")
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.eventLog.dir", historyWritePath())
  }

  if (disableAqe) {
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.adaptive.enabled", "false")
  }

  if (disableBhj) {
    sessionSwitcher
      .defaultConf()
      .setWarningOnOverriding("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  if (disableWscg) {
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.sql.codegen.wholeStage", "false")
  }

  if (minimumScanPartitions) {
    sessionSwitcher
      .defaultConf()
      .setWarningOnOverriding("spark.sql.files.maxPartitionBytes", s"${ByteUnit.PiB.toBytes(1L)}")
    sessionSwitcher
      .defaultConf()
      .setWarningOnOverriding("spark.sql.files.openCostInBytes", s"${ByteUnit.PiB.toBytes(1L)}")
    sessionSwitcher.defaultConf().setWarningOnOverriding("spark.default.parallelism", "1")
  }

  extraSparkConf.toStream.foreach {
    kv => sessionSwitcher.defaultConf().setWarningOnOverriding(kv._1, kv._2)
  }

  // register sessions
  sessionSwitcher.registerSession("test", testConf)
  sessionSwitcher.registerSession("baseline", baselineConf)

  def startHistoryServer(): Unit = {
    val hsConf = new SparkConf(false)
    hsConf.setWarningOnOverriding("spark.history.ui.port", s"$hsUiPort")
    hsConf.setWarningOnOverriding("spark.history.fs.logDirectory", historyWritePath())
    HistoryServerHelper.startHistoryServer(hsConf)
  }

  // boot up history server
  if (enableHsUi) {
    startHistoryServer()
  }

  def run(): Boolean = {
    val succeed = actions.forall {
      action =>
        resetLogLevel() // to prevent log level from being set by unknown external codes
        action.execute(this)
    }
    succeed
  }

  def close(): Unit = {
    sessionSwitcher.close()
    // wait for input, if history server was started
    if (enableHsUi) {
      printf("History server was running at port %d. Press enter to exit... \n", hsUiPort)
      print("> ")
      new Scanner(System.in).nextLine
    }
  }

  private def resetLogLevel(): Unit = {
    LogManager.getRootLogger.setLevel(logLevel)
  }

  private[tpc] def getBaselineConf(): SparkConf = {
    baselineConf.clone()
  }

  private[tpc] def getTestConf(): SparkConf = {
    testConf.clone()
  }

  protected def historyWritePath(): String

  private[tpc] def dataWritePath(scale: Double): String

  private[tpc] def createDataGen(scale: Double, genPartitionedData: Boolean): DataGen

  private[tpc] def queryResource(): String

  protected def typeModifiers(): List[TypeModifier]

  private[tpc] def allQueryIds(): Array[String]

  private[tpc] def desc(): String

}

object TpcSuite {
  implicit class TpcSuiteImplicits(suite: TpcSuite) {
    def selectQueryIds(queryIds: Array[String], excludedQueryIds: Array[String]): Array[String] = {
      if (queryIds.nonEmpty && excludedQueryIds.nonEmpty) {
        throw new IllegalArgumentException(
          "Should not specify queries and excluded queries at the same time")
      }
      val all = suite.allQueryIds()
      val allSet = all.toSet
      if (queryIds.nonEmpty) {
        assert(
          queryIds.forall(id => allSet.contains(id)),
          "Invalid query ID: " + queryIds.collectFirst {
            case id if !allSet.contains(id)=>
              id
          }.get)
        return queryIds
      }
      if (excludedQueryIds.nonEmpty) {
        assert(
          excludedQueryIds.forall(id => allSet.contains(id)),
          "Invalid query ID to exclude: " + excludedQueryIds.collectFirst {
            case id if !allSet.contains(id)=>
              id
          }.get)
        val excludedSet = excludedQueryIds.toSet
        return all.filterNot(excludedSet.contains)
      }
      all
    }
  }
}
