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
package org.apache.gluten.integration.tpc.h

import org.apache.gluten.integration.tpc.{DataGen, TpcSuite, TypeModifier}
import org.apache.gluten.integration.tpc.action.Action
import org.apache.gluten.integration.tpc.h.TpchSuite.{HISTORY_WRITE_PATH, TPCH_WRITE_PATH}

import org.apache.spark.SparkConf

import org.apache.log4j.Level

class TpchSuite(
    val masterUrl: String,
    val actions: Array[Action],
    val testConf: SparkConf,
    val baselineConf: SparkConf,
    val extraSparkConf: Map[String, String],
    val logLevel: Level,
    val errorOnMemLeak: Boolean,
    val enableUi: Boolean,
    val enableHsUi: Boolean,
    val hsUiPort: Int,
    val offHeapSize: String,
    val disableAqe: Boolean,
    val disableBhj: Boolean,
    val disableWscg: Boolean,
    val shufflePartitions: Int,
    val minimumScanPartitions: Boolean)
  extends TpcSuite(
    masterUrl,
    actions,
    testConf,
    baselineConf,
    extraSparkConf,
    logLevel,
    errorOnMemLeak,
    enableUi,
    enableHsUi,
    hsUiPort,
    offHeapSize,
    disableAqe,
    disableBhj,
    disableWscg,
    shufflePartitions,
    minimumScanPartitions
  ) {

  override protected def historyWritePath(): String = HISTORY_WRITE_PATH

  override private[tpc] def dataWritePath(scale: Double): String = TPCH_WRITE_PATH + s"-$scale"

  override private[tpc] def createDataGen(scale: Double, genPartitionedData: Boolean): DataGen =
    new TpchDataGen(
      sessionSwitcher.spark(),
      scale,
      shufflePartitions,
      dataWritePath(scale),
      typeModifiers())

  override private[tpc] def queryResource(): String = {
    "/tpch-queries"
  }

  override protected def typeModifiers(): List[TypeModifier] = {
    List()
  }

  override private[tpc] def allQueryIds(): Array[String] = TpchSuite.ALL_QUERY_IDS

  override private[tpc] def desc(): String = "TPC-H"
}

object TpchSuite {
  private val TPCH_WRITE_PATH = "/tmp/tpch-generated"
  private val ALL_QUERY_IDS = Array(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21",
    "q22")
  private val HISTORY_WRITE_PATH = "/tmp/tpch-history"
}
