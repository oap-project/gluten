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
package io.glutenproject.integration.tpc.ds

import io.glutenproject.integration.tpc.{Constants, DataGen, TpcSuite, TypeModifier}
import io.glutenproject.integration.tpc.action.Action
import io.glutenproject.integration.tpc.ds.TpcdsSuite.{ALL_QUERY_IDS, HISTORY_WRITE_PATH, TPCDS_WRITE_PATH}

import org.apache.spark.SparkConf

import org.apache.log4j.Level

class TpcdsSuite(
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

  override private[tpc] def dataWritePath(scale: Double): String = TPCDS_WRITE_PATH + s"-$scale"

  override private[tpc] def createDataGen(scale: Double, genPartitionedData: Boolean): DataGen =
    new TpcdsDataGen(
      sessionSwitcher.spark(),
      scale,
      shufflePartitions,
      dataWritePath(scale),
      typeModifiers(),
      genPartitionedData)

  override private[tpc] def queryResource(): String = {
    "/tpcds-queries"
  }

  // TODO need add tpcds golden files to resources
  override private[tpc] def goldenFileFolder(): String = {
    "/tpcds-approved-plans"
  }

  override protected def typeModifiers(): List[TypeModifier] = {
    List()
  }

  override private[tpc] def allQueryIds(): Array[String] = ALL_QUERY_IDS

  override private[tpc] def desc(): String = "TPC-DS"
}

object TpcdsSuite {
  private val TPCDS_WRITE_PATH = "/tmp/tpcds-generated"
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
    "q14a",
    "q14b",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21",
    "q22",
    "q23a",
    "q23b",
    "q24a",
    "q24b",
    "q25",
    "q26",
    "q27",
    "q28",
    "q29",
    "q30",
    "q31",
    "q32",
    "q33",
    "q34",
    "q35",
    "q36",
    "q37",
    "q38",
    "q39a",
    "q39b",
    "q40",
    "q41",
    "q42",
    "q43",
    "q44",
    "q45",
    "q46",
    "q47",
    "q48",
    "q49",
    "q50",
    "q51",
    "q52",
    "q53",
    "q54",
    "q55",
    "q56",
    "q57",
    "q58",
    "q59",
    "q60",
    "q61",
    "q62",
    "q63",
    "q64",
    "q65",
    "q66",
    "q67",
    "q68",
    "q69",
    "q70",
    "q71",
    "q72",
    "q73",
    "q74",
    "q75",
    "q76",
    "q77",
    "q78",
    "q79",
    "q80",
    "q81",
    "q82",
    "q83",
    "q84",
    "q85",
    "q86",
    "q87",
    "q88",
    "q89",
    "q90",
    "q91",
    "q92",
    "q93",
    "q94",
    "q95",
    "q96",
    "q97",
    "q98",
    "q99"
  )
  private val HISTORY_WRITE_PATH = "/tmp/tpcds-history"
}
