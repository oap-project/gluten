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
package org.apache.gluten.execution

import org.apache.gluten.GlutenConfig
import org.apache.gluten.benchmarks.GenTPCDSTableScripts
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.{ClickhouseSnapshot, DeltaLog}
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.commons.io.FileUtils

import java.io.File
import java.util

import scala.io.Source

abstract class GlutenClickHouseTPCDSAbstractSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with Logging {
  private var _spark: SparkSession = _

  override protected def spark: SparkSession = _spark

  protected val tablesPath: String = UTSystemParameters.tpcdsDataPath + "/"
  protected val tpcdsQueries: String
  protected val queriesResults: String

  /** Return values: (sql num, is fall back, skip fall back assert) */
  def tpcdsAllQueries(isAqe: Boolean): Seq[(String, Boolean, Boolean)] =
    Range
      .inclusive(1, 99)
      .flatMap(
        queryNum => {
          val sqlNums = if (queryNum == 14 || queryNum == 23 || queryNum == 24 || queryNum == 39) {
            Seq("q" + "%d".format(queryNum) + "a", "q" + "%d".format(queryNum) + "b")
          } else {
            Seq("q" + "%d".format(queryNum))
          }
          val noFallBack = queryNum match {
            case i
                if i == 10 || i == 16 || i == 28 || i == 35 || i == 45 || i == 77 ||
                  i == 88 || i == 90 || i == 94 =>
              // Q10 BroadcastHashJoin, ExistenceJoin
              // Q16 ShuffledHashJoin, NOT condition
              // Q28 BroadcastNestedLoopJoin
              // Q35 BroadcastHashJoin, ExistenceJoin
              // Q45 BroadcastHashJoin, ExistenceJoin
              // Q77 CartesianProduct
              // Q88 BroadcastNestedLoopJoin
              // Q90 BroadcastNestedLoopJoin
              // Q94 BroadcastHashJoin, LeftSemi, NOT condition
              (false, false)
            case j if j == 38 || j == 87 =>
              // Q38 and Q87 : Hash shuffle is not supported for expression in some case
              if (isAqe) {
                (true, true)
              } else {
                (false, true)
              }
            case other => (true, false)
          }
          sqlNums.map((_, noFallBack._1, noFallBack._2))
        })

  // FIXME "q17", stddev_samp inconsistent results, CH return NaN, Spark return null
  protected def excludedTpcdsQueries: Set[String] = Set(
    "q18", // inconsistent results
    "q61", // inconsistent results
    "q67" // inconsistent results
  )

  def executeTPCDSTest(isAqe: Boolean): Unit = {
    tpcdsAllQueries(isAqe).foreach(
      s =>
        if (excludedTpcdsQueries.contains(s._1)) {
          ignore(s"TPCDS ${s._1.toUpperCase()}") {
            runTPCDSQuery(s._1, noFallBack = s._2, skipFallBackAssert = s._3) { df => }
          }
        } else {
          test(s"TPCDS ${s._1.toUpperCase()}") {
            runTPCDSQuery(s._1, noFallBack = s._2, skipFallBackAssert = s._3) { df => }
          }
        })
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
    createTPCDSTables()
  }

  override protected def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = SparkSession
        .builder()
        .appName("Gluten-UT-TPC_DS")
        .config(sparkConf)
        .getOrCreate()
    }
  }

//  override protected def createTPCHNotNullTables(): Unit = {}

  protected def createTPCDSTables(): Unit = {
    val parquetTables =
      GenTPCDSTableScripts.genTPCDSParquetTables("tpcdsdb", tablesPath, "", "")

    for (sql <- parquetTables) {
      spark.sql(sql)
    }
    spark.sql("use tpcdsdb;")
    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
    assert(result.length == 24)
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .setMaster("local[8]")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.gluten.sql.columnar.columnarToRow", "true")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.clickHouseLibPath)
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.sql.decimalOperations.allowPrecisionLoss", "false")
    /* .set("spark.sql.catalogImplementation", "hive")
    .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${
      metaStorePathAbsolute + "/metastore_db"};create=true") */
  }

  override protected def afterAll(): Unit = {
    ClickhouseSnapshot.clearAllFileStatusCache
    DeltaLog.clearCache()

    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }

    FileUtils.forceDelete(new File(basePath))
    // init GlutenConfig in the next beforeAll
    GlutenConfig.ins = null
  }

  protected def runTPCDSQuery(
      queryNum: String,
      tpcdsQueries: String = tpcdsQueries,
      queriesResults: String = queriesResults,
      compareResult: Boolean = true,
      noFallBack: Boolean = true,
      skipFallBackAssert: Boolean = false)(customCheck: DataFrame => Unit): Unit = {

    val sqlFile = tpcdsQueries + "/" + queryNum + ".sql"
    val df = spark.sql(Source.fromFile(new File(sqlFile), "UTF-8").mkString)

    if (compareResult) {
      val fields = new util.ArrayList[StructField]()
      for (elem <- df.schema) {
        fields.add(
          StructField
            .apply(elem.name + fields.size().toString, elem.dataType, elem.nullable, elem.metadata))
      }

      var expectedAnswer: Seq[Row] = null
      withSQLConf(vanillaSparkConfs(): _*) {
        expectedAnswer = spark.read
          .option("delimiter", "|-|")
          .option("nullValue", "null")
          .schema(StructType.apply(fields))
          .csv(queriesResults + "/" + queryNum + ".out")
          .toDF()
          .collect()
      }
      checkAnswer(df, expectedAnswer)
      // using WARN to guarantee printed
      log.warn(s"query: $queryNum, finish comparing with saved result")
    } else {
      val start = System.currentTimeMillis();
      val ret = df.collect()
      // using WARN to guarantee printed
      log.warn(s"query: $queryNum skipped comparing, time cost to collect: ${System
          .currentTimeMillis() - start} ms, ret size: ${ret.length}")
    }
    WholeStageTransformerSuite.checkFallBack(df, noFallBack, skipFallBackAssert)
    customCheck(df)
  }
}
