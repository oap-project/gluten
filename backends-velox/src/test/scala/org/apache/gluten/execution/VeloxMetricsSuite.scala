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
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

class VeloxMetricsSuite extends VeloxWholeStageTransformerSuite with AdaptiveSparkPlanHelper {
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark
      .range(100)
      .selectExpr("id as c1", "id % 3 as c2")
      .write
      .format("parquet")
      .saveAsTable("metrics_t1")

    spark
      .range(200)
      .selectExpr("id as c1", "id % 3 as c2")
      .write
      .format("parquet")
      .saveAsTable("metrics_t2")
  }

  override protected def afterAll(): Unit = {
    spark.sql("drop table metrics_t1")
    spark.sql("drop table metrics_t2")

    super.afterAll()
  }

  test("test sort merge join metrics") {
    withSQLConf(
      GlutenConfig.COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // without preproject
      runQueryAndCompare(
        "SELECT * FROM metrics_t1 join metrics_t2 on metrics_t1.c1 = metrics_t2.c1"
      ) {
        df =>
          val smj = find(df.queryExecution.executedPlan) {
            case _: SortMergeJoinExecTransformer => true
            case _ => false
          }
          assert(smj.isDefined)
          val metrics = smj.get.metrics
          assert(metrics("numOutputRows").value == 100)
          assert(metrics("numOutputVectors").value > 0)
          assert(metrics("numOutputBytes").value > 0)
      }

      // with preproject
      runQueryAndCompare(
        "SELECT * FROM metrics_t1 join metrics_t2 on metrics_t1.c1 + 1 = metrics_t2.c1 + 1"
      ) {
        df =>
          val smj = find(df.queryExecution.executedPlan) {
            case _: SortMergeJoinExecTransformer => true
            case _ => false
          }
          assert(smj.isDefined)
          val metrics = smj.get.metrics
          assert(metrics("numOutputRows").value == 100)
          assert(metrics("numOutputVectors").value > 0)
          assert(metrics("streamPreProjectionCpuCount").value > 0)
          assert(metrics("bufferPreProjectionCpuCount").value > 0)
      }
    }
  }

  test("test shuffle hash join metrics") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // without preproject
      runQueryAndCompare(
        "SELECT * FROM metrics_t1 join metrics_t2 on metrics_t1.c1 = metrics_t2.c1"
      ) {
        df =>
          val smj = find(df.queryExecution.executedPlan) {
            case _: ShuffledHashJoinExecTransformer => true
            case _ => false
          }
          assert(smj.isDefined)
          val metrics = smj.get.metrics
          assert(metrics("numOutputRows").value == 100)
          assert(metrics("numOutputVectors").value > 0)
          assert(metrics("numOutputBytes").value > 0)
      }

      // with preproject
      runQueryAndCompare(
        "SELECT * FROM metrics_t1 join metrics_t2 on metrics_t1.c1 + 1 = metrics_t2.c1 + 1"
      ) {
        df =>
          val smj = find(df.queryExecution.executedPlan) {
            case _: ShuffledHashJoinExecTransformer => true
            case _ => false
          }
          assert(smj.isDefined)
          val metrics = smj.get.metrics
          assert(metrics("numOutputRows").value == 100)
          assert(metrics("numOutputVectors").value > 0)
          assert(metrics("streamPreProjectionCpuCount").value > 0)
          assert(metrics("buildPreProjectionCpuCount").value > 0)
      }
    }
  }

  test("Write metrics") {
    if (SparkShimLoader.getSparkVersion.startsWith("3.4")) {
      withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
        runQueryAndCompare(
          "Insert into table metrics_t1 values(1 , 2)"
        ) {
          df =>
            val plan =
              df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan
            val write = find(plan) {
              case _: WriteFilesExecTransformer => true
              case _ => false
            }
            assert(write.isDefined)
            val metrics = write.get.metrics
            assert(metrics("physicalWrittenBytes").value > 0)
            assert(metrics("numWrittenFiles").value == 1)
        }
      }
    }
  }
}
