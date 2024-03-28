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
package org.apache.gluten.benchmarks

import org.apache.gluten.benchmarks.ShuffleWriterFuzzerTest.{Failed, OOM, Successful, TestResult}
import org.apache.gluten.execution.VeloxWholeStageTransformerSuite
import org.apache.gluten.memory.memtarget.ThrowOnOomMemoryTarget
import org.apache.gluten.tags.{FuzzerTest, SkipTestTags}

import org.apache.spark.SparkConf

object ShuffleWriterFuzzerTest {
  trait TestResult {
    val seed: Long

    def getSeed: Long = seed
  }
  case class Successful(seed: Long) extends TestResult
  case class Failed(seed: Long) extends TestResult
  case class OOM(seed: Long) extends TestResult
}

@FuzzerTest
@SkipTestTags
class ShuffleWriterFuzzerTest extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  private val dataGenerator = RandomParquetDataGenerator(System.currentTimeMillis())
  private val outputPath = getClass.getResource("/").getPath + "fuzzer_output.parquet"

  private val REPARTITION_SQL = (numPartitions: Int) =>
    s"select /*+ REPARTITION($numPartitions) */ * from tbl"
  private val AGG_REPARTITION_SQL =
    """select count(*) as cnt, f_1, f_2, f_3, f_4, f_5, f_6
      |from tbl group by f_1, f_2, f_3, f_4, f_5, f_6
      |order by cnt, f_1, f_2, f_3, f_4, f_5, f_6""".stripMargin

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "512MB")
      .set("spark.driver.memory", "4g")
      .set("spark.driver.maxResultSize", "4g")
  }

  def getRootCause(e: Throwable): Throwable = {
    if (e.getCause == null) {
      return e
    }
    getRootCause(e.getCause)
  }

  def executeQuery(sql: String): TestResult = {
    try {
      System.gc()
      dataGenerator.generateRandomData(spark, outputPath)
      spark.read.format("parquet").load(outputPath).createOrReplaceTempView("tbl")
      runQueryAndCompare(sql, true, false)(_ => {})
      Successful(dataGenerator.getSeed)
    } catch {
      case oom: ThrowOnOomMemoryTarget.OutOfMemoryException =>
        logError(s"Out of memory while running test with seed: ${dataGenerator.getSeed}", oom)
        OOM(dataGenerator.getSeed)
      case t: Throwable =>
        if (
          getRootCause(t).getMessage.contains(
            classOf[ThrowOnOomMemoryTarget.OutOfMemoryException].getName)
        ) {
          logError(s"Out of memory while running test with seed: ${dataGenerator.getSeed}", t)
          OOM(dataGenerator.getSeed)
        } else {
          logError(s"Failed to run test with seed: ${dataGenerator.getSeed}", t)
          Failed(dataGenerator.getSeed)
        }
    }
  }

  def repeatQuery(sql: String, iterations: Int, testName: String): Unit = {
    val result = (0 until iterations)
      .map {
        i =>
          logWarning(
            s"==============================> " +
              s"Started iteration $i (seed: ${dataGenerator.getSeed})")
          val result = executeQuery(sql)
          dataGenerator.reFake(System.currentTimeMillis())
          result
      }
    val oom = result.filter(_.isInstanceOf[OOM]).map(_.getSeed)
    if (oom.nonEmpty) {
      logError(s"Out of memory while running test '$testName' with seed: ${oom.mkString(", ")}")
    }
    val failed = result.filter(_.isInstanceOf[Failed]).map(_.getSeed)
    assert(failed.isEmpty, s"Failed to run test '$testName' with seed: ${failed.mkString(",")}")
  }

  private val REPARTITION_TEST_NAME = (numPartitions: Int) => s"repartition - $numPartitions"
  for (numPartitions <- Seq(1, 3, 10, 100, 1000, 4000, 8000)) {
    val testName = REPARTITION_TEST_NAME(numPartitions)
    test(testName) {
      repeatQuery(REPARTITION_SQL(numPartitions), 10, testName)
    }
  }

  private val AGG_TEST_NAME = "with aggregation"
  ignore(AGG_TEST_NAME) {
    repeatQuery(AGG_REPARTITION_SQL, 10, AGG_TEST_NAME)
  }

  ignore("reproduce") {
    // Replace sql with the actual failed sql.
    val sql = REPARTITION_SQL(100)
    // Replace seed '0L' with the actual failed seed.
    Seq(0L).foreach {
      seed =>
        dataGenerator.reFake(seed)
        logWarning(
          s"==============================> " +
            s"Started reproduction (seed: ${dataGenerator.getSeed})")
        val result = executeQuery(sql)
        assert(result.isInstanceOf[Successful], s"Failed to run 'reproduce' with seed: $seed")
    }
  }
}
