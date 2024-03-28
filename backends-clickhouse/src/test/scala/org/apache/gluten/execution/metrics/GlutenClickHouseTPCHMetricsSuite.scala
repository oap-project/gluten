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
package org.apache.gluten.execution.metrics

import org.apache.gluten.execution._
import org.apache.gluten.extension.GlutenPlan
import org.apache.gluten.vectorized.GeneralInIterator

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.InputIteratorTransformer

import scala.collection.JavaConverters._

class GlutenClickHouseTPCHMetricsSuite extends GlutenClickHouseTPCHAbstractSuite {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  protected val metricsJsonFilePath: String = rootPath + "metrics-json"
  protected val substraitPlansDatPath: String = rootPath + "substrait-plans"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_config.enable_streaming_aggregating",
        "true")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  test("TPCH Q1 scan metrics") {
    runTPCHQuery(1) {
      df =>
        val plans = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
          case hashAggExec: HashAggregateExecBaseTransformer => hashAggExec
        }
        assert(plans.size == 3)

        assert(plans(2).metrics("numFiles").value === 1)
        assert(plans(2).metrics("pruningTime").value === -1)
        assert(plans(2).metrics("filesSize").value === 19230111)

        assert(plans(1).metrics("numOutputRows").value === 4)
        assert(plans(1).metrics("outputVectors").value === 1)

        // Execute Sort operator, it will read the data twice.
        assert(plans(0).metrics("numOutputRows").value === 4)
        assert(plans(0).metrics("outputVectors").value === 1)
    }
  }

  test("test Generate metrics") {
    val sql =
      """
        |select n_nationkey, a from nation lateral view explode(split(n_comment, ' ')) as a
        |order by n_nationkey, a
        |""".stripMargin
    runQueryAndCompare(sql) {
      df =>
        val plans = df.queryExecution.executedPlan.collect {
          case generate: CHGenerateExecTransformer => generate
        }
        assert(plans.size == 1)
        assert(plans.head.metrics("numInputRows").value == 25)
        assert(plans.head.metrics("numOutputRows").value == 266)
        assert(plans.head.metrics("outputVectors").value == 1)
    }
  }

  test("Check the metrics values") {
    withSQLConf(("spark.gluten.sql.columnar.sort", "false")) {
      runTPCHQuery(1, noFallBack = false) {
        df =>
          val plans = df.queryExecution.executedPlan.collect {
            case scanExec: BasicScanExecTransformer => scanExec
            case hashAggExec: HashAggregateExecBaseTransformer => hashAggExec
          }
          assert(plans.size == 3)

          assert(plans(2).metrics("numFiles").value === 1)
          assert(plans(2).metrics("pruningTime").value === -1)
          assert(plans(2).metrics("filesSize").value === 19230111)

          assert(plans(1).metrics("numOutputRows").value === 4)
          assert(plans(1).metrics("outputVectors").value === 1)

          // Execute Sort operator, it will read the data twice.
          assert(plans(0).metrics("numOutputRows").value === 4)
          assert(plans(0).metrics("outputVectors").value === 1)
      }
    }
  }

  test("test tpch wholestage execute") {
    val inBatchIters = new java.util.ArrayList[GeneralInIterator](0)
    val outputAttributes = new java.util.ArrayList[Attribute](0)
    val nativeMetricsList = GlutenClickHouseMetricsUTUtils
      .executeSubstraitPlan(
        substraitPlansDatPath + "/tpch-q4-wholestage-2.json",
        basePath,
        inBatchIters,
        outputAttributes
      )

    assert(nativeMetricsList.size == 1)
    val nativeMetricsData = nativeMetricsList(0)
    assert(nativeMetricsData.metricsDataList.size() == 3)

    assert(nativeMetricsData.metricsDataList.get(0).getName.equals("kRead"))
    assert(
      nativeMetricsData.metricsDataList
        .get(0)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 600572)

    assert(nativeMetricsData.metricsDataList.get(1).getName.equals("kFilter"))
    assert(
      nativeMetricsData.metricsDataList
        .get(1)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getInputRows == 600572)
    assert(
      nativeMetricsData.metricsDataList
        .get(1)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 379809)

    assert(nativeMetricsData.metricsDataList.get(2).getName.equals("kProject"))
    assert(
      nativeMetricsData.metricsDataList
        .get(2)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 379809)
  }

  test("Check TPCH Q2 metrics updater") {
    withDataFrame(tpchSQL(2, tpchQueries)) {
      q2Df =>
        val allWholeStageTransformers = q2Df.queryExecution.executedPlan.collect {
          case wholeStage: WholeStageTransformer => wholeStage
        }
        assert(allWholeStageTransformers.size == 10)

        val wholeStageTransformer0 = allWholeStageTransformers(2)

        GlutenClickHouseMetricsUTUtils.executeMetricsUpdater(
          wholeStageTransformer0,
          metricsJsonFilePath + "/tpch-q2-wholestage-1-metrics.json"
        ) {
          () =>
            wholeStageTransformer0.collect {
              case s: FileSourceScanExecTransformer =>
                assert(s.metrics("scanTime").value == 2)
                assert(s.metrics("inputWaitTime").value == 4)
                assert(s.metrics("outputWaitTime").value == 2)
                assert(s.metrics("numOutputRows").value == 20000)
                assert(s.metrics("outputBytes").value == 1451663)
              case f: FilterExecTransformerBase =>
                assert(f.metrics("totalTime").value == 3)
                assert(f.metrics("inputWaitTime").value == 14)
                assert(f.metrics("outputWaitTime").value == 1)
                assert(f.metrics("numOutputRows").value == 73)
                assert(f.metrics("outputBytes").value == 5304)
                assert(f.metrics("numInputRows").value == 20000)
                assert(f.metrics("inputBytes").value == 1451663)
                assert(f.metrics("extraTime").value == 1)
              case p: ProjectExecTransformer =>
                assert(p.metrics("totalTime").value == 0)
                assert(p.metrics("inputWaitTime").value == 7)
                assert(p.metrics("outputWaitTime").value == 0)
                assert(p.metrics("numOutputRows").value == 73)
                assert(p.metrics("outputBytes").value == 2336)
                assert(p.metrics("numInputRows").value == 73)
                assert(p.metrics("inputBytes").value == 5085)
            }
        }

        val wholeStageTransformer1 = allWholeStageTransformers(1)

        GlutenClickHouseMetricsUTUtils.executeMetricsUpdater(
          wholeStageTransformer1,
          metricsJsonFilePath + "/tpch-q2-wholestage-2-metrics.json"
        ) {
          () =>
            val allGlutenPlans = wholeStageTransformer1.collect {
              case g: GlutenPlan if !g.isInstanceOf[InputIteratorTransformer] => g
            }

            val scanPlan = allGlutenPlans(9)
            assert(scanPlan.metrics("scanTime").value == 2)
            assert(scanPlan.metrics("inputWaitTime").value == 3)
            assert(scanPlan.metrics("outputWaitTime").value == 1)
            assert(scanPlan.metrics("numOutputRows").value == 80000)
            assert(scanPlan.metrics("outputBytes").value == 2160000)

            val filterPlan = allGlutenPlans(8)
            assert(filterPlan.metrics("totalTime").value == 1)
            assert(filterPlan.metrics("inputWaitTime").value == 13)
            assert(filterPlan.metrics("outputWaitTime").value == 1)
            assert(filterPlan.metrics("numOutputRows").value == 80000)
            assert(filterPlan.metrics("outputBytes").value == 2160000)
            assert(filterPlan.metrics("numInputRows").value == 80000)
            assert(filterPlan.metrics("inputBytes").value == 2160000)

            val joinPlan = allGlutenPlans(2)
            assert(joinPlan.metrics("totalTime").value == 1)
            assert(joinPlan.metrics("inputWaitTime").value == 6)
            assert(joinPlan.metrics("outputWaitTime").value == 0)
            assert(joinPlan.metrics("numOutputRows").value == 292)
            assert(joinPlan.metrics("outputBytes").value == 16644)
            assert(joinPlan.metrics("numInputRows").value == 80000)
            assert(joinPlan.metrics("inputBytes").value == 1920000)
        }

        val wholeStageTransformer2 = allWholeStageTransformers(0)

        GlutenClickHouseMetricsUTUtils.executeMetricsUpdater(
          wholeStageTransformer2,
          metricsJsonFilePath + "/tpch-q2-wholestage-11-metrics.json"
        ) {
          () =>
            val allGlutenPlans = wholeStageTransformer2.collect {
              case g: GlutenPlan if !g.isInstanceOf[InputIteratorTransformer] => g
            }

            assert(allGlutenPlans.size == 58)

            val shjPlan = allGlutenPlans(8)
            assert(shjPlan.metrics("totalTime").value == 6)
            assert(shjPlan.metrics("inputWaitTime").value == 5)
            assert(shjPlan.metrics("outputWaitTime").value == 0)
            assert(shjPlan.metrics("numOutputRows").value == 44)
            assert(shjPlan.metrics("outputBytes").value == 3740)
            assert(shjPlan.metrics("numInputRows").value == 11985)
            assert(shjPlan.metrics("inputBytes").value == 299625)
        }
    }
  }

  test("GLUTEN-1754: test agg func covar_samp, covar_pop final stage execute") {
    val inBatchIters = new java.util.ArrayList[GeneralInIterator](0)
    val outputAttributes = new java.util.ArrayList[Attribute](0)
    val nativeMetricsList = GlutenClickHouseMetricsUTUtils
      .executeSubstraitPlan(
        substraitPlansDatPath + "/covar_samp-covar_pop-partial-agg-stage.json",
        basePath,
        inBatchIters,
        outputAttributes
      )

    assert(nativeMetricsList.size == 1)
    val nativeMetricsData = nativeMetricsList(0)
    assert(nativeMetricsData.metricsDataList.size() == 5)

    assert(nativeMetricsData.metricsDataList.get(0).getName.equals("kRead"))
    assert(
      nativeMetricsData.metricsDataList
        .get(0)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 600572)

    assert(nativeMetricsData.metricsDataList.get(1).getName.equals("kFilter"))
    assert(
      nativeMetricsData.metricsDataList
        .get(1)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getInputRows == 600572)
    assert(
      nativeMetricsData.metricsDataList
        .get(1)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 591673)

    assert(nativeMetricsData.metricsDataList.get(2).getName.equals("kProject"))

    assert(nativeMetricsData.metricsDataList.get(3).getName.equals("kProject"))
    assert(nativeMetricsData.metricsDataList.get(4).getName.equals("kAggregate"))
    assert(
      nativeMetricsData.metricsDataList
        .get(4)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getInputRows == 591673)
    assert(
      nativeMetricsData.metricsDataList
        .get(4)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 4)

    assert(
      nativeMetricsData.metricsDataList
        .get(4)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 4)

    val inBatchItersFinal = new java.util.ArrayList[GeneralInIterator](
      Array(0).map(iter => new ColumnarNativeIterator(Iterator.empty.asJava)).toSeq.asJava)
    val outputAttributesFinal = new java.util.ArrayList[Attribute](0)

    val nativeMetricsListFinal = GlutenClickHouseMetricsUTUtils
      .executeSubstraitPlan(
        substraitPlansDatPath + "/covar_samp-covar_pop-final-agg-stage.json",
        basePath,
        inBatchItersFinal,
        outputAttributesFinal
      )

    assert(nativeMetricsListFinal.size == 1)
    val nativeMetricsDataFinal = nativeMetricsListFinal(0)
    assert(nativeMetricsDataFinal.metricsDataList.size() == 3)

    assert(nativeMetricsDataFinal.metricsDataList.get(0).getName.equals("kRead"))
    assert(nativeMetricsDataFinal.metricsDataList.get(1).getName.equals("kAggregate"))
    assert(nativeMetricsDataFinal.metricsDataList.get(1).getSteps.size() == 2)
    assert(
      nativeMetricsDataFinal.metricsDataList
        .get(1)
        .getSteps
        .get(0)
        .getName
        .equals("GraceMergingAggregatedStep"))
    assert(
      nativeMetricsDataFinal.metricsDataList.get(1).getSteps.get(1).getName.equals("Expression"))
    assert(nativeMetricsDataFinal.metricsDataList.get(2).getName.equals("kProject"))
  }
}
