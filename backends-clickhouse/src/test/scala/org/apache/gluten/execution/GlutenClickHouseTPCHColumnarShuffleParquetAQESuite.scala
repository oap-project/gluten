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

import org.apache.gluten.extension.GlutenPlan

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.execution.InputIteratorTransformer
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}

class GlutenClickHouseTPCHColumnarShuffleParquetAQESuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.sql.columnar.backend.ch.shuffle.hash.algorithm", "sparkMurmurHash3_32")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_config.enable_streaming_aggregating",
        "true")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  test("TPCH Q1") {
    runTPCHQuery(1) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case hashAggExec: HashAggregateExecBaseTransformer => hashAggExec
          case inputIteratorTransformer: InputIteratorTransformer => inputIteratorTransformer
        }
        assert(plans.size == 5)

        assert(plans(4).metrics("numFiles").value === 1)
        assert(plans(4).metrics("pruningTime").value === -1)
        assert(plans(4).metrics("filesSize").value === 19230111)
        assert(plans(4).metrics("numOutputRows").value === 600572)

        assert(plans(3).metrics("numInputRows").value === 591673)
        assert(plans(3).metrics("numOutputRows").value === 4)
        assert(plans(3).metrics("outputVectors").value === 1)

        assert(plans(2).metrics("numInputRows").value === 8)
        assert(plans(2).metrics("numOutputRows").value === 8)

        // Execute Sort operator, it will read the data twice.
        assert(plans(1).metrics("numOutputRows").value === 8)
        assert(plans(1).metrics("outputVectors").value === 2)

        assert(plans(0).metrics("numInputRows").value === 4)
        assert(plans(0).metrics("numOutputRows").value === 4)
    }
  }

  test("Check the metrics values") {
    withSQLConf(("spark.gluten.sql.columnar.sort", "false")) {
      runTPCHQuery(1, noFallBack = false) {
        df =>
          assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val plans = collect(df.queryExecution.executedPlan) {
            case scanExec: BasicScanExecTransformer => scanExec
            case hashAggExec: HashAggregateExecBaseTransformer => hashAggExec
          }
          assert(plans.size == 3)

          assert(plans(2).metrics("numFiles").value === 1)
          assert(plans(2).metrics("pruningTime").value === -1)
          assert(plans(2).metrics("filesSize").value === 19230111)

          assert(plans(1).metrics("numInputRows").value === 591673)
          assert(plans(1).metrics("numOutputRows").value === 4)
          assert(plans(1).metrics("outputVectors").value === 1)

          // Execute Sort operator, it will read the data twice.
          assert(plans(0).metrics("numOutputRows").value === 8)
          assert(plans(0).metrics("outputVectors").value === 2)
      }
    }
  }

  test("TPCH Q2") {
    runTPCHQuery(2) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val scanExec = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 8)
    }
  }

  test("TPCH Q3") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(3) {
        df =>
          assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val shjBuildLeft = collect(df.queryExecution.executedPlan) {
            case shj: ShuffledHashJoinExecTransformerBase if shj.joinBuildSide == BuildLeft => shj
          }
          assert(shjBuildLeft.size == 2)

          val inputIteratorTransformers = collect(df.queryExecution.executedPlan) {
            case inputIteratorTransformer: InputIteratorTransformer => inputIteratorTransformer
          }

          assert(inputIteratorTransformers.size == 4)

          assert(inputIteratorTransformers(3).metrics("numInputRows").value === 324322)
          assert(inputIteratorTransformers(3).metrics("numOutputRows").value === 324322)

          assert(inputIteratorTransformers(2).metrics("numInputRows").value === 72678)
          assert(inputIteratorTransformers(2).metrics("numOutputRows").value === 72678)

          assert(inputIteratorTransformers(1).metrics("numInputRows").value === 3111)
          assert(inputIteratorTransformers(1).metrics("numOutputRows").value === 3111)

          assert(inputIteratorTransformers(0).metrics("numInputRows").value === 15224)
          assert(inputIteratorTransformers(0).metrics("numOutputRows").value === 15224)
      }
    }
  }

  test("TPCH Q4") {
    runTPCHQuery(4) { df => }
  }

  test("TPCH Q5") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(5) {
        df =>
          assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val bhjRes = collect(df.queryExecution.executedPlan) {
            case bhj: BroadcastHashJoinExecTransformerBase => bhj
          }
          assert(bhjRes.isEmpty)
      }
    }
  }

  test("TPCH Q6") {
    runTPCHQuery(6) { df => }
  }

  /**
   * TODO: With Spark 3.3, it can not support to use Spark Shuffle Manager and set
   * shuffle.partitions=1 at the same time, because OptimizeOneRowPlan rule will remove Sort
   * operator.
   */
  test("TPCH Q7 - with shuffle.partitions=1") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(7) { df => }
    }
  }

  test("TPCH Q7") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "2"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(7) { df => }
    }
  }

  test("TPCH Q8") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(8) { df => }
    }
  }

  test("TPCH Q9") {
    runTPCHQuery(9, compareResult = false) { df => }
  }

  test("TPCH Q10") {
    runTPCHQuery(10) { df => }
  }

  test("TPCH Q11") {
    runTPCHQuery(11, compareResult = false) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
  }

  test("TPCH Q12") {
    runTPCHQuery(12) { df => }
  }

  test("TPCH Q13") {
    runTPCHQuery(13) { df => }
  }

  test("TPCH Q14") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(14) { df => }
    }
  }

  test("TPCH Q15") {
    runTPCHQuery(15) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
  }

  test("TPCH Q16") {
    runTPCHQuery(16, noFallBack = false) { df => }
  }

  test("TPCH Q17") {
    withSQLConf(("spark.shuffle.sort.bypassMergeThreshold", "2")) {
      runTPCHQuery(17) { df => }
    }
  }

  test("TPCH Q18") {
    withSQLConf(("spark.shuffle.sort.bypassMergeThreshold", "2")) {
      runTPCHQuery(18) { df => }
    }
  }

  test("TPCH Q19") {
    runTPCHQuery(19) { df => }
  }

  test("TPCH Q20") {
    runTPCHQuery(20) { df => }
  }

  test("TPCH Q21") {
    runTPCHQuery(21, noFallBack = false) {
      df =>
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case filterExec: FilterExecTransformerBase => filterExec
        }
        assert(plans(2).metrics("numInputRows").value === 600572)
        assert(plans(2).metrics("numOutputRows").value === 379809)

        assert(plans(3).metrics("numOutputRows").value === 600572)
    }
  }

  test("TPCH Q22") {
    runTPCHQuery(22) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
  }

  test("Test 'spark.gluten.enabled' false") {
    withSQLConf(("spark.gluten.enabled", "false")) {
      runTPCHQuery(2, noFallBack = false) {
        df =>
          val glutenPlans = collect(df.queryExecution.executedPlan) {
            case glutenPlan: GlutenPlan => glutenPlan
          }
          assert(glutenPlans.isEmpty)
      }
    }
  }

  test("collect_set") {
    val sql =
      """
        |select a, b from (
        |select n_regionkey as a, collect_set(if(n_regionkey=0, n_name, null))
        | as set from nation group by n_regionkey)
        |lateral view explode(set) as b
        |order by a, b
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[CHHashAggregateExecTransformer])
  }

  test("test 'aggregate function collect_list'") {
    val df = runQueryAndCompare(
      "select l_orderkey,from_unixtime(l_orderkey, 'yyyy-MM-dd HH:mm:ss') " +
        "from lineitem order by l_orderkey desc limit 10"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test max string") {
    withSQLConf(("spark.gluten.sql.columnar.force.hashagg", "true")) {
      val sql =
        """
          |SELECT
          |    l_returnflag,
          |    l_linestatus,
          |    max(l_comment)
          |FROM
          |    lineitem
          |WHERE
          |    l_shipdate <= date'1998-09-02' - interval 1 day
          |GROUP BY
          |    l_returnflag,
          |    l_linestatus
          |""".stripMargin
      runQueryAndCompare(sql) { df => }
    }
  }
}
