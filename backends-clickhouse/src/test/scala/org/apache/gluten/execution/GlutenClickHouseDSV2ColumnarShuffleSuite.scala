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

import org.apache.spark.SparkConf

class GlutenClickHouseDSV2ColumnarShuffleSuite extends GlutenClickHouseTPCHAbstractSuite {

  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  /** Run Gluten + ClickHouse Backend with ColumnarShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  test("TPCH Q1") {
    runTPCHQuery(1) { df => }
  }

  test("TPCH Q2") {
    runTPCHQuery(2) { df => }
  }

  test("TPCH Q3") {
    runTPCHQuery(3) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 1)
    }
  }

  test("TPCH Q4") {
    runTPCHQuery(4) { df => }
  }

  test("TPCH Q5") {
    runTPCHQuery(5) { df => }
  }

  test("TPCH Q6") {
    runTPCHQuery(6) { df => }
  }

  test("TPCH Q7") {
    runTPCHQuery(7) { df => }
  }

  test("TPCH Q8") {
    runTPCHQuery(8) { df => }
  }

  test("TPCH Q9") {
    runTPCHQuery(9) { df => }
  }

  test("TPCH Q10") {
    runTPCHQuery(10) { df => }
  }

  test("TPCH Q11") {
    runTPCHQuery(11) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 3)
    }
  }

  test("TPCH Q12") {
    runTPCHQuery(12) { df => }
  }

  test("TPCH Q13") {
    runTPCHQuery(13) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 3)
    }
  }

  test("TPCH Q14") {
    runTPCHQuery(14) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 1)
    }
  }

  test("TPCH Q15") {
    runTPCHQuery(15) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 4)
    }
  }

  test("TPCH Q16") {
    runTPCHQuery(16, noFallBack = false) { df => }
  }

  test("TPCH Q17") {
    runTPCHQuery(17) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 3)
    }
  }

  test("TPCH Q18") {
    runTPCHQuery(18) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 4)
    }
  }

  test("TPCH Q19") {
    runTPCHQuery(19) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 1)
    }
  }

  test("TPCH Q20") {
    runTPCHQuery(20) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 1)
    }
  }

  test("TPCH Q21") {
    runTPCHQuery(21, noFallBack = false) { df => }
  }

  test("TPCH Q22") {
    runTPCHQuery(22) { df => }
  }
}
