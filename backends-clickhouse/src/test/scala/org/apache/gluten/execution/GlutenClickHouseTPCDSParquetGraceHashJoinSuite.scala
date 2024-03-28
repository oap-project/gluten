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

import org.apache.gluten.utils.FallbackUtil

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Not}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

class GlutenClickHouseTPCDSParquetGraceHashJoinSuite extends GlutenClickHouseTPCDSAbstractSuite {

  override protected val tpcdsQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpcds-queries/tpcds.queries.original"
  override protected val queriesResults: String = rootPath + "tpcds-queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.memory.offHeap.size", "8g")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_settings.join_algorithm", "grace_hash")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_settings.max_bytes_in_join", "3145728")
  }

  executeTPCDSTest(false);

  test(
    "test fallback operations not supported by ch backend " +
      "in CHHashJoinExecTransformer && CHBroadcastHashJoinExecTransformer") {
    val testSql =
      """
        | SELECT i_brand_id AS brand_id, i_brand AS brand, i_manufact_id, i_manufact,
        |     sum(ss_ext_sales_price) AS ext_price
        | FROM date_dim
        | LEFT JOIN store_sales ON d_date_sk = ss_sold_date_sk
        | LEFT JOIN item ON ss_item_sk = i_item_sk AND i_manager_id = 7
        | LEFT JOIN customer ON ss_customer_sk = c_customer_sk
        | LEFT JOIN customer_address ON c_current_addr_sk = ca_address_sk
        | LEFT JOIN store ON ss_store_sk = s_store_sk AND substr(ca_zip,1,5) <> substr(s_zip,1,5)
        | WHERE d_moy = 11
        |   AND d_year = 1999
        | GROUP BY i_brand_id, i_brand, i_manufact_id, i_manufact
        | ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
        | LIMIT 100;
        |""".stripMargin

    val df = spark.sql(testSql)
    val operateWithCondition = df.queryExecution.executedPlan.collect {
      case f: BroadcastHashJoinExec if f.condition.get.isInstanceOf[Not] => f
    }
    assert(
      operateWithCondition(0).left
        .asInstanceOf[InputAdapter]
        .child
        .isInstanceOf[CHColumnarToRowExec])
  }

  test("test fallbackutils") {
    val testSql =
      """
        | SELECT i_brand_id AS brand_id, i_brand AS brand, i_manufact_id, i_manufact,
        |    sum(ss_ext_sales_price) AS ext_price
        | FROM date_dim
        | LEFT JOIN store_sales ON d_date_sk = ss_sold_date_sk
        | LEFT JOIN item ON ss_item_sk = i_item_sk AND i_manager_id = 7
        | LEFT JOIN customer ON ss_customer_sk = c_customer_sk
        | LEFT JOIN customer_address ON c_current_addr_sk = ca_address_sk
        | LEFT JOIN store ON ss_store_sk = s_store_sk AND substr(ca_zip,1,5) <> substr(s_zip,1,5)
        | WHERE d_moy = 11
        |   AND d_year = 1999
        | GROUP BY i_brand_id, i_brand, i_manufact_id, i_manufact
        | ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
        | LIMIT 100;
        |""".stripMargin

    val df = spark.sql(testSql)
    assert(FallbackUtil.hasFallback(df.queryExecution.executedPlan))
  }

  test("Gluten-4458: test clickhouse not support join with IN condition") {
    val testSql =
      """
        | SELECT *
        | FROM date_dim t1
        | LEFT JOIN date_dim t2 ON t1.d_date_sk = t2.d_date_sk
        |   AND datediff(t1.d_day_name, t2.d_day_name) IN (1, 3)
        | LIMIT 100;
        |""".stripMargin

    val df = spark.sql(testSql)
    assert(FallbackUtil.hasFallback(df.queryExecution.executedPlan))
  }

  test("Gluten-4458: test join with Equal computing two table in one side") {
    val testSql =
      """
        | SELECT *
        | FROM date_dim t1
        | LEFT JOIN date_dim t2 ON t1.d_date_sk = t2.d_date_sk AND t1.d_year - t2.d_year = 1
        | LIMIT 100;
        |""".stripMargin

    val df = spark.sql(testSql)
    assert(FallbackUtil.hasFallback(df.queryExecution.executedPlan))
  }

  test("Gluten-4458: test inner join can support join with IN condition") {
    val testSql =
      """
        | SELECT *
        | FROM date_dim t1
        | INNER JOIN date_dim t2 ON t1.d_date_sk = t2.d_date_sk
        |   AND datediff(t1.d_day_name, t2.d_day_name) IN (1, 3)
        | LIMIT 100;
        |""".stripMargin

    val df = spark.sql(testSql)
    assert(!FallbackUtil.hasFallback(df.queryExecution.executedPlan))
  }

  test("Gluten-1235: Fix missing reading from the broadcasted value when executing DPP") {
    val testSql =
      """
        |select dt.d_year
        |       ,sum(ss_ext_sales_price) sum_agg
        | from  date_dim dt
        |      ,store_sales
        | where dt.d_date_sk = store_sales.ss_sold_date_sk
        |   and dt.d_moy=12
        | group by dt.d_year
        | order by dt.d_year
        |         ,sum_agg desc
        |  LIMIT 100 ;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      testSql,
      true,
      df => {
        val foundDynamicPruningExpr = df.queryExecution.executedPlan.collect {
          case f: FileSourceScanExecTransformer => f
        }
        assert(foundDynamicPruningExpr.size == 2)
        assert(
          foundDynamicPruningExpr(1)
            .asInstanceOf[FileSourceScanExecTransformer]
            .partitionFilters
            .exists(_.isInstanceOf[DynamicPruningExpression]))
        assert(
          foundDynamicPruningExpr(1)
            .asInstanceOf[FileSourceScanExecTransformer]
            .selectedPartitions
            .size == 1823)
      }
    )
  }

  test("TPCDS Q21 with DPP + SHJ") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")) {
      runTPCDSQuery("q21") {
        df =>
          val foundDynamicPruningExpr = df.queryExecution.executedPlan.find {
            case f: FileSourceScanExecTransformer =>
              f.partitionFilters.exists {
                case _: DynamicPruningExpression => true
                case _ => false
              }
            case _ => false
          }
          assert(foundDynamicPruningExpr.nonEmpty == true)

          val reuseExchange = df.queryExecution.executedPlan.find {
            case r: ReusedExchangeExec => true
            case _ => false
          }
          assert(reuseExchange.isEmpty)
      }
    }
  }

  test("TPCDS Q21 with non-separated scan rdd") {
    withSQLConf(("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", "false")) {
      runTPCDSQuery("q21") {
        df =>
          val foundDynamicPruningExpr = df.queryExecution.executedPlan.find {
            case f: FileSourceScanExecTransformer =>
              f.partitionFilters.exists {
                case _: DynamicPruningExpression => true
                case _ => false
              }
            case _ => false
          }
          assert(foundDynamicPruningExpr.nonEmpty == true)

          val reuseExchange = df.queryExecution.executedPlan.find {
            case r: ReusedExchangeExec => true
            case _ => false
          }
          assert(reuseExchange.nonEmpty == true)
      }
    }
  }

  test("Gluten-4452: Fix get wrong hash table when multi joins in a task") {
    val testSql =
      """
        | SELECT ws_item_sk, ws_sold_date_sk, ws_ship_date_sk,
        |        t3.d_date_id as sold_date_id, t2.d_date_id as ship_date_id
        | FROM (
        | SELECT ws_item_sk, ws_sold_date_sk, ws_ship_date_sk, t1.d_date_id
        | FROM web_sales
        | LEFT JOIN
        |   (SELECT d_date_id, d_date_sk from date_dim GROUP BY d_date_id, d_date_sk) t1
        | ON ws_sold_date_sk == t1.d_date_sk) t3
        | INNER JOIN
        |   (SELECT d_date_id, d_date_sk from date_dim GROUP BY d_date_id, d_date_sk) t2
        | ON ws_ship_date_sk == t2.d_date_sk
        | LIMIT 100;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      testSql,
      true,
      df => {
        val foundBroadcastHashJoinExpr = df.queryExecution.executedPlan.collect {
          case f: CHBroadcastHashJoinExecTransformer => f
        }
        assert(foundBroadcastHashJoinExpr.size == 2)
      }
    )
  }
}
