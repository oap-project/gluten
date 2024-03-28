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
package org.apache.spark.sql.gluten.parquet

import org.apache.gluten.execution.{FileSourceScanExecTransformer, GlutenClickHouseWholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.gluten.test.{GlutenSQLTestUtils, GlutenTPCHBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal

import java.time.LocalDate

class GlutenParquetFilterSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with GlutenSQLTestUtils
  with GlutenTPCHBase
  with Logging {

  override protected val rootPath = this.getClass.getResource("/").getPath
  override protected val basePath = rootPath + "tests-working-home"
  override protected val warehouse = basePath + "/spark-warehouse"
  override protected val metaStorePathAbsolute = basePath + "/meta"
  override protected val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"

  private val tpchQueriesResourceFolder: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED, false)
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L) // disable broadcast

  private val result: Array[Map[String, Seq[Predicate]]] = Array(
    Map( // q1
      "lineitem0" -> Seq(
        'l_shipdate.date.isNotNull,
        'l_shipdate.date <= LocalDate.of(1998, 9, 1)
      )),
    Map( // q2
      "part0" -> Seq(
        'p_size.int.isNotNull,
        'p_type.string.isNotNull,
        'p_size.int === 15,
        'p_partkey.long.isNotNull
      ),
      "partsupp1" -> Seq(
        'ps_partkey.long.isNotNull,
        'ps_suppkey.long.isNotNull,
        'ps_supplycost.decimal(10, 0).isNotNull
      ),
      "partsupp2" -> Seq(
        'ps_partkey.long.isNotNull,
        'ps_suppkey.long.isNotNull
      ),
      "supplier3" -> Seq(
        's_suppkey.long.isNotNull,
        's_nationkey.long.isNotNull
      ),
      "nation4" -> Seq(
        'n_nationkey.long.isNotNull,
        'n_regionkey.long.isNotNull
      ),
      "region5" -> Seq(
        'r_name.string.isNotNull,
        'r_name.string === "EUROPE",
        'r_regionkey.long.isNotNull
      ),
      "supplier6" -> Seq(
        's_suppkey.long.isNotNull,
        's_nationkey.long.isNotNull
      ),
      "nation7" -> Seq(
        'n_nationkey.long.isNotNull,
        'n_regionkey.long.isNotNull
      )
    ),
    Map( // q3
      "customer0" -> Seq(
        'c_mktsegment.string.isNotNull,
        'c_mktsegment.string === "BUILDING",
        'c_custkey.long.isNotNull
      ),
      "orders1" -> Seq(
        'o_orderkey.long.isNotNull,
        'o_custkey.long.isNotNull,
        'o_orderdate.date.isNotNull,
        'o_orderdate.date < LocalDate.of(1995, 3, 15)
      ),
      "lineitem2" -> Seq(
        'l_orderkey.long.isNotNull,
        'l_shipdate.date.isNotNull,
        'l_shipdate.date > LocalDate.of(1995, 3, 15)
      )
    ),
    Map( // q4
      "orders0" -> Seq(
        'o_orderdate.date.isNotNull,
        'o_orderdate.date >= LocalDate.of(1993, 7, 1),
        'o_orderdate.date < LocalDate.of(1993, 10, 1)
      ),
      "lineitem1" -> Seq(
        'l_commitdate.date.isNotNull,
        'l_receiptdate.date.isNotNull
      )
    ),
    Map( // q5
      "customer0" -> Seq(
        'c_custkey.long.isNotNull,
        'c_nationkey.long.isNotNull
      ),
      "orders1" -> Seq(
        'o_orderkey.long.isNotNull,
        'o_custkey.long.isNotNull,
        'o_orderdate.date.isNotNull,
        'o_orderdate.date >= LocalDate.of(1994, 1, 1),
        'o_orderdate.date < LocalDate.of(1995, 1, 1)
      ),
      "lineitem2" -> Seq(
        'l_orderkey.long.isNotNull,
        'l_suppkey.long.isNotNull
      ),
      "supplier3" -> Seq(
        's_suppkey.long.isNotNull,
        's_nationkey.long.isNotNull
      ),
      "nation4" -> Seq(
        'n_nationkey.long.isNotNull,
        'n_regionkey.long.isNotNull
      ),
      "region5" -> Seq(
        'r_name.string.isNotNull,
        'r_name.string === "ASIA",
        'r_regionkey.long.isNotNull
      )
    ),
    Map( // q6
      "lineitem0" -> Seq(
        'l_shipdate.date.isNotNull,
        'l_discount.decimal(10, 2).isNotNull,
        'l_quantity.decimal(10, 0).isNotNull,
        'l_shipdate.date >= LocalDate.of(1994, 1, 1),
        'l_shipdate.date < LocalDate.of(1995, 1, 1),
        'l_discount.decimal(10, 2) >= Decimal(BigDecimal(0.05), 10, 2),
        'l_discount.decimal(10, 2) <= Decimal(BigDecimal(0.07), 10, 2),
        'l_quantity.decimal(10, 0) < Decimal(24)
      )),
    Map( // q7
      "supplier0" -> Seq(
        's_suppkey.long.isNotNull,
        's_nationkey.long.isNotNull
      ),
      "lineitem1" -> Seq(
        'l_shipdate.date.isNotNull,
        'l_shipdate.date >= LocalDate.of(1995, 1, 1),
        'l_shipdate.date <= LocalDate.of(1996, 12, 31),
        'l_suppkey.long.isNotNull,
        'l_orderkey.long.isNotNull
      ),
      "orders2" -> Seq(
        'o_orderkey.long.isNotNull,
        'o_custkey.long.isNotNull
      ),
      "customer3" -> Seq(
        'c_nationkey.long.isNotNull,
        'c_custkey.long.isNotNull
      ),
      "nation4" -> Seq(
        'n_nationkey.long.isNotNull,
        'n_name.string === "FRANCE" || 'n_name.string === "GERMANY"
      )
    ),
    Map( // q8
      "part0" -> Seq(
        'p_partkey.long.isNotNull,
        'p_type.string.isNotNull,
        'p_type.string === "ECONOMY ANODIZED STEEL"
      ),
      "lineitem1" -> Seq(
        'l_partkey.long.isNotNull,
        'l_suppkey.long.isNotNull,
        'l_orderkey.long.isNotNull
      ),
      "supplier2" -> Seq(
        's_suppkey.long.isNotNull,
        's_nationkey.long.isNotNull
      ),
      "orders3" -> Seq(
        'o_orderkey.long.isNotNull,
        'o_custkey.long.isNotNull,
        'o_orderdate.date.isNotNull,
        'o_orderdate.date >= LocalDate.of(1995, 1, 1),
        'o_orderdate.date <= LocalDate.of(1996, 12, 31)
      ),
      "customer4" -> Seq(
        'c_custkey.long.isNotNull,
        'c_nationkey.long.isNotNull
      ),
      "nation5" -> Seq(
        'n_nationkey.long.isNotNull,
        'n_regionkey.long.isNotNull
      ),
      "nation6" -> Seq(
        'n_nationkey.long.isNotNull
      ),
      "region7" -> Seq(
        'r_regionkey.long.isNotNull,
        'r_name.string.isNotNull,
        'r_name.string === "AMERICA"
      )
    ),
    Map( // q9
      "part0" -> Seq(
        'p_partkey.long.isNotNull,
        'p_name.string.isNotNull
      ),
      "lineitem1" -> Seq(
        'l_partkey.long.isNotNull,
        'l_suppkey.long.isNotNull,
        'l_orderkey.long.isNotNull
      ),
      "supplier2" -> Seq(
        's_suppkey.long.isNotNull,
        's_nationkey.long.isNotNull
      ),
      "partsupp3" -> Seq(
        'ps_partkey.long.isNotNull,
        'ps_suppkey.long.isNotNull
      ),
      "orders4" -> Seq(
        'o_orderkey.long.isNotNull
      ),
      "nation5" -> Seq(
        'n_nationkey.long.isNotNull
      )
    ),
    Map( // q10
      "customer0" -> Seq(
        'c_custkey.long.isNotNull,
        'c_nationkey.long.isNotNull
      ),
      "orders1" -> Seq(
        'o_orderkey.long.isNotNull,
        'o_custkey.long.isNotNull,
        'o_orderdate.date.isNotNull,
        'o_orderdate.date >= LocalDate.of(1993, 10, 1),
        'o_orderdate.date < LocalDate.of(1994, 1, 1)
      ),
      "lineitem2" -> Seq(
        'l_orderkey.long.isNotNull,
        'l_returnflag.string.isNotNull,
        'l_returnflag.string === "R"
      ),
      "nation3" -> Seq(
        'n_nationkey.long.isNotNull
      )
    ),
    Map( // q11
      "partsupp0" -> Seq(
        'ps_suppkey.long.isNotNull
      ),
      "supplier1" -> Seq(
        's_suppkey.long.isNotNull,
        's_nationkey.long.isNotNull
      ),
      "nation2" -> Seq(
        'n_nationkey.long.isNotNull,
        'n_name.string.isNotNull,
        'n_name.string === "GERMANY"
      )
    ),
    Map( // q12
      "orders0" -> Seq(
        'o_orderkey.long.isNotNull
      ),
      "lineitem1" -> Seq(
        'l_orderkey.long.isNotNull,
        'l_receiptdate.date.isNotNull,
        'l_receiptdate.date >= LocalDate.of(1994, 1, 1),
        'l_receiptdate.date < LocalDate.of(1995, 1, 1),
        'l_commitdate.date.isNotNull,
        'l_shipdate.date.isNotNull,
        'l_shipmode.string.in("MAIL", "SHIP").asInstanceOf[Predicate]
      )
    ),
    Map( // q13
      "customer0" -> Nil,
      "orders1" -> Seq(
        'o_custkey.long.isNotNull,
        'o_comment.string.isNotNull
      )),
    Map( // q14
      "lineitem0" -> Seq(
        'l_partkey.long.isNotNull,
        'l_shipdate.date.isNotNull,
        'l_shipdate.date >= LocalDate.of(1995, 9, 1),
        'l_shipdate.date < LocalDate.of(1995, 10, 1)
      ),
      "part1" -> Seq(
        'p_partkey.long.isNotNull
      )
    ),
    Map( // q15
      "supplier0" -> Seq(
        's_suppkey.long.isNotNull
      ),
      "lineitem1" -> Seq(
        'l_suppkey.long.isNotNull,
        'l_shipdate.date.isNotNull,
        'l_shipdate.date >= LocalDate.of(1996, 1, 1),
        'l_shipdate.date < LocalDate.of(1996, 4, 1)
      )
    ),
    Map( // q16
      "partsupp0" -> Seq(
        'ps_partkey.long.isNotNull
      ),
      "supplier1" -> Seq(
        's_comment.string.isNotNull
      ),
      "part2" -> Seq(
        'p_partkey.long.isNotNull,
        'p_brand.string.isNotNull,
        'p_brand.string =!= "Brand#45",
        'p_type.string.isNotNull,
        'p_size.int.in(49, 14, 23, 45, 19, 3, 36, 9).asInstanceOf[Predicate]
      )
    ),
    Map( // q17
      "lineitem0" -> Seq(
        'l_partkey.long.isNotNull,
        'l_quantity.decimal(10, 0).isNotNull
      ),
      "part1" -> Seq(
        'p_partkey.long.isNotNull,
        'p_brand.string.isNotNull,
        'p_brand.string === "Brand#23",
        'p_container.string.isNotNull,
        'p_container.string === "MED BOX"
      ),
      "lineitem2" -> Seq(
        'l_partkey.long.isNotNull
      )
    ),
    Map( // q18
      "customer0" -> Seq(
        'c_custkey.long.isNotNull
      ),
      "orders1" -> Seq(
        'o_orderkey.long.isNotNull,
        'o_custkey.long.isNotNull
      ),
      "lineitem2" -> Nil,
      "lineitem3" -> Seq(
        'l_orderkey.long.isNotNull
      )
    ),
    Map( // q19
      "lineitem0" -> Seq(
        'l_shipinstruct.string.isNotNull,
        'l_shipmode.string.in("AIR", "AIR REG").asInstanceOf[Predicate],
        'l_shipinstruct.string === "DELIVER IN PERSON",
        'l_partkey.long.isNotNull,
        ('l_quantity.decimal(10, 0) >= Decimal(1) &&
          'l_quantity.decimal(10, 0) <= Decimal(11)) ||
          ('l_quantity.decimal(10, 0) >= Decimal(10) &&
            'l_quantity.decimal(10, 0) <= Decimal(20)) ||
          ('l_quantity.decimal(10, 0) >= Decimal(20) &&
            'l_quantity.decimal(10, 0) <= Decimal(30))
      ),
      "part1" -> Seq(
        'p_size.int.isNotNull,
        'p_size.int >= 1,
        'p_partkey.long.isNotNull,
        ('p_brand.string === "Brand#12" &&
          ('p_container.string in ("SM CASE", "SM BOX", "SM PACK", "SM PKG")) &&
          'p_size.int <= 5) ||
          ('p_brand.string === "Brand#23" &&
            ('p_container.string in ("MED BAG", "MED BOX", "MED PKG", "MED PACK")) &&
            'p_size.int <= 10) ||
          ('p_brand.string === "Brand#34" &&
            ('p_container.string in ("LG CASE", "LG BOX", "LG PACK", "LG PKG")) &&
            'p_size.int <= 15)
      )
    ),
    Map( // q20
      "supplier0" -> Seq(
        's_nationkey.long.isNotNull
      ),
      "partsupp1" -> Seq(
        'ps_suppkey.long.isNotNull,
        'ps_partkey.long.isNotNull,
        'ps_availqty.int.isNotNull
      ),
      "part2" -> Seq(
        'p_name.string.isNotNull
      ),
      "lineitem3" -> Seq(
        'l_partkey.long.isNotNull,
        'l_suppkey.long.isNotNull,
        'l_shipdate.date.isNotNull,
        'l_shipdate.date >= LocalDate.of(1994, 1, 1),
        'l_shipdate.date < LocalDate.of(1995, 1, 1)
      ),
      "nation4" -> Seq(
        'n_nationkey.long.isNotNull,
        'n_name.string.isNotNull,
        'n_name.string === "CANADA"
      )
    ),
    Map( // q21
      "supplier0" -> Seq(
        's_nationkey.long.isNotNull,
        's_suppkey.long.isNotNull
      ),
      "lineitem1" -> Seq(
        'l_orderkey.long.isNotNull,
        'l_suppkey.long.isNotNull,
        'l_commitdate.date.isNotNull,
        'l_receiptdate.date.isNotNull
      ),
      "lineitem2" -> Nil,
      "lineitem3" -> Seq(
        'l_receiptdate.date.isNotNull,
        'l_commitdate.date.isNotNull
      ),
      "orders4" -> Seq(
        'o_orderstatus.string.isNotNull,
        'o_orderstatus.string === "F",
        'o_orderkey.long.isNotNull
      ),
      "nation5" -> Seq(
        'n_nationkey.long.isNotNull,
        'n_name.string.isNotNull,
        'n_name.string === "SAUDI ARABIA"
      )
    ),
    Map( // q22
      "customer0" -> Seq(
        'c_acctbal.decimal(10, 0).isNotNull
      ),
      "orders1" -> Nil)
  )

  tpchQueries.zipWithIndex.foreach {
    case (q, i) =>
      test(q) {
        withDataFrame(tpchSQL(i + 1, tpchQueriesResourceFolder)) {
          df =>
            val scans = df.queryExecution.executedPlan
              .collect { case scan: FileSourceScanExecTransformer => scan }
            assertResult(result(i).size)(scans.size)
            scans.zipWithIndex
              .foreach {
                case (scan, fileIndex) =>
                  val tableName = scan.tableIdentifier
                    .map(_.table)
                    .getOrElse(scan.relation.options("path").split("/").last)
                  val predicates = scan.filterExprs()
                  val expected = result(i)(s"$tableName$fileIndex")
                  assertResult(expected.size)(predicates.size)
                  if (expected.isEmpty) assert(predicates.isEmpty)
                  else compareExpressions(expected.reduceLeft(And), predicates.reduceLeft(And))
              }
        }
      }
  }
}
