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
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import java.sql.Timestamp

class GlutenClickHouseDatetimeExpressionSuite
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
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  test("test date_trunc function") {
    val sql =
      s"""
         |select l_shipdate,
         |       date_trunc(lower('YEAR'), l_shipdate) a,
         |       date_trunc('year', l_shipdate) b,
         |       date_trunc('quarter', l_shipdate) c,
         |       date_trunc('month', l_shipdate) d,
         |       date_trunc('WEEK', l_shipdate) e,
         |       date_trunc('day', l_shipdate) f,
         |       date_trunc('hour', l_shipdate) g,
         |       date_trunc('minute', l_shipdate) h,
         |       date_trunc('second', l_shipdate) i
         |from lineitem
         |order by l_shipdate
         |limit 50
         |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })

    import testImplicits._
    val df = Seq(
      (1, Timestamp.valueOf("2015-07-22 10:01:40.123456")),
      (2, Timestamp.valueOf("2014-12-31 05:29:06.123456")),
      (3, Timestamp.valueOf("2015-07-22 16:01:40.123456")),
      (4, Timestamp.valueOf("2012-02-29 23:01:40.123456"))
    ).toDF("i", "t")

    df.createOrReplaceTempView("date_trunc_tmp")

    val sql1 =
      s"""
         |select t,
         |       date_trunc(lower('YEAR'), t) a,
         |       date_trunc('year', t) a1,
         |       date_trunc('yyyy', t) a2,
         |       date_trunc('yy', t) a3,
         |       date_trunc('quarter', t) b,
         |       date_trunc('month', t) c,
         |       date_trunc('mm', t) c1,
         |       date_trunc('mon', t) c2,
         |       date_trunc('WEEK', t) d,
         |       date_trunc('day', t) e,
         |       date_trunc('dd', t) e2,
         |       date_trunc('hour', t) f,
         |       date_trunc('minute', t) g,
         |       date_trunc('second', t) h
         |from date_trunc_tmp
         |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })

    Seq(
      "date_trunc(case when i = 1 then 'year' else 'month' end, t) a",
      "date_trunc(null, t) a",
      "date_trunc('value', t) a",
      "date_trunc('MILLISECOND', t) a",
      "date_trunc('MICROSECOND', t) a"
    ).foreach(
      expr => {
        val sql2 =
          s"""
             |select t,
             |       $expr
             |from date_trunc_tmp
             |""".stripMargin
        compareResultsAgainstVanillaSpark(sql2, true, { _ => }, false)
      })

    DateTimeTestUtils.withDefaultTimeZone(DateTimeTestUtils.UTC) {
      val inputDate = Timestamp.valueOf("2015-07-22 05:30:06")

      import testImplicits._
      val df = Seq(
        (1, Timestamp.valueOf("2015-07-22 05:30:06")),
        (2, Timestamp.valueOf("2014-12-31 05:29:06.123456"))
      ).toDF("i", "t")

      df.createOrReplaceTempView("date_trunc_tmp1")

      val sql1 =
        s"""
           |select t,
           |       date_trunc('HOUR', t) a,
           |       date_trunc('MINUTE', t) b,
           |       date_trunc('DAY', t) c
           |from date_trunc_tmp1
           |""".stripMargin
      compareResultsAgainstVanillaSpark(sql1, true, { _ => })

      val sql2 =
        s"""
           |select t,
           |       date_trunc('YEAR', t) a,
           |       date_trunc('WEEK', t) b,
           |       date_trunc('month', t) c
           |from date_trunc_tmp1
           |""".stripMargin
      compareResultsAgainstVanillaSpark(sql2, true, { _ => }, false)
    }
  }

  test("test months_between function") {
    val sql =
      s"""
         |select l_shipdate,
         |       months_between('1991-01-01', l_shipdate) a,
         |       months_between('1991-01-01', l_shipdate, False) b,
         |       months_between(l_shipdate, '1991-01-01') c,
         |       months_between(l_shipdate, '1991-01-01', False) d
         |from lineitem
         |order by l_shipdate
         |limit 50
         |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }
}
