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
package io.glutenproject.execution

import io.glutenproject.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

abstract class VeloxAggregateFunctionsSuite extends VeloxWholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
  }

  test("count") {
    val df =
      runQueryAndCompare("select count(*) from lineitem where l_partkey in (1552, 674, 1062)") {
        checkOperatorMatch[HashAggregateExecTransformer]
      }
    runQueryAndCompare("select count(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("avg") {
    val df = runQueryAndCompare("select avg(l_partkey) from lineitem where l_partkey < 1000") {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select avg(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "select avg(cast (l_quantity as DECIMAL(12, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "select avg(cast (l_quantity as DECIMAL(22, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    // Test the situation that precision + 4 of input decimal value exceeds 38.
    runQueryAndCompare(
      "select avg(cast (l_quantity as DECIMAL(36, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("sum") {
    runQueryAndCompare("select sum(l_partkey) from lineitem where l_partkey < 2000") {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select sum(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("select sum(cast (l_quantity as DECIMAL(22, 2))) from lineitem") {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select sum(cast (l_quantity as DECIMAL(12, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "select sum(cast (l_quantity as DECIMAL(22, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }

    // Test the situation that precision + 4 of input decimal value exceeds 38.
    runQueryAndCompare(
      "select sum(cast (l_quantity as DECIMAL(36, 2))), " +
        "count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("min and max") {
    runQueryAndCompare(
      "select min(l_partkey), max(l_partkey) from lineitem where l_partkey < 2000") {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select min(l_partkey), max(l_partkey), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("groupby") {
    val df = runQueryAndCompare(
      "select l_orderkey, sum(l_partkey) as sum from lineitem " +
        "where l_orderkey < 3 group by l_orderkey") { _ => }
    checkLengthAndPlan(df, 2)
  }

  test("group sets") {
    val result = runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
        "where l_orderkey < 3 group by ROLLUP(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey ") { _ => }
  }

  test("stddev_samp") {
    runQueryAndCompare("""
                         |select stddev_samp(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey, stddev_samp(l_quantity) from lineitem
                         |group by l_orderkey;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select stddev_samp(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("stddev_pop") {
    runQueryAndCompare("""
                         |select stddev_pop(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey, stddev_pop(l_quantity) from lineitem
                         |group by l_orderkey;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select stddev_pop(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("var_samp") {
    runQueryAndCompare("""
                         |select var_samp(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey, var_samp(l_quantity) from lineitem
                         |group by l_orderkey;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select var_samp(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("var_pop") {
    runQueryAndCompare("""
                         |select var_pop(l_quantity) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("""
                         |select l_orderkey, var_pop(l_quantity) from lineitem
                         |group by l_orderkey;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select var_pop(l_quantity), count(distinct l_partkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("bit_and bit_or bit_xor") {
    val bitAggs = Seq("bit_and", "bit_or", "bit_xor")
    for (func <- bitAggs) {
      runQueryAndCompare(s"""
                            |select $func(l_linenumber) from lineitem
                            |group by l_orderkey;
                            |""".stripMargin) {
        checkOperatorMatch[HashAggregateExecTransformer]
      }
      runQueryAndCompare(s"select $func(l_linenumber), count(distinct l_partkey) from lineitem") {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 4)
          }
      }
    }
  }

  test("corr covar_pop covar_samp") {
    runQueryAndCompare("""
                         |select corr(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select corr(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("""
                         |select covar_pop(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select covar_pop(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("""
                         |select covar_samp(l_partkey, l_suppkey) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select covar_samp(l_partkey, l_suppkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("first") {
    runQueryAndCompare(s"""
                          |select first(l_linenumber), first(l_linenumber, true) from lineitem;
                          |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      s"""
         |select first_value(l_linenumber), first_value(l_linenumber, true) from lineitem
         |group by l_orderkey;
         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      s"""
         |select first(l_linenumber), first(l_linenumber, true), count(distinct l_partkey)
         |from lineitem
         |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("last") {
    runQueryAndCompare(s"""
                          |select last(l_linenumber), last(l_linenumber, true) from lineitem;
                          |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(s"""
                          |select last_value(l_linenumber), last_value(l_linenumber, true)
                          |from lineitem
                          |group by l_orderkey;
                          |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      s"""
         |select last(l_linenumber), last(l_linenumber, true), count(distinct l_partkey)
         |from lineitem
         |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("approx_count_distinct") {
    runQueryAndCompare("""
                         |select approx_count_distinct(l_shipmode) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(
      "select approx_count_distinct(l_partkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 0)
        }
    }
  }

  test("max_by") {
    runQueryAndCompare(s"""
                          |select max_by(l_linenumber, l_comment) from lineitem;
                          |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(s"""
                          |select max_by(distinct l_linenumber, l_comment)
                          |from lineitem
                          |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("min_by") {
    runQueryAndCompare(s"""
                          |select min_by(l_linenumber, l_comment) from lineitem;
                          |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare(s"""
                          |select min_by(distinct l_linenumber, l_comment)
                          |from lineitem
                          |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("distinct functions") {
    runQueryAndCompare("SELECT sum(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT sum(DISTINCT l_partkey), count(*), sum(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT avg(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT avg(DISTINCT l_partkey), count(*), avg(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT count(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT count(DISTINCT l_partkey), count(*), count(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT stddev_samp(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT stddev_samp(DISTINCT l_partkey), count(*), " +
        "stddev_samp(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT stddev_pop(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT stddev_pop(DISTINCT l_partkey), count(*), " +
        "stddev_pop(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT var_samp(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT var_samp(DISTINCT l_partkey), count(*), " +
        "var_samp(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare("SELECT var_pop(DISTINCT l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT var_pop(DISTINCT l_partkey), count(*), " +
        "var_pop(l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT corr(DISTINCT l_partkey, l_suppkey)," +
        "corr(DISTINCT l_suppkey, l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT corr(DISTINCT l_partkey, l_suppkey)," +
        "count(*), corr(l_suppkey, l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT covar_pop(DISTINCT l_partkey, l_suppkey)," +
        "covar_pop(DISTINCT l_suppkey, l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT covar_pop(DISTINCT l_partkey, l_suppkey)," +
        "count(*), covar_pop(l_suppkey, l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT covar_samp(DISTINCT l_partkey, l_suppkey)," +
        "covar_samp(DISTINCT l_suppkey, l_partkey), count(*) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT covar_samp(DISTINCT l_partkey, l_suppkey)," +
        "count(*), covar_samp(l_suppkey, l_partkey) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
    runQueryAndCompare(
      "SELECT collect_list(DISTINCT n_name), count(*), collect_list(n_name) FROM nation") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("test collect_set") {
    runQueryAndCompare("SELECT array_sort(collect_set(l_partkey)) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 2)
        }
    }

    runQueryAndCompare(
      """
        |SELECT array_sort(collect_set(l_suppkey)), array_sort(collect_set(l_partkey))
        |FROM lineitem
        |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 2)
        }
    }

    runQueryAndCompare(
      "SELECT count(distinct l_suppkey), array_sort(collect_set(l_partkey)) FROM lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("test collect_set/collect_list with null") {
    import testImplicits._

    withTempView("collect_tmp") {
      Seq((1, null), (1, "a"), (2, null), (3, null), (3, null), (4, "b"))
        .toDF("c1", "c2")
        .createOrReplaceTempView("collect_tmp")

      // basic test
      runQueryAndCompare("SELECT collect_set(c2), collect_list(c2) FROM collect_tmp GROUP BY c1") {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 2)
          }
      }

      // test pre project and post project
      runQueryAndCompare("""
                           |SELECT
                           |size(collect_set(if(c2 = 'a', 'x', 'y'))) as x,
                           |size(collect_list(if(c2 = 'a', 'x', 'y'))) as y
                           |FROM collect_tmp GROUP BY c1
                           |""".stripMargin) {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 2)
          }
      }

      // test distinct
      runQueryAndCompare(
        "SELECT collect_set(c2), collect_list(distinct c2) FROM collect_tmp GROUP BY c1") {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 4)
          }
      }

      // test distinct + pre project and post project
      runQueryAndCompare("""
                           |SELECT
                           |size(collect_set(if(c2 = 'a', 'x', 'y'))),
                           |size(collect_list(distinct if(c2 = 'a', 'x', 'y')))
                           |FROM collect_tmp GROUP BY c1
                           |""".stripMargin) {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 4)
          }
      }

      // test cast array to string
      runQueryAndCompare("""
                           |SELECT
                           |cast(collect_set(c2) as string),
                           |cast(collect_list(c2) as string)
                           |FROM collect_tmp GROUP BY c1
                           |""".stripMargin) {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[HashAggregateExecTransformer]
                }) == 2)
          }
      }
    }
  }

  test("count(1)") {
    runQueryAndCompare(
      """
        |select count(1) from (select * from values(1,2) as data(a,b) group by a,b union all
        |select * from values(2,3),(3,4) as data(c,d) group by c,d);
        |""".stripMargin) {
      df =>
        assert(
          getExecutedPlan(df).count(plan => plan.isInstanceOf[HashAggregateExecTransformer]) >= 2)
    }
  }

  test("bind reference failed when subquery in agg expressions") {
    runQueryAndCompare("""
                         |select sum(if(c > (select sum(a) from values (1), (-1) AS tab(a)), 1, -1))
                         |from values (5), (-10), (15) AS tab(c);
                         |""".stripMargin)(
      df => assert(getExecutedPlan(df).count(_.isInstanceOf[HashAggregateExecTransformer]) == 2))

    runQueryAndCompare("""
                         |select sum(if(c > (select sum(a) from values (1), (-1) AS tab(a)), 1, -1))
                         |from values (1L, 5), (1L, -10), (2L, 15) AS tab(sum, c) group by sum;
                         |""".stripMargin)(
      df => assert(getExecutedPlan(df).count(_.isInstanceOf[HashAggregateExecTransformer]) == 2))
  }

  test("collect_list null inputs") {
    runQueryAndCompare("""
                         |select collect_list(a) from values (1), (-1), (null) AS tab(a)
                         |""".stripMargin)(
      df => assert(getExecutedPlan(df).count(_.isInstanceOf[HashAggregateExecTransformer]) == 2))
  }

  test("skewness") {
    runQueryAndCompare("""
                         |select skewness(l_partkey) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select skewness(l_partkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }

  test("kurtosis") {
    runQueryAndCompare("""
                         |select kurtosis(l_partkey) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
    runQueryAndCompare("select kurtosis(l_partkey), count(distinct l_orderkey) from lineitem") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[HashAggregateExecTransformer]
              }) == 4)
        }
    }
  }
}

class VeloxAggregateFunctionsDefaultSuite extends VeloxAggregateFunctionsSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // Disable flush. This may cause spilling to happen on partial aggregations.
      .set(GlutenConfig.VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED.key, "false")
  }

  test("group sets with keys") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      runQueryAndCompare(VeloxAggregateFunctionsSuite.GROUP_SETS_TEST_SQL) {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[RegularHashAggregateExecTransformer]))
          assert(
            !executedPlan.exists(plan => plan.isInstanceOf[FlushableHashAggregateExecTransformer]))
      }
    }
  }
}

class VeloxAggregateFunctionsFlushSuite extends VeloxAggregateFunctionsSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // To test flush behaviors, set low flush threshold to ensure flush happens.
      .set(GlutenConfig.VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED.key, "true")
      .set(GlutenConfig.ABANDON_PARTIAL_AGGREGATION_MIN_PCT.key, "1")
      .set(GlutenConfig.ABANDON_PARTIAL_AGGREGATION_MIN_ROWS.key, "10")
  }

  test("flushable aggregate rule") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1k") {
      runQueryAndCompare("select distinct l_partkey from lineitem") {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[RegularHashAggregateExecTransformer]))
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[FlushableHashAggregateExecTransformer]))
      }
    }
  }

  test("flushable aggregate decimal sum") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1k") {
      runQueryAndCompare("select sum(l_quantity) from lineitem") {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[RegularHashAggregateExecTransformer]))
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[FlushableHashAggregateExecTransformer]))
      }
    }
  }
}

object VeloxAggregateFunctionsSuite {
  val GROUP_SETS_TEST_SQL: String =
    "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
      "where l_orderkey < 3 group by ROLLUP(l_orderkey, l_partkey) " +
      "order by l_orderkey, l_partkey "
}
