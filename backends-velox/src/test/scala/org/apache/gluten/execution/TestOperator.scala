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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.{FilterExec, GenerateExec, ProjectExec, RDDScanExec}
import org.apache.spark.sql.functions.{avg, col, lit, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

import scala.collection.JavaConverters

class TestOperator extends VeloxWholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

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
      .set("spark.sql.sources.useV1SourceList", "avro,parquet")
  }

  test("simple_select") {
    val df = runQueryAndCompare("select * from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("select_part_column") {
    val df = runQueryAndCompare("select l_shipdate, l_orderkey from lineitem limit 1") {
      df => { assert(df.schema.fields.length == 2) }
    }
    checkLengthAndPlan(df, 1)
  }

  test("select_as") {
    val df = runQueryAndCompare("select l_shipdate as my_col from lineitem limit 1") {
      df => { assert(df.schema.fieldNames(0).equals("my_col")) }
    }
    checkLengthAndPlan(df, 1)
  }

  test("where") {
    val df = runQueryAndCompare("select * from lineitem where l_shipdate < '1998-09-02'") { _ => }
    checkLengthAndPlan(df, 59288)
  }

  test("is_null") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_comment is null") { _ => }
    assert(df.isEmpty)
    checkLengthAndPlan(df, 0)
  }

  test("is_null_has_null") {
    val data = Seq(Row(null), Row("data"), Row(null))
    val schema = StructType(Array(StructField("col1", StringType, nullable = true)))
    spark
      .createDataFrame(JavaConverters.seqAsJavaList(data), schema)
      .createOrReplaceTempView("temp_test_is_null")
    val df = runQueryAndCompare("select * from temp_test_is_null where col1 is null") { _ => }
    checkLengthAndPlan(df, 2)
  }

  test("is_not_null") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem where l_comment is not null " +
        "and l_orderkey = 1") { _ => }
    checkLengthAndPlan(df, 6)
  }

  test("and pushdown") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem where l_orderkey > 2 " +
        "and l_orderkey = 1") { _ => }
    assert(df.isEmpty)
    checkLengthAndPlan(df, 0)
  }

  test("in") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674, 1062)") { _ => }
    checkLengthAndPlan(df, 122)
  }

  test("in_and") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674, 1062) and l_partkey in (1552, 674)") { _ => }
    checkLengthAndPlan(df, 73)
  }

  test("in_or") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    checkLengthAndPlan(df, 122)
  }

  test("in_or_and") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552) and l_orderkey > 1") { _ => }
    checkLengthAndPlan(df, 73)
  }

  test("in_not") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    checkLengthAndPlan(df, 60141)
  }

  test("coalesce") {
    var df = runQueryAndCompare(
      "select l_orderkey, coalesce(l_comment, 'default_val') " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, l_comment, 'default_val') " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, null, l_comment) " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, null, 1, 2) " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, null, null) " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
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

  test("orderby") {
    val df = runQueryAndCompare(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey") { _ => }
    checkLengthAndPlan(df, 7)
  }

  test("orderby expression") {
    val df = runQueryAndCompare(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey / 2 ") { _ => }
    checkLengthAndPlan(df, 7)
  }

  test("window expression") {
    Seq("sort", "streaming").foreach {
      windowType =>
        withSQLConf("spark.gluten.sql.columnar.backend.velox.window.type" -> windowType) {
          runQueryAndCompare(
            "select ntile(4) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          runQueryAndCompare(
            "select row_number() over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          runQueryAndCompare(
            "select rank() over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          runQueryAndCompare(
            "select dense_rank() over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

          runQueryAndCompare(
            "select percent_rank() over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

          runQueryAndCompare(
            "select cume_dist() over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

          runQueryAndCompare(
            "select l_suppkey, l_orderkey, nth_value(l_orderkey, 2) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          runQueryAndCompare(
            "select l_suppkey, l_orderkey, nth_value(l_orderkey, 2) IGNORE NULLS over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          runQueryAndCompare(
            "select sum(l_partkey + 1) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem") {
            checkOperatorMatch[WindowExecTransformer]
          }

          runQueryAndCompare(
            "select max(l_partkey) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          runQueryAndCompare(
            "select min(l_partkey) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          runQueryAndCompare(
            "select avg(l_partkey) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          runQueryAndCompare(
            "select lag(l_orderkey) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          runQueryAndCompare(
            "select lead(l_orderkey) over" +
              " (partition by l_suppkey order by l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          // Test same partition/ordering keys.
          runQueryAndCompare(
            "select avg(l_partkey) over" +
              " (partition by l_suppkey order by l_suppkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }

          // Test overlapping partition/ordering keys.
          runQueryAndCompare(
            "select avg(l_partkey) over" +
              " (partition by l_suppkey order by l_suppkey, l_orderkey) from lineitem ") {
            checkOperatorMatch[WindowExecTransformer]
          }
        }
    }
  }

  test("df.count()") {
    val df = runQueryAndCompare("select * from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("union_all two tables") {
    runQueryAndCompare("""
                         |select count(orderkey) from (
                         | select l_orderkey as orderkey from lineitem
                         | union all
                         | select o_orderkey as orderkey from orders
                         |);
                         |""".stripMargin) {
      df =>
        {
          getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[ColumnarUnionExec]).isDefined)
        }
    }
  }

  test("union_all three tables") {
    runQueryAndCompare("""
                         |select count(orderkey) from (
                         | select l_orderkey as orderkey from lineitem
                         | union all
                         | select o_orderkey as orderkey from orders
                         | union all
                         | (select o_orderkey as orderkey from orders limit 100)
                         |);
                         |""".stripMargin) {
      df =>
        {
          getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[ColumnarUnionExec]).isDefined)
        }
    }
  }

  test("union two tables") {
    val df = runQueryAndCompare("""
                                  |select count(orderkey) from (
                                  | select l_orderkey as orderkey from lineitem
                                  | union
                                  | select o_orderkey as orderkey from orders
                                  |);
                                  |""".stripMargin) {
      df =>
        {
          getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[ColumnarUnionExec]).isDefined)
        }
    }
  }

  test("test 'select global/local limit'") {
    runQueryAndCompare("""
                         |select * from (
                         | select * from lineitem limit 10
                         |) where l_suppkey != 0 limit 100;
                         |""".stripMargin) {
      checkOperatorMatch[LimitTransformer]
    }
  }

  test("bool scan") {
    withTempPath {
      path =>
        Seq(true, false, true, true, false, false)
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("SELECT a from view") {
          checkOperatorMatch[FileSourceScanExecTransformer]
        }
    }
  }

  test("decimal abs") {
    runQueryAndCompare("""
                         |select abs(cast (l_quantity * (-1.0) as decimal(12, 2))),
                         |abs(cast (l_quantity * (-1.0) as decimal(22, 2))),
                         |abs(cast (l_quantity as decimal(12, 2))),
                         |abs(cast (l_quantity as decimal(12, 2))) from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[ProjectExecTransformer]
    }
    withTempPath {
      path =>
        Seq(-3099.270000, -3018.367500, -2833.887500, -1304.180000, -1263.289167, -1480.093333)
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("SELECT abs(cast (a as decimal(19, 6))) from view") {
          checkOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("nested decimal arithmetics") {
    runQueryAndCompare("""
                         |SELECT
                         |  l_orderkey,
                         |  SUM(
                         |    (l_extendedprice * (1 - l_discount)) +
                         |    (l_extendedprice * (1 - l_discount) * 0.05)
                         |  ) AS total_revenue_with_tax
                         |FROM
                         |  lineitem
                         |GROUP BY
                         |  l_orderkey
                         |ORDER BY
                         |  l_orderkey
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
  }

  test("Cast double to decimal") {
    val d = 0.034567890
    val df = Seq(d, d, d, d, d, d, d, d, d, d).toDF("DecimalCol")
    val result = df
      .select($"DecimalCol".cast(DecimalType(38, 33)))
      .select(col("DecimalCol"))
      .agg(avg($"DecimalCol"))
    // Double precision loss:
    // https://github.com/facebookincubator/velox/pull/6051#issuecomment-1731028215.
    // assert(result.collect()(0).get(0).toString.equals("0.0345678900000000000000000000000000000"))
    assert((result.collect()(0).get(0).toString.toDouble - d).abs < 0.00000000001)
    checkOperatorMatch[HashAggregateExecTransformer](result)
  }

  test("orc scan") {
    val df = spark.read
      .format("orc")
      .load("../cpp/velox/benchmarks/data/bm_lineitem/orc/lineitem.orc")
    df.createOrReplaceTempView("lineitem_orc")
    runQueryAndCompare("select l_orderkey from lineitem_orc") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[BatchScanExecTransformer]
              }) == 1)
        }
    }
  }

  test("test OneRowRelation") {
    val df = sql("SELECT 1")
    checkAnswer(df, Row(1))
    val plan = df.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[RDDScanExec]).isDefined)
    assert(plan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
    assert(plan.find(_.isInstanceOf[RowToVeloxColumnarExec]).isDefined)
  }

  test("equal null safe") {
    runQueryAndCompare("""
                         |select l_quantity <=> 1000 from lineitem;
                         |""".stripMargin) {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("test overlay function") {
    runQueryAndCompare("""
                         |select overlay(l_shipdate placing '_' from 0) from lineitem limit 1;
                         |""".stripMargin) {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Improve the local sort ensure requirements") {
    withSQLConf(
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.gluten.sql.columnar.forceShuffledHashJoin" -> "false") {
      withTable("t1", "t2") {
        sql("""
              |create table t1 using parquet as
              |select cast(id as int) as c1, cast(id as string) c2 from range(100)
              |""".stripMargin)
        sql("""
              |create table t2 using parquet as
              |select cast(id as int) as c1, cast(id as string) c2 from range(100) order by c1 desc;
              |""".stripMargin)

        runQueryAndCompare(
          """
            |select * from (select c1, max(c2) from t1 group by c1)t1
            |join t2 on t1.c1 = t2.c1 and t1.c1 > conv(t2.c1, 2, 10);
            |""".stripMargin
        ) {
          checkOperatorMatch[HashAggregateExecTransformer]
        }
      }
    }
  }

  test("Fix Generate fail when required child output is not same with child output") {
    withTable("t") {
      spark
        .range(10)
        .selectExpr("id as c1", "id as c2")
        .write
        .format("parquet")
        .saveAsTable("t")

      runQueryAndCompare("SELECT c1, explode(array(c2)) FROM t") {
        checkOperatorMatch[GenerateExecTransformer]
      }

      runQueryAndCompare("SELECT c1, explode(c3) FROM (SELECT c1, array(c2) as c3 FROM t)") {
        checkOperatorMatch[GenerateExecTransformer]
      }
    }
  }

  test("Validation should fail if unsupported expression is used for Generate.") {
    withTable("t") {
      spark
        .range(10)
        .selectExpr("id as c1", "id as c2")
        .write
        .format("parquet")
        .saveAsTable("t")

      // Add a simple UDF to generate the unsupported case
      val intToArrayFunc = udf((s: Int) => Array(s))
      spark.udf.register("intToArray", intToArrayFunc)

      // Testing unsupported case
      runQueryAndCompare("SELECT explode(intToArray(c1)) from t;") {
        df =>
          {
            getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[GenerateExec]).isDefined)
          }
      }

      // Testing unsupported case in case when
      runQueryAndCompare("""
                           |SELECT explode(case when size(intToArray(c1)) > 0
                           |then array(c1) else array(c2) end) from t;
                           |""".stripMargin) {
        df =>
          {
            getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[GenerateExec]).isDefined)
          }
      }
    }
  }

  test("Support get native plan tree string, Velox single aggregation") {
    runQueryAndCompare("""
                         |select l_partkey + 1, count(*)
                         |from (select /*+ repartition(2) */ * from lineitem) group by l_partkey + 1
                         |""".stripMargin) {
      df =>
        val wholeStageTransformers = collect(df.queryExecution.executedPlan) {
          case w: WholeStageTransformer => w
        }
        assert(wholeStageTransformers.size == 3)
        val nativePlanString = wholeStageTransformers.head.nativePlanString()
        assert(nativePlanString.contains("Aggregation[SINGLE"))
        assert(nativePlanString.contains("ValueStream"))
        assert(wholeStageTransformers(1).nativePlanString().contains("ValueStream"))
        assert(wholeStageTransformers.last.nativePlanString().contains("TableScan"))
    }
  }

  test("Support StreamingAggregate if child output ordering is satisfied") {
    withTable("t") {
      spark
        .range(10000)
        .selectExpr(s"id % 999 as c1", "id as c2")
        .write
        .saveAsTable("t")

      withSQLConf(
        GlutenConfig.COLUMNAR_PREFER_STREAMING_AGGREGATE.key -> "true",
        GlutenConfig.COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"
      ) {
        val query =
          """
            |SELECT c1, count(*), sum(c2) FROM (
            |SELECT t1.c1, t2.c2 FROM t t1 JOIN t t2 ON t1.c1 = t2.c1
            |)
            |GROUP BY c1
            |""".stripMargin
        runQueryAndCompare(query) {
          df =>
            assert(
              find(df.queryExecution.executedPlan)(
                _.isInstanceOf[SortMergeJoinExecTransformer]).isDefined)
            assert(
              find(df.queryExecution.executedPlan)(
                _.isInstanceOf[HashAggregateExecTransformer]).isDefined)
        }
      }
    }
  }

  test("Verify parquet field name with special character") {
    withTable("t") {

      // https://github.com/apache/spark/pull/35229 Spark remove parquet field name check after 3.2
      if (!SparkShimLoader.getSparkVersion.startsWith("3.2")) {
        sql("create table t using parquet as select sum(l_partkey) from lineitem")
        runQueryAndCompare("select * from t") {
          checkOperatorMatch[FileSourceScanExecTransformer]
        }
      } else {
        val msg = intercept[AnalysisException] {
          sql("create table t using parquet as select sum(l_partkey) from lineitem")
        }.message
        assert(msg.contains("contains invalid character"))
      }
    }
  }

  test("test explode/posexplode function") {
    Seq("explode", "posexplode").foreach {
      func =>
        // Literal: func(literal)
        runQueryAndCompare(s"""
                              |SELECT $func(array(1, 2, 3));
                              |""".stripMargin) {
          checkOperatorMatch[GenerateExecTransformer]
        }
        runQueryAndCompare(s"""
                              |SELECT $func(map(1, 'a', 2, 'b'));
                              |""".stripMargin) {
          checkOperatorMatch[GenerateExecTransformer]
        }
        runQueryAndCompare(
          s"""
             |SELECT $func(array(map(1, 'a', 2, 'b'), map(3, 'c', 4, 'd'), map(5, 'e', 6, 'f')));
             |""".stripMargin) {
          checkOperatorMatch[GenerateExecTransformer]
        }
        runQueryAndCompare(s"""
                              |SELECT $func(map(1, array(1, 2), 2, array(3, 4)));
                              |""".stripMargin) {
          checkOperatorMatch[GenerateExecTransformer]
        }

        // CreateArray/CreateMap: func(array(col)), func(map(k, v))
        withTempView("t1") {
          sql("""select * from values (1), (2), (3), (4)
                |as tbl(a)
         """.stripMargin).createOrReplaceTempView("t1")
          runQueryAndCompare(s"""
                                |SELECT $func(array(a)) from t1;
                                |""".stripMargin) {
            checkOperatorMatch[GenerateExecTransformer]
          }
          sql("""select * from values (1, 'a'), (2, 'b'), (3, null), (4, null)
                |as tbl(a, b)
         """.stripMargin).createOrReplaceTempView("t1")
          runQueryAndCompare(s"""
                                |SELECT $func(map(a, b)) from t1;
                                |""".stripMargin) {
            checkOperatorMatch[GenerateExecTransformer]
          }
        }

        // AttributeReference: func(col)
        withTempView("t2") {
          sql("""select * from values
                |  array(1, 2, 3),
                |  array(4, null)
                |as tbl(a)
         """.stripMargin).createOrReplaceTempView("t2")
          runQueryAndCompare(s"""
                                |SELECT $func(a) from t2;
                                |""".stripMargin) {
            checkOperatorMatch[GenerateExecTransformer]
          }
          sql("""select * from values
                |  map(1, 'a', 2, 'b', 3, null),
                |  map(4, null)
                |as tbl(a)
         """.stripMargin).createOrReplaceTempView("t2")
          runQueryAndCompare(s"""
                                |SELECT $func(a) from t2;
                                |""".stripMargin) {
            checkOperatorMatch[GenerateExecTransformer]
          }
        }
    }
  }

  test("test inline function") {
    // Literal: func(literal)
    runQueryAndCompare(s"""
                          |SELECT inline(array(
                          |  named_struct('c1', 0, 'c2', 1),
                          |  named_struct('c1', 2, 'c2', null)));
                          |""".stripMargin) {
      checkOperatorMatch[GenerateExecTransformer]
    }

    // CreateArray: func(array(col))
    withTempView("t1") {
      sql("""SELECT * from values
            |  (named_struct('c1', 0, 'c2', 1)),
            |  (named_struct('c1', 2, 'c2', null)),
            |  (null)
            |as tbl(a)
         """.stripMargin).createOrReplaceTempView("t1")
      runQueryAndCompare(s"""
                            |SELECT inline(array(a)) from t1;
                            |""".stripMargin) {
        checkOperatorMatch[GenerateExecTransformer]
      }
    }

    withTempView("t2") {
      sql("""SELECT * from values
            |  array(
            |    named_struct('c1', 0, 'c2', 1),
            |    null,
            |    named_struct('c1', 2, 'c2', 3)
            |  ),
            |  array(
            |    null,
            |    named_struct('c1', 0, 'c2', 1),
            |    named_struct('c1', 2, 'c2', 3)
            |  )
            |as tbl(a)
         """.stripMargin).createOrReplaceTempView("t2")
      runQueryAndCompare("""
                           |SELECT inline(a) from t2;
                           |""".stripMargin) {
        checkOperatorMatch[GenerateExecTransformer]
      }
    }

    // Fallback for array(struct(...), null) literal.
    runQueryAndCompare(s"""
                          |SELECT inline(array(
                          |  named_struct('c1', 0, 'c2', 1),
                          |  named_struct('c1', 2, 'c2', null),
                          |  null));
                          |""".stripMargin)(_)
  }

  test("test array functions") {
    withTable("t") {
      sql("CREATE TABLE t (c1 ARRAY<INT>, c2 ARRAY<INT>, c3 STRING) using parquet")
      sql("INSERT INTO t VALUES (ARRAY(0, 1, 2, 3, 3), ARRAY(2, 2, 3, 4, 6), 'abc')")
      runQueryAndCompare("""
                           |SELECT array_except(c1, c2) FROM t;
                           |""".stripMargin) {
        checkOperatorMatch[ProjectExecTransformer]
      }
      runQueryAndCompare("""
                           |SELECT array_distinct(c1), array_distinct(c2) FROM t;
                           |""".stripMargin) {
        checkOperatorMatch[ProjectExecTransformer]
      }
      runQueryAndCompare("""
                           |SELECT array_position(c1, 3), array_position(c2, 2) FROM t;
                           |""".stripMargin) {
        checkOperatorMatch[ProjectExecTransformer]
      }
      runQueryAndCompare("""
                           |SELECT array_repeat(c3, 5) FROM t;
                           |""".stripMargin) {
        checkOperatorMatch[ProjectExecTransformer]
      }
    }
  }

  test("Support bool type filter in scan") {
    withTable("t") {
      sql("create table t (id int, b boolean) using parquet")
      sql("insert into t values (1, true), (2, false), (3, null)")
      runQueryAndCompare("select * from t where b = true") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }
      runQueryAndCompare("select * from t where b = false") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }
      runQueryAndCompare("select * from t where b is NULL") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }
    }
  }

  test("Support short int type filter in scan") {
    withTable("short_table") {
      sql("create table short_table (a short, b int) using parquet")
      sql(
        s"insert into short_table values " +
          s"(1, 1), (null, 2), (${Short.MinValue}, 3), (${Short.MaxValue}, 4)")
      runQueryAndCompare("select * from short_table where a = 1") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }

      runQueryAndCompare("select * from short_table where a is NULL") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }

      runQueryAndCompare(s"select * from short_table where a != ${Short.MinValue}") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }

      runQueryAndCompare(s"select * from short_table where a != ${Short.MaxValue}") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }
    }
  }

  test("Support int type filter in scan") {
    withTable("int_table") {
      sql("create table int_table (a int, b int) using parquet")
      sql(
        s"insert into int_table values " +
          s"(1, 1), (null, 2), (${Int.MinValue}, 3), (${Int.MaxValue}, 4)")
      runQueryAndCompare("select * from int_table where a = 1") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }

      runQueryAndCompare("select * from int_table where a is NULL") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }

      runQueryAndCompare(s"select * from int_table where a != ${Int.MinValue}") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }

      runQueryAndCompare(s"select * from int_table where a != ${Int.MaxValue}") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }
    }
  }

  test("Fallback on timestamp column filter") {
    withTable("ts") {
      sql("create table ts (c1 int, c2 timestamp) using parquet")
      sql("insert into ts values (1, timestamp'2016-01-01 10:11:12.123456')")
      sql("insert into ts values (2, null)")
      sql("insert into ts values (3, timestamp'1965-01-01 10:11:12.123456')")

      runQueryAndCompare("select c1, c2 from ts where c1 = 1") {
        checkOperatorMatch[FileSourceScanExecTransformer]
      }

      // Fallback should only happen when there is a filter on timestamp column
      runQueryAndCompare(
        "select c1, c2 from ts where" +
          " c2 = timestamp'1965-01-01 10:11:12.123456'") { _ => }

      runQueryAndCompare(
        "select c1, c2 from ts where" +
          " c1 = 1 and c2 = timestamp'1965-01-01 10:11:12.123456'") { _ => }
    }
  }

  test("test cross join") {
    withTable("t1", "t2") {
      sql("""
            |create table t1 using parquet as
            |select cast(id as int) as c1, cast(id as string) c2 from range(100)
            |""".stripMargin)
      sql("""
            |create table t2 using parquet as
            |select cast(id as int) as c1, cast(id as string) c2 from range(100) order by c1 desc;
            |""".stripMargin)

      runQueryAndCompare(
        """
          |select * from t1 cross join t2 on t1.c1 = t2.c1;
          |""".stripMargin
      ) {
        checkOperatorMatch[ShuffledHashJoinExecTransformer]
      }

      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "1MB") {
        runQueryAndCompare(
          """
            |select * from t1 cross join t2 on t1.c1 = t2.c1;
            |""".stripMargin
        ) {
          checkOperatorMatch[BroadcastHashJoinExecTransformer]
        }
      }

      withSQLConf("spark.gluten.sql.columnar.forceShuffledHashJoin" -> "false") {
        runQueryAndCompare(
          """
            |select * from t1 cross join t2 on t1.c1 = t2.c1;
            |""".stripMargin
        ) {
          checkOperatorMatch[SortMergeJoinExecTransformer]
        }
      }

      runQueryAndCompare(
        """
          |select * from t1 cross join t2;
          |""".stripMargin
      ) {
        checkOperatorMatch[CartesianProductExecTransformer]
      }

      runQueryAndCompare(
        """
          |select * from t1 cross join t2 on t1.c1 > t2.c1;
          |""".stripMargin
      ) {
        checkOperatorMatch[CartesianProductExecTransformer]
      }

      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "1MB") {
        runQueryAndCompare(
          """
            |select * from t1 cross join t2 on 2*t1.c1 > 3*t2.c1;
            |""".stripMargin
        ) {
          checkOperatorMatch[BroadcastNestedLoopJoinExecTransformer]
        }
      }
    }
  }

  test("Fix incorrect path by decode") {
    val c = "?.+<_>|/"
    val path = rootPath + "/test +?.+<_>|"
    val key1 = s"${c}key1 $c$c"
    val key2 = s"${c}key2 $c$c"
    val valueA = s"${c}some$c${c}value${c}A"
    val valueB = s"${c}some$c${c}value${c}B"
    val valueC = s"${c}some$c${c}value${c}C"
    val valueD = s"${c}some$c${c}value${c}D"

    val df1 = spark.range(3).withColumn(key1, lit(valueA)).withColumn(key2, lit(valueB))
    val df2 = spark.range(4, 7).withColumn(key1, lit(valueC)).withColumn(key2, lit(valueD))
    val df = df1.union(df2)
    df.write.partitionBy(key1, key2).format("parquet").mode("overwrite").save(path)

    spark.read.format("parquet").load(path).createOrReplaceTempView("test")
    runQueryAndCompare("select * from test") {
      checkOperatorMatch[FileSourceScanExecTransformer]
    }
  }

  test("timestamp cast fallback") {
    withTempPath {
      path =>
        (0 to 3).toDF("x").write.parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare(
          "SELECT x FROM view WHERE cast(x as timestamp) " +
            "IN ('1970-01-01 08:00:00.001','1970-01-01 08:00:00.2')")(_)
    }
  }

  test("Columnar cartesian product with other join") {
    withTable("cartesian1", "cartesian2") {
      spark.sql("""
                  |CREATE TABLE cartesian1 USING PARQUET
                  |AS SELECT id as c1, id % 3 as c2 FROM range(20)
                  |""".stripMargin)
      spark.sql("""
                  |CREATE TABLE cartesian2 USING PARQUET
                  |AS SELECT id as c1, id % 3 as c2 FROM range(20)
                  |""".stripMargin)

      runQueryAndCompare(
        """
          |SELECT * FROM (
          | SELECT /*+ shuffle_replicate_nl(cartesian1) */ cartesian1.c1, cartesian2.c2
          | FROM cartesian1 join cartesian2
          |)tmp
          |join cartesian2 on tmp.c1 = cartesian2.c1
          |""".stripMargin)(df => checkFallbackOperators(df, 0))
    }
  }

  test("Support multi-children count") {
    runQueryAndCompare(
      """
        |select l_orderkey, count(distinct l_partkey, l_comment)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |select l_orderkey, count(l_shipdate, l_comment)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |select l_orderkey, count(distinct l_partkey, l_comment), count(l_shipdate, l_comment)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |select l_orderkey, count(distinct l_partkey), count(l_shipdate, l_comment)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |select l_orderkey, count(distinct l_partkey, l_comment), count(l_shipdate)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
  }

  test("Support multi-children count with row construct") {
    runQueryAndCompare(
      """
        |select l_orderkey, count(distinct l_partkey, l_comment), corr(l_partkey, l_partkey+1)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
  }

  test("Remainder with non-foldable right side") {
    withTable("remainder") {
      spark.sql("""
                  |CREATE TABLE remainder USING PARQUET
                  |AS SELECT id as c1, id % 3 as c2 FROM range(3)
                  |""".stripMargin)
      spark.sql("INSERT INTO TABLE remainder VALUES(0, null)")

      runQueryAndCompare("SELECT c1 % c2 FROM remainder")(df => checkFallbackOperators(df, 0))
    }
  }

  test("Support Array type signature") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1(id INT, l ARRAY<INT>) USING PARQUET")
      sql("INSERT INTO t1 VALUES(1, ARRAY(1, 2)), (2, ARRAY(3, 4))")
      runQueryAndCompare("SELECT first(l) FROM t1")(df => checkFallbackOperators(df, 0))

      sql("CREATE TABLE t2(id INT, l ARRAY<STRUCT<k: INT, v: INT>>) USING PARQUET")
      sql("INSERT INTO t2 VALUES(1, ARRAY(STRUCT(1, 100))), (2, ARRAY(STRUCT(2, 200)))")
      runQueryAndCompare("SELECT first(l) FROM t2")(df => checkFallbackOperators(df, 1))
    }
  }

  test("Fall back multiple expressions") {
    runQueryAndCompare(
      """
        |select (l_partkey % 10 + 5)
        |from lineitem
        |""".stripMargin
    )(checkOperatorMatch[ProjectExecTransformer])

    runQueryAndCompare(
      """
        |select l_partkey
        |from lineitem where (l_partkey % 10 + 5) > 6
        |""".stripMargin
    )(checkOperatorMatch[FilterExecTransformer])

    withSQLConf(GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "2") {
      runQueryAndCompare(
        """
          |select (l_partkey % 10 + 5)
          |from lineitem
          |""".stripMargin
      )(checkFallbackOperatorMatch[ProjectExec])

      runQueryAndCompare(
        """
          |select l_partkey
          |from lineitem where (l_partkey % 10 + 5) > 6
          |""".stripMargin
      )(checkFallbackOperatorMatch[FilterExec])
    }
  }

  test("test array literal") {
    withTable("array_table") {
      sql("create table array_table(a array<bigint>) using parquet")
      sql("insert into table array_table select array(1)")
      runQueryAndCompare("select size(coalesce(a, array())) from array_table") {
        df =>
          {
            assert(getExecutedPlan(df).count(_.isInstanceOf[ProjectExecTransformer]) == 1)
          }
      }
    }
  }

  test("test map literal") {
    withTable("map_table") {
      sql("create table map_table(a map<bigint, string>) using parquet")
      sql("insert into table map_table select map(1, 'hello')")
      runQueryAndCompare("select size(coalesce(a, map())) from map_table") {
        df =>
          {
            assert(getExecutedPlan(df).count(_.isInstanceOf[ProjectExecTransformer]) == 1)
          }
      }
    }
  }

  test("Support In list option contains non-foldable expression") {
    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (1, 2, l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (1, 2, l_partkey - 1, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey not in (1, 2, l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (l_partkey + 1, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey not in (l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
  }

  test("Support StructType in HashAggregate") {
    runQueryAndCompare("""
                         |select s, count(1) from (
                         |   select named_struct('id', cast(id as int),
                         |   'id_str', cast(id as string)) as s from range(100)
                         |) group by s
                         |""".stripMargin) {
      checkOperatorMatch[HashAggregateExecTransformer]
    }
  }

  test("test roundrobine with sort") {
    // scalastyle:off
    runQueryAndCompare("SELECT /*+ REPARTITION(3) */ l_orderkey, l_partkey FROM lineitem") {
      /*
        ColumnarExchange RoundRobinPartitioning(3), REPARTITION_BY_NUM, [l_orderkey#16L, l_partkey#17L)
        +- ^(2) ProjectExecTransformer [l_orderkey#16L, l_partkey#17L]
          +- ^(2) SortExecTransformer [hash_partition_key#302 ASC NULLS FIRST], false, 0
            +- ^(2) ProjectExecTransformer [hash(l_orderkey#16L, l_partkey#17L) AS hash_partition_key#302, l_orderkey#16L, l_partkey#17L]
                +- ^(2) BatchScanExecTransformer[l_orderkey#16L, l_partkey#17L] ParquetScan DataFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<l_orderkey:bigint,l_partkey:bigint>, PushedFilters: [] RuntimeFilters: []
       */
      checkOperatorMatch[SortExecTransformer]
    }
    // scalastyle:on

    withSQLConf("spark.sql.execution.sortBeforeRepartition" -> "false") {
      runQueryAndCompare("""SELECT /*+ REPARTITION(3) */
                           | l_orderkey, l_partkey FROM lineitem""".stripMargin) {
        df =>
          {
            assert(getExecutedPlan(df).count(_.isInstanceOf[SortExecTransformer]) == 0)
          }
      }
    }
  }

  test("Support Map type signature") {
    // test map<str,str>
    withTempView("t1") {
      Seq[(Int, Map[String, String])]((1, Map("byte1" -> "aaa")), (2, Map("byte2" -> "bbbb")))
        .toDF("c1", "map_c2")
        .createTempView("t1")
      runQueryAndCompare("""
                           |SELECT c1, collect_list(map_c2) FROM t1 group by c1;
                           |""".stripMargin) {
        checkOperatorMatch[HashAggregateExecTransformer]
      }
    }
    // test map<str,map<str,str>>
    withTempView("t2") {
      Seq[(Int, Map[String, Map[String, String]])](
        (1, Map("byte1" -> Map("test1" -> "aaaa"))),
        (2, Map("byte2" -> Map("test1" -> "bbbb"))))
        .toDF("c1", "map_c2")
        .createTempView("t2")
      runQueryAndCompare("""
                           |SELECT c1, collect_list(map_c2) FROM t2 group by c1;
                           |""".stripMargin) {
        checkOperatorMatch[HashAggregateExecTransformer]
      }
    }
    // test map<map<str,str>,map<str,str>>
    withTempView("t3") {
      Seq[(Int, Map[Map[String, String], Map[String, String]])](
        (1, Map(Map("byte1" -> "aaaa") -> Map("test1" -> "aaaa"))),
        (2, Map(Map("byte2" -> "bbbb") -> Map("test1" -> "bbbb"))))
        .toDF("c1", "map_c2")
        .createTempView("t3")
      runQueryAndCompare("""
                           |SELECT collect_list(map_c2) FROM t3 group by c1;
                           |""".stripMargin) {
        checkOperatorMatch[HashAggregateExecTransformer]
      }
    }
    // test map<str,list<str>>
    withTempView("t4") {
      Seq[(Int, Map[String, Array[String]])](
        (1, Map("test1" -> Array("test1", "test2"))),
        (2, Map("test2" -> Array("test1", "test2"))))
        .toDF("c1", "map_c2")
        .createTempView("t4")
      runQueryAndCompare("""
                           |SELECT collect_list(map_c2) FROM t4 group by c1;
                           |""".stripMargin) {
        checkOperatorMatch[HashAggregateExecTransformer]
      }
    }
  }
}
