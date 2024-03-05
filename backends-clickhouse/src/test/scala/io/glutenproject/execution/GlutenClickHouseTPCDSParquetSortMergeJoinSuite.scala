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

import io.glutenproject.utils.FallbackUtil

import org.apache.spark.SparkConf

class GlutenClickHouseTPCDSParquetSortMergeJoinSuite extends GlutenClickHouseTPCDSAbstractSuite {

  override protected val tpcdsQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpcds-queries/tpcds.queries.original"
  override protected val queriesResults: String = rootPath + "tpcds-queries-output"

  override protected def excludedTpcdsQueries: Set[String] = Set(
    // fallback due to left semi/anti
    "q8",
    "q14a",
    "q14b",
    "q23a",
    "q23b",
    "q51",
    "q69",
    "q70",
    "q78",
    "q95",
    "q97"
  ) ++ super.excludedTpcdsQueries

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.memory.offHeap.size", "8g")
      .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "false")
  }

  executeTPCDSTest(false)

  test("sort merge join: inner join") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val testSql =
        """SELECT  count(*) cnt
          |FROM item i join item j on j.i_category = i.i_category
          |where
          |i.i_current_price > 1.0 """.stripMargin
      compareResultsAgainstVanillaSpark(
        testSql,
        true,
        df => {
          val smjTransformers = df.queryExecution.executedPlan.collect {
            case f: CHSortMergeJoinExecTransformer => f
          }
          assert(smjTransformers.size == 1)
        }
      )
    }
  }

  test("sort merge join: left outer join") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val testSql =
        """SELECT  count(*) cnt
          |FROM item i left outer join item j on j.i_category = i.i_category
        """.stripMargin
      compareResultsAgainstVanillaSpark(
        testSql,
        true,
        df => {
          val smjTransformers = df.queryExecution.executedPlan.collect {
            case f: CHSortMergeJoinExecTransformer => f
          }
          assert(smjTransformers.size == 1)
        }
      )
    }
  }

  test("sort merge join: right outer join") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val testSql =
        """SELECT  count(*) cnt
          |FROM item i right outer join item j on j.i_category = i.i_category
        """.stripMargin
      compareResultsAgainstVanillaSpark(
        testSql,
        true,
        df => {
          val smjTransformers = df.queryExecution.executedPlan.collect {
            case f: CHSortMergeJoinExecTransformer => f
          }
          assert(smjTransformers.size == 1)
        }
      )
    }
  }

  test("sort merge join: left semi join should fallback") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val testSql =
        """SELECT  count(*) cnt
          |FROM item i left semi join item j on j.i_category = i.i_category
          |where
          |i.i_current_price > 1.0 """.stripMargin
      val df = spark.sql(testSql)
      val smjTransformers = df.queryExecution.executedPlan.collect {
        case f: CHSortMergeJoinExecTransformer => f
      }
      assert(smjTransformers.size == 0)
      assert(FallbackUtil.hasFallback(df.queryExecution.executedPlan))
    }
  }

  test("sort merge join: left anti join should fallback") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val testSql =
        """SELECT  count(*) cnt
          |FROM item i left anti join item j on j.i_category = i.i_category
          |where
          |i.i_current_price > 1.0 """.stripMargin
      val df = spark.sql(testSql)
      val smjTransformers = df.queryExecution.executedPlan.collect {
        case f: CHSortMergeJoinExecTransformer => f
      }
      assert(smjTransformers.size == 0)
      assert(FallbackUtil.hasFallback(df.queryExecution.executedPlan))
    }
  }

  val createItem =
    """CREATE TABLE myitem (
      |  i_current_price DECIMAL(7,2),
      |  i_category STRING)
      |USING parquet""".stripMargin

  val insertItem =
    """insert into myitem values
      |(null,null),
      |(null,null),
      |(0.63,null),
      |(0.74,null),
      |(null,null),
      |(90.72,'Books'),
      |(99.89,'Books'),
      |(99.41,'Books')
      |""".stripMargin

  test("sort merge join: full outer join") {
    withTable("myitem") {
      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
        spark.sql(createItem)
        spark.sql(insertItem)
        val testSql =
          """SELECT  count(*) cnt
            |FROM myitem i full outer join myitem j on j.i_category = i.i_category
          """.stripMargin
        compareResultsAgainstVanillaSpark(
          testSql,
          true,
          df => {
            val smjTransformers = df.queryExecution.executedPlan.collect {
              case f: CHSortMergeJoinExecTransformer => f
            }
            assert(smjTransformers.size == 1)
          }
        )
      }
    }
  }

  // TODO enable this after CH support nulls smallest in MergeJoinTransform
  ignore("sort merge join: nulls smallest") {
    withTable("myitem") {
      withSQLConf(
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.shuffle.partitions" -> "3") {
        spark.sql(createItem)
        spark.sql(insertItem)
        val testSql =
          """SELECT  count(*) cnt
            |FROM myitem i
            |where
            |i.i_current_price > 1.0 *
            |  (SELECT avg(j.i_current_price)
            |  FROM myitem j
            |  WHERE j.i_category = i.i_category
            | ) """.stripMargin
        spark.sql(testSql).explain()
        spark.sql(testSql).show()
        compareResultsAgainstVanillaSpark(
          testSql,
          true,
          df => {
            val smjTransformers = df.queryExecution.executedPlan.collect {
              case f: CHSortMergeJoinExecTransformer => f
            }
            assert(smjTransformers.size == 1)
          }
        )
      }
    }

  }

}
