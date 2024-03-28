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

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.InputIteratorTransformer

class VeloxHashJoinSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.unsafe.exceptionOnMemoryLeak", "true")

  test("generate hash join plan - v1") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.adaptive.enabled", "false"),
      ("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")) {
      createTPCHNotNullTables()
      val df = spark.sql("""select l_partkey from
                           | lineitem join part join partsupp
                           | on l_partkey = p_partkey
                           | and l_suppkey = ps_suppkey""".stripMargin)
      val plan = df.queryExecution.executedPlan
      val joins = plan.collect { case shj: ShuffledHashJoinExecTransformer => shj }
      // scalastyle:off println
      System.out.println(plan)
      // scalastyle:on println line=68 column=19
      assert(joins.length == 2)

      // Children of Join should be seperated into different `TransformContext`s.
      assert(joins.forall(_.children.forall(_.isInstanceOf[InputIteratorTransformer])))

      // WholeStageTransformer should be inserted for joins and its children separately.
      val wholeStages = plan.collect { case wst: WholeStageTransformer => wst }
      assert(wholeStages.length == 5)

      // Join should be in `TransformContext`
      val countSHJ = wholeStages.map {
        _.collectFirst {
          case _: InputIteratorTransformer => 0
          case _: ShuffledHashJoinExecTransformer => 1
        }.getOrElse(0)
      }.sum
      assert(countSHJ == 2)
    }
  }

  test("generate hash join plan - v2") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.adaptive.enabled", "false"),
      ("spark.gluten.sql.columnar.forceShuffledHashJoin", "true"),
      ("spark.sql.sources.useV1SourceList", "avro")
    ) {
      createTPCHNotNullTables()
      val df = spark.sql("""select l_partkey from
                           | lineitem join part join partsupp
                           | on l_partkey = p_partkey
                           | and l_suppkey = ps_suppkey""".stripMargin)
      val plan = df.queryExecution.executedPlan
      val joins = plan.collect { case shj: ShuffledHashJoinExecTransformer => shj }
      assert(joins.length == 2)

      // The computing is combined into one single whole stage transformer.
      val wholeStages = plan.collect { case wst: WholeStageTransformer => wst }
      if (SparkShimLoader.getSparkVersion.startsWith("3.2.")) {
        assert(wholeStages.length == 1)
      } else {
        assert(wholeStages.length == 3)
      }

      // Join should be in `TransformContext`
      val countSHJ = wholeStages.map {
        _.collectFirst {
          case _: InputIteratorTransformer => 0
          case _: ShuffledHashJoinExecTransformer => 1
        }.getOrElse(0)
      }.sum
      if (SparkShimLoader.getSparkVersion.startsWith("3.2.")) {
        assert(countSHJ == 1)
      } else {
        assert(countSHJ == 2)
      }
    }
  }
}
