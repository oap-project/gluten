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
package org.apache.spark.sql

import io.glutenproject.execution.{FileSourceScanExecTransformer, WholeStageTransformer}

import org.apache.spark.sql.execution.{ReusedSubqueryExec, SubqueryExec}
import org.apache.spark.sql.internal.SQLConf

class GlutenSubquerySuite extends SubquerySuite with GlutenSQLTestsTrait {

  // Test Canceled: IntegratedUDFTestUtils.shouldTestPythonUDFs was false
  override def testNameBlackList: Seq[String] = Seq(
    "SPARK-28441: COUNT bug in WHERE clause (Filter) with PythonUDF",
    "SPARK-28441: COUNT bug in SELECT clause (Project) with PythonUDF",
    "SPARK-28441: COUNT bug in Aggregate with PythonUDF",
    "SPARK-28441: COUNT bug negative examples with PythonUDF",
    "SPARK-28441: COUNT bug in nested subquery with PythonUDF",
    "SPARK-28441: COUNT bug with nasty predicate expr with PythonUDF",
    "SPARK-28441: COUNT bug in HAVING clause (Filter) with PythonUDF",
    "SPARK-28441: COUNT bug with attribute ref in subquery input and output with PythonUDF"
  )

  // === Following cases override super class's cases ===

  testGluten("SPARK-26893 Allow pushdown of partition pruning subquery filters to file source") {
    withTable("a", "b") {
      spark.range(4).selectExpr("id", "id % 2 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(2).write.saveAsTable("b")

      // need to execute the query before we can examine fs.inputRDDs()
      val df = sql("SELECT * FROM a WHERE p <= (SELECT MIN(id) FROM b)")
      checkAnswer(df, Seq(Row(0, 0), Row(2, 0)))
      assert(stripAQEPlan(df.queryExecution.executedPlan).collectFirst {
        case t: WholeStageTransformer => t
      } match {
        case Some(WholeStageTransformer(fs: FileSourceScanExecTransformer, _)) =>
          fs.dynamicallySelectedPartitions
            .exists(_.files.exists(_.getPath.toString.contains("p=0")))
        case _ => false
      })
    }
  }

  // InsertPreProject rule will move subquery from aggregate to pre-project. The
  // reused subquery appeared double times in partial and final aggregation in the past.
  // But now it appears only in pre-project. ClickHouse pull out pre-project in
  // columnar, its subqueryIds.size is same as vanilla Spark.

  testGluten("Merge non-correlated scalar subqueries in a subquery") {
    Seq(false, true).foreach {
      enableAQE =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
          val df = sql("""
                         |SELECT (
                         |  SELECT
                         |    SUM(
                         |      (SELECT avg(key) FROM testData) +
                         |      (SELECT sum(key) FROM testData) +
                         |      (SELECT count(distinct key) FROM testData))
                         |   FROM testData
                         |)
          """.stripMargin)

          checkAnswer(df, Row(520050.0) :: Nil)

          val plan = df.queryExecution.executedPlan
          val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
          val reusedSubqueryIds = collectWithSubqueries(plan) {
            case rs: ReusedSubqueryExec => rs.child.id
          }

          if (enableAQE) {
            assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
            assert(
              reusedSubqueryIds.size == 2,
              "Missing or unexpected reused ReusedSubqueryExec in the plan")
          } else {
            assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
            assert(
              reusedSubqueryIds.size == 2,
              "Missing or unexpected reused ReusedSubqueryExec in the plan")
          }
        }
    }
  }

  testGluten("Merge non-correlated scalar subqueries from different levels") {
    Seq(false, true).foreach {
      enableAQE =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
          val df = sql("""
                         |SELECT
                         |  (SELECT avg(key) FROM testData),
                         |  (
                         |    SELECT
                         |      SUM(
                         |        (SELECT sum(key) FROM testData)
                         |      )
                         |    FROM testData
                         |  )
          """.stripMargin)

          checkAnswer(df, Row(50.5, 505000) :: Nil)

          val plan = df.queryExecution.executedPlan
          val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
          val reusedSubqueryIds = collectWithSubqueries(plan) {
            case rs: ReusedSubqueryExec => rs.child.id
          }

          assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
          assert(
            reusedSubqueryIds.size == 1,
            "Missing or unexpected reused ReusedSubqueryExec in the plan")
        }
    }
  }

  testGluten("Merge non-correlated scalar subqueries from different parent plans") {
    Seq(false, true).foreach {
      enableAQE =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
          val df = sql("""
                         |SELECT
                         |  (
                         |    SELECT
                         |      SUM(
                         |        (SELECT avg(key) FROM testData)
                         |      )
                         |    FROM testData
                         |  ),
                         |  (
                         |    SELECT
                         |      SUM(
                         |        (SELECT sum(key) FROM testData)
                         |      )
                         |    FROM testData
                         |  )
          """.stripMargin)

          checkAnswer(df, Row(5050.0, 505000) :: Nil)

          val plan = df.queryExecution.executedPlan
          val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
          val reusedSubqueryIds = collectWithSubqueries(plan) {
            case rs: ReusedSubqueryExec => rs.child.id
          }

          if (enableAQE) {
            assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
            assert(
              reusedSubqueryIds.size == 2,
              "Missing or unexpected reused ReusedSubqueryExec in the plan")
          } else {
            assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
            assert(
              reusedSubqueryIds.size == 2,
              "Missing or unexpected reused ReusedSubqueryExec in the plan")
          }
        }
    }
  }
}
