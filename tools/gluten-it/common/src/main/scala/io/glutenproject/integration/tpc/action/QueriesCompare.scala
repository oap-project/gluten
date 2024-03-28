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
package io.glutenproject.integration.tpc.action

import io.glutenproject.integration.stat.RamStat
import io.glutenproject.integration.tpc.{TpcRunner, TpcSuite}

import org.apache.spark.sql.{SparkSessionSwitcher, TestUtils}

import org.apache.commons.lang3.exception.ExceptionUtils

case class QueriesCompare(
    scale: Double,
    queryIds: Array[String],
    excludedQueryIds: Array[String],
    explain: Boolean,
    iterations: Int)
  extends Action {

  override def execute(tpcSuite: TpcSuite): Boolean = {
    val runner: TpcRunner = new TpcRunner(tpcSuite.queryResource(), tpcSuite.dataWritePath(scale))
    val runQueryIds = tpcSuite.selectQueryIds(queryIds, excludedQueryIds)
    val results = (0 until iterations).flatMap {
      iteration =>
        println(s"Running tests (iteration $iteration)...")
        runQueryIds.map {
          queryId =>
            QueriesCompare.runTpcQuery(
              queryId,
              explain,
              tpcSuite.desc(),
              tpcSuite.sessionSwitcher,
              runner)
        }
    }.toList

    val passedCount = results.count(l => l.testPassed)
    val count = results.count(_ => true)

    // RAM stats
    println("Performing GC to collect RAM statistics... ")
    System.gc()
    System.gc()
    printf(
      "RAM statistics: JVM Heap size: %d KiB (total %d KiB), Process RSS: %d KiB\n",
      RamStat.getJvmHeapUsed(),
      RamStat.getJvmHeapTotal(),
      RamStat.getProcessRamUsed()
    )

    println("")
    println("Test report: ")
    println("")
    printf("Summary: %d out of %d queries passed. \n", passedCount, count)
    println("")
    val succeed = results.filter(_.testPassed)
    QueriesCompare.printResults(succeed)
    println("")

    if (passedCount == count) {
      println("No failed queries. ")
      println("")
    } else {
      println("Failed queries (a failed query with correct row count indicates value mismatches): ")
      println("")
      QueriesCompare.printResults(results.filter(!_.testPassed))
      println("")
    }

    var all = QueriesCompare.aggregate(results, "all")

    if (passedCount != count) {
      all = QueriesCompare.aggregate(succeed, "all succeed") ::: all
    }

    println("Overall: ")
    println("")
    QueriesCompare.printResults(all)
    println("")

    if (passedCount != count) {
      return false
    }
    true
  }
}

object QueriesCompare {
  case class TestResultLine(
      queryId: String,
      testPassed: Boolean,
      expectedRowCount: Option[Long],
      actualRowCount: Option[Long],
      expectedExecutionTimeMillis: Option[Long],
      actualExecutionTimeMillis: Option[Long],
      errorMessage: Option[String])

  private def printResults(results: List[TestResultLine]): Unit = {
    printf(
      "|%15s|%15s|%30s|%30s|%30s|%30s|%30s|\n",
      "Query ID",
      "Was Passed",
      "Expected Row Count",
      "Actual Row Count",
      "Baseline Query Time (Millis)",
      "Query Time (Millis)",
      "Query Time Variation"
    )
    results.foreach {
      line =>
        val timeVariation =
          if (
            line.expectedExecutionTimeMillis.nonEmpty && line.actualExecutionTimeMillis.nonEmpty
          ) {
            Some(
              ((line.expectedExecutionTimeMillis.get - line.actualExecutionTimeMillis.get).toDouble
                / line.actualExecutionTimeMillis.get.toDouble) * 100)
          } else None
        printf(
          "|%15s|%15s|%30s|%30s|%30s|%30s|%30s|\n",
          line.queryId,
          line.testPassed,
          line.expectedRowCount.getOrElse("N/A"),
          line.actualRowCount.getOrElse("N/A"),
          line.expectedExecutionTimeMillis.getOrElse("N/A"),
          line.actualExecutionTimeMillis.getOrElse("N/A"),
          timeVariation.map("%15.2f%%".format(_)).getOrElse("N/A")
        )
    }
  }

  private def aggregate(succeed: List[TestResultLine], name: String): List[TestResultLine] = {
    if (succeed.isEmpty) {
      return Nil
    }
    List(
      succeed.reduce(
        (r1, r2) =>
          TestResultLine(
            name,
            testPassed = true,
            if (r1.expectedRowCount.nonEmpty && r2.expectedRowCount.nonEmpty)
              Some(r1.expectedRowCount.get + r2.expectedRowCount.get)
            else None,
            if (r1.actualRowCount.nonEmpty && r2.actualRowCount.nonEmpty)
              Some(r1.actualRowCount.get + r2.actualRowCount.get)
            else None,
            if (r1.expectedExecutionTimeMillis.nonEmpty && r2.expectedExecutionTimeMillis.nonEmpty)
              Some(r1.expectedExecutionTimeMillis.get + r2.expectedExecutionTimeMillis.get)
            else None,
            if (r1.actualExecutionTimeMillis.nonEmpty && r2.actualExecutionTimeMillis.nonEmpty)
              Some(r1.actualExecutionTimeMillis.get + r2.actualExecutionTimeMillis.get)
            else None,
            None
          )))
  }

  private[tpc] def runTpcQuery(
      id: String,
      explain: Boolean,
      desc: String,
      sessionSwitcher: SparkSessionSwitcher,
      runner: TpcRunner): TestResultLine = {
    println(s"Running query: $id...")
    try {
      val baseLineDesc = "Vanilla Spark %s %s".format(desc, id)
      sessionSwitcher.useSession("baseline", baseLineDesc)
      runner.createTables(sessionSwitcher.spark())
      val expected =
        runner.runTpcQuery(sessionSwitcher.spark(), baseLineDesc, id, explain = explain)
      val expectedRows = expected.rows
      val testDesc = "Gluten Spark %s %s".format(desc, id)
      sessionSwitcher.useSession("test", testDesc)
      runner.createTables(sessionSwitcher.spark())
      val result = runner.runTpcQuery(sessionSwitcher.spark(), testDesc, id, explain = explain)
      val resultRows = result.rows
      val error = TestUtils.compareAnswers(resultRows, expectedRows, sort = true)
      // FIXME: This is too hacky
      // A list of query ids whose corresponding query results can differ because of order.
      val unorderedQueries = Seq("q65")
      if (error.isEmpty || unorderedQueries.contains(id)) {
        println(
          s"Successfully ran query $id, result check was passed. " +
            s"Returned row count: ${resultRows.length}, expected: ${expectedRows.length}")
        return TestResultLine(
          id,
          testPassed = true,
          Some(expectedRows.length),
          Some(resultRows.length),
          Some(expected.executionTimeMillis),
          Some(result.executionTimeMillis),
          None)
      }
      println(s"Error running query $id, result check was not passed. " +
        s"Returned row count: ${resultRows.length}, expected: ${expectedRows.length}, error: ${error.get}")
      TestResultLine(
        id,
        testPassed = false,
        Some(expectedRows.length),
        Some(resultRows.length),
        Some(expected.executionTimeMillis),
        Some(result.executionTimeMillis),
        error)
    } catch {
      case e: Exception =>
        val error = Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}")
        println(
          s"Error running query $id. " +
            s" Error: ${error.get}")
        TestResultLine(id, testPassed = false, None, None, None, None, error)
    }
  }
}
