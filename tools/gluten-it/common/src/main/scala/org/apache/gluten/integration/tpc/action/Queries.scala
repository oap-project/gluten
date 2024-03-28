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
package org.apache.gluten.integration.tpc.action

import org.apache.gluten.integration.stat.RamStat
import org.apache.gluten.integration.tpc.{TpcRunner, TpcSuite}

import org.apache.commons.lang3.exception.ExceptionUtils

case class Queries(
    scale: Double,
    queryIds: Array[String],
    excludedQueryIds: Array[String],
    explain: Boolean,
    iterations: Int,
    randomKillTasks: Boolean)
  extends Action {

  override def execute(tpcSuite: TpcSuite): Boolean = {
    val runQueryIds = tpcSuite.selectQueryIds(queryIds, excludedQueryIds)
    val runner: TpcRunner = new TpcRunner(tpcSuite.queryResource(), tpcSuite.dataWritePath(scale))
    val results = (0 until iterations).flatMap {
      iteration =>
        println(s"Running tests (iteration $iteration)...")
        runQueryIds.map {
          queryId =>
            Queries.runTpcQuery(
              runner,
              tpcSuite.sessionSwitcher,
              queryId,
              tpcSuite.desc(),
              explain,
              randomKillTasks)
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
    Queries.printResults(succeed)
    println("")

    if (passedCount == count) {
      println("No failed queries. ")
      println("")
    } else {
      println("Failed queries: ")
      println("")
      Queries.printResults(results.filter(!_.testPassed))
      println("")
    }

    var all = Queries.aggregate(results, "all")

    if (passedCount != count) {
      all = Queries.aggregate(succeed, "all succeed") ::: all
    }

    println("Overall: ")
    println("")
    Queries.printResults(all)
    println("")

    if (passedCount != count) {
      return false
    }
    true
  }
}

object Queries {
  case class TestResultLine(
      queryId: String,
      testPassed: Boolean,
      rowCount: Option[Long],
      executionTimeMillis: Option[Long],
      errorMessage: Option[String])

  private def printResults(results: List[TestResultLine]): Unit = {
    printf(
      "|%15s|%15s|%30s|%30s|\n",
      "Query ID",
      "Was Passed",
      "Row Count",
      "Query Time (Millis)"
    )
    results.foreach {
      line =>
        printf(
          "|%15s|%15s|%30s|%30s|\n",
          line.queryId,
          line.testPassed,
          line.rowCount.getOrElse("N/A"),
          line.executionTimeMillis.getOrElse("N/A")
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
            if (r1.rowCount.nonEmpty && r2.rowCount.nonEmpty)
              Some(r1.rowCount.get + r2.rowCount.get)
            else None,
            if (r1.executionTimeMillis.nonEmpty && r2.executionTimeMillis.nonEmpty)
              Some(r1.executionTimeMillis.get + r2.executionTimeMillis.get)
            else None,
            None
          )))
  }

  private def runTpcQuery(
      runner: _root_.org.apache.gluten.integration.tpc.TpcRunner,
      sessionSwitcher: _root_.org.apache.spark.sql.SparkSessionSwitcher,
      id: _root_.java.lang.String,
      desc: _root_.java.lang.String,
      explain: Boolean,
      randomKillTasks: Boolean) = {
    println(s"Running query: $id...")
    try {
      val testDesc = "Gluten Spark %s %s".format(desc, id)
      sessionSwitcher.useSession("test", testDesc)
      runner.createTables(sessionSwitcher.spark())
      val result = runner.runTpcQuery(
        sessionSwitcher.spark(),
        testDesc,
        id,
        explain = explain,
        randomKillTasks = randomKillTasks)
      val resultRows = result.rows
      println(
        s"Successfully ran query $id. " +
          s"Returned row count: ${resultRows.length}")
      TestResultLine(
        id,
        testPassed = true,
        Some(resultRows.length),
        Some(result.executionTimeMillis),
        None)
    } catch {
      case e: Exception =>
        val error = Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}")
        println(
          s"Error running query $id. " +
            s" Error: ${error.get}")
        TestResultLine(id, testPassed = false, None, None, error)
    }
  }
}
