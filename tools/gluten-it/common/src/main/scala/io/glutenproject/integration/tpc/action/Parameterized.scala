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

import org.apache.spark.sql.ConfUtils.ConfImplicits._
import org.apache.spark.sql.SparkSessionSwitcher

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Parameterized(
    scale: Double,
    queryIds: Array[String],
    excludedQueryIds: Array[String],
    iterations: Int,
    warmupIterations: Int,
    configDimensions: Seq[Dim],
    metrics: Array[String])
  extends Action {

  private def validateDims(configDimensions: Seq[Dim]): Unit = {
    if (
      configDimensions
        .map(
          dim => {
            dim.name
          })
        .toSet
        .size != configDimensions.size
    ) {
      throw new IllegalStateException("Duplicated dimension name found")
    }

    configDimensions.foreach {
      dim =>
        if (dim.dimValues.map(dimValue => dimValue.name).toSet.size != dim.dimValues.size) {
          throw new IllegalStateException("Duplicated dimension value found")
        }
    }
  }

  private val coordinates: mutable.LinkedHashMap[Coordinate, Seq[(String, String)]] = {
    validateDims(configDimensions)
    val dimCount = configDimensions.size
    val coordinateMap = mutable.LinkedHashMap[Coordinate, Seq[(String, String)]]()

    def fillCoordinates(
        dimOffset: Int,
        intermediateCoordinates: Map[String, String],
        intermediateConf: Seq[(String, String)]): Unit = {
      if (dimOffset == dimCount) {
        // we got one coordinate
        coordinateMap(Coordinate(intermediateCoordinates)) = intermediateConf
        return
      }
      val dim = configDimensions(dimOffset)
      dim.dimValues.foreach {
        dimValue =>
          fillCoordinates(
            dimOffset + 1,
            intermediateCoordinates + (dim.name -> dimValue.name),
            intermediateConf ++ dimValue.conf)
      }
    }

    fillCoordinates(0, Map(), Seq())

    coordinateMap
  }

  override def execute(tpcSuite: TpcSuite): Boolean = {
    val runner: TpcRunner = new TpcRunner(tpcSuite.queryResource(), tpcSuite.dataWritePath(scale))
    val allQueries = tpcSuite.allQueryIds()

    val sessionSwitcher = tpcSuite.sessionSwitcher
    val testConf = tpcSuite.getTestConf()

    coordinates.foreach {
      entry =>
        // register one session per coordinate
        val coordinate = entry._1
        val coordinateConf = entry._2
        val conf = testConf.clone()
        conf.setAllWarningOnOverriding(coordinateConf)
        sessionSwitcher.registerSession(coordinate.toString, conf)
    }

    val runQueryIds = tpcSuite.selectQueryIds(queryIds, excludedQueryIds)

    // warm up
    (0 until warmupIterations).foreach {
      _ =>
        runQueryIds.foreach {
          queryId => Parameterized.warmUp(queryId, tpcSuite.desc(), sessionSwitcher, runner)
        }
    }

    val results = coordinates.flatMap {
      entry =>
        val coordinate = entry._1
        val coordinateResults = (0 until iterations).flatMap {
          iteration =>
            println(s"Running tests (iteration $iteration) with coordinate $coordinate...")
            runQueryIds.map {
              queryId =>
                Parameterized.runTpcQuery(
                  runner,
                  sessionSwitcher,
                  queryId,
                  coordinate,
                  tpcSuite.desc(),
                  metrics)
            }
        }.toList
        coordinateResults
    }

    val dimNames = configDimensions.map(dim => dim.name)

    val passedCount = results.count(l => l.succeed)
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
    TestResultLines(dimNames, metrics, results.filter(_.succeed)).print()
    println("")

    if (passedCount == count) {
      println("No failed queries. ")
      println("")
    } else {
      println("Failed queries: ")
      println("")
      TestResultLines(dimNames, metrics, results.filter(!_.succeed)).print()
      println("")
    }

    if (passedCount != count) {
      return false
    }
    true
  }
}

case class Dim(name: String, dimValues: Seq[DimValue])
case class DimValue(name: String, conf: Seq[(String, String)])
case class Coordinate(coordinate: Map[String, String]) // [dim, dim value]

case class TestResultLine(
    queryId: String,
    succeed: Boolean,
    coordinate: Coordinate,
    rowCount: Option[Long],
    executionTimeMillis: Option[Long],
    metrics: Map[String, Long],
    errorMessage: Option[String])

case class TestResultLines(
    dimNames: Seq[String],
    metricNames: Seq[String],
    lines: Iterable[TestResultLine]) {
  def print(): Unit = {
    var fmt = "|%15s|%15s"
    for (_ <- dimNames.indices) {
      fmt = fmt + "|%20s"
    }
    for (_ <- metricNames.indices) {
      fmt = fmt + "|%35s"
    }
    fmt = fmt + "|%30s|%30s|\n"
    val fields = ArrayBuffer[String]("Query ID", "Succeed")
    dimNames.foreach(dimName => fields.append(dimName))
    metricNames.foreach(metricName => fields.append(metricName))
    fields.append("Row Count")
    fields.append("Query Time (Millis)")
    printf(fmt, fields: _*)
    lines.foreach {
      line =>
        val values = ArrayBuffer[Any](line.queryId, line.succeed)
        dimNames.foreach {
          dimName =>
            val coordinate = line.coordinate.coordinate
            if (!coordinate.contains(dimName)) {
              throw new IllegalStateException("Dimension name not found" + dimName)
            }
            values.append(coordinate(dimName))
        }
        metricNames.foreach {
          metricName =>
            val metrics = line.metrics
            values.append(metrics.getOrElse(metricName, "N/A"))
        }
        values.append(line.rowCount.getOrElse("N/A"))
        values.append(line.executionTimeMillis.getOrElse("N/A"))
        printf(fmt, values: _*)
    }
  }
}

object Parameterized {
  private def runTpcQuery(
      runner: TpcRunner,
      sessionSwitcher: SparkSessionSwitcher,
      id: String,
      coordinate: Coordinate,
      desc: String,
      metrics: Array[String]) = {
    println(s"Running query: $id...")
    try {
      val testDesc = "Gluten Spark %s %s %s".format(desc, id, coordinate)
      sessionSwitcher.useSession(coordinate.toString, testDesc)
      runner.createTables(sessionSwitcher.spark())
      val result =
        runner.runTpcQuery(sessionSwitcher.spark(), testDesc, id, explain = false, metrics)
      val resultRows = result.rows
      println(
        s"Successfully ran query $id. " +
          s"Returned row count: ${resultRows.length}")
      TestResultLine(
        id,
        succeed = true,
        coordinate,
        Some(resultRows.length),
        Some(result.executionTimeMillis),
        result.metrics,
        None)
    } catch {
      case e: Exception =>
        val error = Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}")
        println(
          s"Error running query $id. " +
            s" Error: ${error.get}")
        TestResultLine(id, succeed = false, coordinate, None, None, Map.empty, error)
    }
  }

  private[tpc] def warmUp(
      id: String,
      desc: String,
      sessionSwitcher: SparkSessionSwitcher,
      runner: TpcRunner): Unit = {
    println(s"Warming up: Running query: $id...")
    try {
      val testDesc = "Gluten Spark %s %s warm up".format(desc, id)
      sessionSwitcher.useSession("test", testDesc)
      runner.createTables(sessionSwitcher.spark())
      val result = runner.runTpcQuery(sessionSwitcher.spark(), testDesc, id, explain = false)
      val resultRows = result.rows
      println(
        s"Warming up: Successfully ran query $id. " +
          s"Returned row count: ${resultRows.length}")
    } catch {
      case e: Exception =>
        val error = Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}")
        println(
          s"Warming up: Error running query $id. " +
            s" Error: ${error.get}")
    }
  }
}
