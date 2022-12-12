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

import java.io.File
import java.util.TimeZone

import scala.collection.JavaConverters._

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.utils.SystemParameters
import org.apache.commons.io.FileUtils
import org.junit.Assert
import org.scalactic.source.Position
import org.scalatest.{Assertions, Canceled, Tag}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.util.{sideBySide, stackTraceToString}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Basic trait for Gluten SQL test cases
 */
trait GlutenSQLTestsTrait extends QueryTest with SharedSparkSession with GlutenTestsBaseTrait {

  def prepareWorkDir(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
  }

  override def beforeAll(): Unit = {
    prepareWorkDir()
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override protected def test(testName: String,
                              testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(testFun)
    } else {
      logWarning(s"Ignore test case: ${testName}")
    }
  }

  override def logForFailedTest(): Unit = {
    logError("Test failed so abort")
    // FIXME the code swallows exceptions. See https://github.com/oap-project/gluten/issues/676
    System.exit(1)
  }

  override def sparkConf: SparkConf = {
    // Native SQL configs
    val conf = super.sparkConf
      .setAppName("Gluten-UT")
      .set("spark.driver.memory", "1G")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.files.maxPartitionBytes", "134217728")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.warehouse.dir", warehouse)

    if (BackendsApiManager.getBackendName.equalsIgnoreCase(
      GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)) {
      conf
        .set("spark.io.compression.codec", "LZ4")
        .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
        .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
        .set("spark.gluten.sql.enable.native.validation", "false")
        .set(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getClickHouseLibPath)
        .set("spark.sql.files.openCostInBytes", "134217728")
        .set("spark.unsafe.exceptionOnMemoryLeak", "true")
    } else {
      conf.set("spark.unsafe.exceptionOnMemoryLeak", "false")
    }

    conf
  }

  override protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF =
      try df
      catch {
        case ae: AnalysisException =>
          if (ae.plan.isDefined) {
            fail(s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
          } else {
            throw ae
          }
      }

    assertEmptyMissingInput(analyzedDF)

    GlutenQueryTest.checkAnswer(analyzedDF, expectedAnswer)
  }
}

object GlutenQueryTest extends Assertions {

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df the DataFrame to be executed
   * @param expectedAnswer the expected result in a Seq of Rows.
   * @param checkToRDD whether to verify deserialization to an RDD. This runs the query twice.
   */
  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row], checkToRDD: Boolean = true): Unit = {
    getErrorMessageInCheckAnswer(df, expectedAnswer, checkToRDD) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a None will
   * be returned.
   *
   * @param df the DataFrame to be executed
   * @param expectedAnswer the expected result in a Seq of Rows.
   * @param checkToRDD whether to verify deserialization to an RDD. This runs the query twice.
   */
  def getErrorMessageInCheckAnswer(
      df: DataFrame,
      expectedAnswer: Seq[Row],
      checkToRDD: Boolean = true): Option[String] = {
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    if (checkToRDD) {
      SQLExecution.withSQLConfPropagated(df.sparkSession) {
        df.rdd.count() // Also attempt to deserialize as an RDD [SPARK-15791]
      }
    }

    val sparkAnswer =
      try df.collect().toSeq
      catch {
        case e: Exception =>
          val errorMessage =
            s"""
             |Exception thrown while executing query:
             |${df.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
          return Some(errorMessage)
      }

    sameRows(expectedAnswer, sparkAnswer, isSorted).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
         |${df.queryExecution}
         |== Results ==
         |$results
       """.stripMargin
    }
  }

  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case bd: java.math.BigDecimal => BigDecimal(bd)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] =>
        seq.map {
          case b: java.lang.Byte => b.byteValue
          case s: java.lang.Short => s.shortValue
          case i: java.lang.Integer => i.intValue
          case l: java.lang.Long => l.longValue
          case f: java.lang.Float => f.floatValue
          case d: java.lang.Double => d.doubleValue
          case x => x
        }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  private def genError(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): String = {
    val getRowType: Option[Row] => String = row =>
      row
        .map(row =>
          if (row.schema == null) {
            "struct<>"
          } else {
            s"${row.schema.catalogString}"
          })
        .getOrElse("struct<>")

    s"""
       |== Results ==
       |${sideBySide(
         s"== Correct Answer - ${expectedAnswer.size} ==" +:
           getRowType(expectedAnswer.headOption) +:
           prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
         s"== Spark Answer - ${sparkAnswer.size} ==" +:
           getRowType(sparkAnswer.headOption) +:
           prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")}
    """.stripMargin
  }

  def includesRows(expectedRows: Seq[Row], sparkAnswer: Seq[Row]): Option[String] = {
    if (!prepareAnswer(expectedRows, true).toSet.subsetOf(prepareAnswer(sparkAnswer, true).toSet)) {
      return Some(genError(expectedRows, sparkAnswer, true))
    }
    None
  }

  private def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r) }
    case (a: Map[_, _], b: Map[_, _]) =>
      a.size == b.size && a.keys.forall { aKey =>
        b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
      }
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r) }
    case (a: Product, b: Product) =>
      compare(a.productIterator.toSeq, b.productIterator.toSeq)
    case (a: Row, b: Row) =>
      compare(a.toSeq, b.toSeq)
    // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
    case (a: Double, b: Double) =>
      if (isNaN(a) || isNaN(b)) {
        java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
      } else {
        Math.abs(a - b) < 0.00001
      }
    case (a: Float, b: Float) =>
      java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
    case (a, b) => a == b
  }

  def isNaN(num: Double): Boolean = {
    num.isNaN || num.isInfinite || num.isNegInfinity || num.isPosInfinity
  }

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    // modify method 'compare'
    if (!compare(prepareAnswer(expectedAnswer, isSorted), prepareAnswer(sparkAnswer, isSorted))) {
      return Some(genError(expectedAnswer, sparkAnswer, isSorted))
    }
    None
  }

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   *
   * @param actualAnswer the actual result in a [[Row]].
   * @param expectedAnswer the expected result in a[[Row]].
   * @param absTol the absolute tolerance between actual and expected answers.
   */
  protected def checkAggregatesWithTol(actualAnswer: Row, expectedAnswer: Row, absTol: Double) = {
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")

    // TODO: support other numeric types besides Double
    // TODO: support struct types?
    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        assert(
          math.abs(actual - expected) < absTol,
          s"actual answer $actual not within $absTol of correct answer $expected")
      case (actual, expected) =>
        assert(actual == expected, s"$actual did not equal $expected")
    }
  }

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): Unit = {
    getErrorMessageInCheckAnswer(df, expectedAnswer.asScala.toSeq) match {
      case Some(errorMessage) => Assert.fail(errorMessage)
      case None =>
    }
  }
}
