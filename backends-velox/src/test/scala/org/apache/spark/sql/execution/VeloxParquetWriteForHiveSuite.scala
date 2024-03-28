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
package org.apache.spark.sql.execution

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils

class VeloxParquetWriteForHiveSuite extends GlutenQueryTest with SQLTestUtils {
  private var _spark: SparkSession = null

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    if (_spark == null) {
      _spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    }

    _spark.sparkContext.setLogLevel("info")
  }

  override protected def spark: SparkSession = _spark

  protected def defaultSparkConf: SparkConf = {
    val conf = new SparkConf()
      .set("spark.master", "local[1]")
      .set("spark.sql.test", "")
      .set("spark.sql.testkey", "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
      .set(
        HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES.key,
        "org.apache.spark.sql.hive.execution.PairSerDe")
      // SPARK-8910
      .set(UI_ENABLED, false)
      .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      // Hive changed the default of hive.metastore.disallow.incompatible.col.type.changes
      // from false to true. For details, see the JIRA HIVE-12320 and HIVE-17764.
      .set("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)

    conf.set(
      StaticSQLConf.WAREHOUSE_PATH,
      conf.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)
  }

  protected def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.gluten.sql.native.writer.enabled", "true")
  }

  private def checkNativeWrite(sqlStr: String, native: Boolean): Unit = {
    val testAppender = new LogAppender("native write tracker")
    withLogAppender(testAppender) {
      spark.sql(sqlStr)
    }
    assert(
      testAppender.loggingEvents.exists(
        _.getMessage.toString.contains("Use Gluten parquet write for hive")) == native)
  }

  private def checkNativeStaticPartitionWrite(sqlStr: String, native: Boolean): Unit = {
    val testAppender = new LogAppender("native write tracker")
    withLogAppender(testAppender) {
      spark.sql(sqlStr)
    }
    assert(
      testAppender.loggingEvents.exists(
        _.getMessage.toString.contains("Use Gluten partition write for hive")) == native)
  }

  test("test hive static partition write table") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (c int, d long, e long)" +
          " STORED AS PARQUET partitioned by (c, d)")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        if (SparkShimLoader.getSparkVersion.startsWith("3.4")) {
          checkNativeStaticPartitionWrite(
            "INSERT OVERWRITE TABLE t partition(c=1, d=2)" +
              " SELECT 3 as e",
            native = false)
        } else {
          checkNativeStaticPartitionWrite(
            "INSERT OVERWRITE TABLE t partition(c=1, d=2)" +
              " SELECT 3 as e",
            native = true)
        }

      }
      checkAnswer(spark.table("t"), Row(3, 1, 2))
    }
  }

  test("test hive dynamic and static partition write table") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (c int, d long, e long)" +
          " STORED AS PARQUET partitioned by (c, d)")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        checkNativeStaticPartitionWrite(
          "INSERT OVERWRITE TABLE t partition(c=1, d)" +
            " SELECT 3 as e, 2 as e",
          native = false)
      }
      checkAnswer(spark.table("t"), Row(3, 1, 2))
    }
  }

  test("test hive write table") {
    withTable("t") {
      spark.sql("CREATE TABLE t (c int) STORED AS PARQUET")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
        if (SparkShimLoader.getSparkVersion.startsWith("3.4")) {
          checkNativeWrite("INSERT OVERWRITE TABLE t SELECT 1 as c", native = false)
        } else {
          checkNativeWrite("INSERT OVERWRITE TABLE t SELECT 1 as c", native = true)
        }

      }
      checkAnswer(spark.table("t"), Row(1))
    }
  }

  test("test hive write dir") {
    withTempPath {
      f =>
        // compatible with Spark3.3 and later
        withSQLConf("spark.sql.hive.convertMetastoreInsertDir" -> "false") {
          if (SparkShimLoader.getSparkVersion.startsWith("3.4")) {
            checkNativeWrite(
              s"""
                 |INSERT OVERWRITE DIRECTORY '${f.getCanonicalPath}' STORED AS PARQUET SELECT 1 as c
                 |""".stripMargin,
              native = false
            )
          } else {
            checkNativeWrite(
              s"""
                 |INSERT OVERWRITE DIRECTORY '${f.getCanonicalPath}' STORED AS PARQUET SELECT 1 as c
                 |""".stripMargin,
              native = true
            )
          }

          checkAnswer(spark.read.parquet(f.getCanonicalPath), Row(1))
        }
    }
  }

  test("select plain hive table") {
    withTable("t") {
      sql("CREATE TABLE t AS SELECT 1 as c")
      checkAnswer(sql("SELECT * FROM t"), Row(1))
    }
  }
}
