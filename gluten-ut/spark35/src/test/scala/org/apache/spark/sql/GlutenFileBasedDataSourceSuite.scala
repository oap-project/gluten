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

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.hadoop.fs.Path

import scala.collection.mutable

class GlutenFileBasedDataSourceSuite extends FileBasedDataSourceSuite with GlutenSQLTestsTrait {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "false")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
  }

  // test data path is jar path, so failed, test code is same with spark
  test("gluten Option recursiveFileLookup: disable partition inferring") {
    val dataPath = getWorkspaceFilePath(
      "sql",
      "core",
      "src",
      "test",
      "resources").toString + "/" + "test-data/text-partitioned"

    val df = spark.read
      .format("binaryFile")
      .option("recursiveFileLookup", true)
      .load(dataPath)

    assert(!df.columns.contains("year"), "Expect partition inferring disabled")
    val fileList = df.select("path").collect().map(_.getString(0))

    val expectedFileList = Array(
      dataPath + "/year=2014/data.txt",
      dataPath + "/year=2015/data.txt"
    ).map(path => "file:" + new Path(path).toString)

    assert(fileList.toSet === expectedFileList.toSet)
  }

  test("gluten Spark native readers should respect spark.sql.caseSensitive - parquet") {
    withTempDir {
      dir =>
        val format = "parquet"
        val tableName = s"spark_25132_${format}_native"
        val tableDir = dir.getCanonicalPath + s"/$tableName"
        withTable(tableName) {
          val end = 5
          val data = spark.range(end).selectExpr("id as A", "id * 2 as b", "id * 3 as B")
          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            data.write.format(format).mode("overwrite").save(tableDir)
          }
          sql(s"CREATE TABLE $tableName (a LONG, b LONG) USING $format LOCATION '$tableDir'")

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
            checkAnswer(sql(s"select a from $tableName"), data.select("A"))
            checkAnswer(sql(s"select A from $tableName"), data.select("A"))

            // TODO: gluten can catch exception in executor side, but cannot catch SparkException
            //  in Driver side
            // RuntimeException is triggered at executor side, which is then wrapped as
            // SparkException at driver side
            //          val e1 = intercept[SparkException] {
            //            sql(s"select b from $tableName").collect()
            //          }
            //
            //          assert(
            //            e1.getCause.isInstanceOf[RuntimeException] &&
            //              e1.getMessage.contains(
            //                """Found duplicate field(s) b in case-insensitive mode """))
            //          val e2 = intercept[SparkException] {
            //            sql(s"select B from $tableName").collect()
            //          }
            //          assert(
            //            e2.getCause.isInstanceOf[RuntimeException] &&
            //              e2.getMessage.contains(
            //                """Found duplicate field(s) b in case-insensitive mode"""))
          }

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            checkAnswer(sql(s"select a from $tableName"), (0 until end).map(_ => Row(null)))
            checkAnswer(sql(s"select b from $tableName"), data.select("b"))
          }
        }
    }
  }

  test("gluten SPARK-22790,SPARK-27668: spark.sql.sources.compressionFactor takes effect") {
    Seq(1.0, 0.5).foreach {
      compressionFactor =>
        withSQLConf(
          SQLConf.FILE_COMPRESSION_FACTOR.key -> compressionFactor.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "350") {
          withTempPath {
            workDir =>
              // the file size is 504 bytes
              val workDirPath = workDir.getAbsolutePath
              val data1 = Seq(100, 200, 300, 400).toDF("count")
              data1.write.orc(workDirPath + "/data1")
              val df1FromFile = spark.read.orc(workDirPath + "/data1")
              val data2 = Seq(100, 200, 300, 400).toDF("count")
              data2.write.orc(workDirPath + "/data2")
              val df2FromFile = spark.read.orc(workDirPath + "/data2")
              val joinedDF = df1FromFile.join(df2FromFile, Seq("count"))
              if (compressionFactor == 0.5) {
                val bJoinExec = collect(joinedDF.queryExecution.executedPlan) {
                  case bJoin: BroadcastHashJoinExec => bJoin
                }
                assert(bJoinExec.nonEmpty)
                val smJoinExec = collect(joinedDF.queryExecution.executedPlan) {
                  case smJoin: SortMergeJoinExec => smJoin
                }
                assert(smJoinExec.isEmpty)
              } else {
                // compressionFactor is 1.0
                val bJoinExec = collect(joinedDF.queryExecution.executedPlan) {
                  case bJoin: BroadcastHashJoinExec => bJoin
                }
                assert(bJoinExec.isEmpty)
                val smJoinExec = collect(joinedDF.queryExecution.executedPlan) {
                  case smJoin: SortMergeJoinExec => smJoin
                }
                assert(smJoinExec.nonEmpty)
              }
          }
        }
    }
  }

  test("gluten SPARK-25237 compute correct input metrics in FileScanRDD") {
    // TODO: Test CSV V2 as well after it implements [[SupportsReportStatistics]].
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "csv") {
      withTempPath {
        p =>
          val path = p.getAbsolutePath
          spark.range(1000).repartition(1).write.csv(path)
          val bytesReads = new mutable.ArrayBuffer[Long]()
          val bytesReadListener = new SparkListener() {
            override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
              bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
            }
          }
          sparkContext.addSparkListener(bytesReadListener)
          try {
            spark.read.csv(path).limit(1).collect()
            sparkContext.listenerBus.waitUntilEmpty()
            // plan is different, so metric is different
            assert(bytesReads.sum === 7864)
          } finally {
            sparkContext.removeSparkListener(bytesReadListener)
          }
      }
    }
  }

}
