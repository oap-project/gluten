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
package io.glutenproject

import org.apache.spark.sql.SparkSession

import java.util.Scanner

import scala.util.control.Breaks.break

/**
 * LD_PRELOAD=/usr/local/clickhouse/lib/libch.so
 * /home/nemon/software/jdk-8u251-linux-x64/jdk1.8.0_251/jre/lib/amd64/libjsig.so
 */
object NemonConsoleTests {
  val libCH = "/usr/local/clickhouse/lib/libch.so"
  def main(args: Array[String]): Unit = {
    val spark = buildSpark(args)
    val scanner = new Scanner(System.in)
    val content = new StringBuilder()
    var oneLine = ""
    var start = true
    while (true) {
//      Thread.sleep(100)
      if (start) {
        System.out.print("sql/>")
        start = false
      }
      oneLine = scanner.nextLine().trim
      if ("!q".equalsIgnoreCase(oneLine)) {
        break
      } else if (oneLine.endsWith(";")) {
        start = true
        oneLine = oneLine.substring(0, oneLine.size - 1)
        if (oneLine.nonEmpty) {
          if (content.nonEmpty) {
            content.append("\n")
          }
          content.append(oneLine)
        }
        val sqlText = content.toString()
        content.clear()
        try {
          val start = System.currentTimeMillis()
          val plan = spark.sql(sqlText)
          val result = plan.collect()
          // scalastyle:off
          Thread.sleep(1000)
          println("---------------------start---------------------------")
          result.foreach(println(_))
          println("----------------------end--------------------------")
          val end = System.currentTimeMillis()

          System.out.println("taken time: " + (end - start) + " ms")
          // scalastyle:on
        } catch {
          case ex: Throwable => ex.printStackTrace(System.err)
        }
      } else {
        if (content.nonEmpty) {
          content.append("\n")
        }
        content.append(oneLine)
      }
    }
  }

  private def buildSpark(args: Array[String]): SparkSession = {
    val backendPrefix = "spark.gluten.sql.columnar.backend.ch.runtime_config.local_engine.settings"
    val spark = SparkSession
      .builder()
      .appName("Gazelle-Jni-Benchmark")
      .master("local[1]")
      .config(
        "spark.sql.warehouse.dir",
        "/home/nemon/software/spark-3.1.2-bin-hadoop3.2/spark-warehouse")
      .config(
        "javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=/home/nemon/software/" +
          s"spark-3.2.1-bin-hadoop3.2/metastore_db;create=true")
//      .config("spark.sql.files.maxPartitionBytes", "1073741824")
      .config("spark.gluten.enabled", "true")
//      .config("spark.gluten.ch.query.batch.size", "10")
      .config("spark.task.maxFailures", "1")
      .config("spark.plugins", "io.glutenproject.GlutenPlugin")
      .config("spark.gluten.sql.columnar.shuffle", true)
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "10000000000")
      .config("spark.sql.columnVector.offheap.enabled", "true")
      .config("spark.gluten.sql.columnar.columnartorow", "true")
      .config("spark.gluten.sql.columnar.loadnative", "true")
      .config("spark.gluten.sql.columnar.backend.lib", "ch")
      .config("spark.gluten.sql.columnar.libpath", libCH)
      .config(
        "spark.executorEnv.LD_PRELOAD",
        "/usr/local/clickhouse/lib/libch.so" +
          " /home/nemon/software/jdk-8u251-linux-x64/jdk1.8.0_251/jre/lib/amd64/libjsig.so")
      .config("spark.gluten.sql.columnar.loadarrow", "false")
      .config("spark.gluten.sql.enable.native.validation", "false")
      .config("spark.io.compression.codec", "LZ4")
      .config("spark.executor.heartbeatInterval", "1000s")
      .config("spark.network.timeout", "1200s")
      //      .config("spark.default.parallelism", "100")
      .config("spark.sql.shuffle.partitions", "3")
//      .config("spark.hadoop.fs.obs.access.key.backend", ak)
//      .config("spark.hadoop.fs.obs.secret.key.backend", sk)
      .config("spark.hadoop.fs.obs.endpoint", "obs.cn-north-7.ulanqab.huawei.com")
      .config("fs.obs.endpoint", "obs.cn-north-7.ulanqab.huawei.com")
      .config("spark.hadoop.fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem")
//      .config("fs.obs.access.key.backend", ak)
//      .config("fs.obs.secret.key.backend", sk)
//      .config("fs.obs.endpoint", "obs.cn-north-7.ulanqab.huawei.com")
      .config("spark.hadoop.hive.in.test", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", "false")
      .config("spark.sql.orc.enableVectorizedReader", "true")
      .config("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "trace")
      .config("spark.gluten.sql.columnar.backend.ch.runtime_config.logger", "ClickHouseBackend")
      .config("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.log", "/tmp/info.log")
      .config(
        "spark.gluten.sql.columnar.backend.ch.runtime_config.logger.errorlog",
        "/tmp/error.log")
//      .config("spark.gluten.sql.columnar.backend.ch.runtime_conf.logger.level", "information")
      .config("spark.gluten.sql.columnar.backend.ch.runtime_conf.logger.level", "debug")
//      .config("spark.sql.orc.impl", "hive")
      .config("spark.sql.orc.impl", "native")
//      .config("spark.gluten.sql.columnar.shuffledhashjoin", false)
//      .config("spark.gluten.sql.columnar.hashagg", false)
//      .config("spark.gluten.sql.columnar.sort", false)
//      .config("spark.gluten.sql.columnar.hashagg.enablefinal", false)
      .config("spark.gluten.sql.columnar.preferJavaReader", false)
//      .config("spark.sql.optimizer.excludedRules",
      //      "org.apache.spark.sql.catalyst.optimizer.ConstantFolding")
//            .config("spark.gluten.sql.columnar.filter", false)
//            .config("spark.gluten.sql.columnar.project", false)
      .config("spark.sql.adaptive.enabled", "false")
//      .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "3MB")
//      .config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "2MB")
//      .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "1MB")
//      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.sql.hive.convertMetastoreCtas", "false")
      .config(
        "metastore.task.threads.always",
        "org.apache.hadoop.hive.metastore.events.EventCleanerTask")
      .config("spark.gluten.sql.substrait.plan.logLevel", "debug")
      .config("spark.sql.legacy.castComplexTypesToString.enabled", "true")
      .config("spark.sql.legacy.doLooseUpcast", "true")
//      .config("spark.gluten.sql.columnar.shuffle" , false)
//      .config("spark.gluten.sql.columnar.sort", false)
//      .config("spark.gluten.sql.columnar.filter", false)
//      .config("spark.gluten.sql.columnar.project", false)
//      .config("spark.gluten.sql.columnar.hashagg", false)
//      .config("spark.gluten.sql.columnar.filescan", false)
//      .config("spark.sql.codegen.wholeStage", false)
      .config("spark.gluten.sql.columnar.sortMergeJoin", true)
//      .config(backendPrefix + ".sort_nulls_direction", "-1")
//      .config("spark.gluten.sql.columnar.backend.ch.
      //      runtime_config.tmp_path", "/home/nemon/tmp")
//      .config("spark.gluten.sql.columnar.backend.ch.
      //      runtime_config.use_current_directory_as_tmp", false)
      .config("spark.gluten.sql.columnar.forceShuffledHashJoin", false)
      .config("spark.task.maxFailures", 1)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
