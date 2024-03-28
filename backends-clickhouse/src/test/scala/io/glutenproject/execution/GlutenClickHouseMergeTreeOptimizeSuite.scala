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

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseMergeTreeOptimizeSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  protected lazy val sparkVersion: String = {
    val version = SPARK_VERSION_SHORT.split("\\.")
    version(0) + "." + version(1)
  }

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "error")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_config.user_defined_path",
        "/tmp/user_defined")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_settings.min_insert_block_size_rows",
        "10000"
      ) // so that we have enough parts to test
//      .set("spark.ui.enabled", "true")
//      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.dump_pipeline", "true")
//      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "debug")
      .set(
        "spark.databricks.delta.retentionDurationCheck.enabled",
        "false"
      ) // otherwise RETAIN 0 HOURS will fail
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  test("test mergetree optimize basic") {
    withSQLConf("spark.databricks.delta.optimize.maxFileSize" -> "2000000") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_optimize;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize
                   |USING clickhouse
                   |LOCATION '$basePath/lineitem_mergetree_optimize'
                   | as select * from lineitem
                   |""".stripMargin)

      spark.sql("optimize lineitem_mergetree_optimize")
      val ret = spark.sql("select count(*) from lineitem_mergetree_optimize").collect()
      assert(ret.apply(0).get(0) == 600572)

      spark.sql("optimize lineitem_mergetree_optimize")
      assert(
        countFiles(new File(s"$basePath/lineitem_mergetree_optimize")) == 462
      ) // many merged parts
    }
  }

  def countFiles(directory: File): Int = {
    if (directory.exists && directory.isDirectory) {
      val files = directory.listFiles
      val count = files
        .filter(!_.getName.endsWith(".crc"))
        .count(_.isFile) + files.filter(_.isDirectory).map(countFiles).sum
      count + 1
    } else {
      0
    }
  }

  test("test mergetree optimize partitioned, each partition too small to trigger optimize") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p
                 |USING clickhouse
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$basePath/lineitem_mergetree_optimize_p'
                 | as select * from lineitem
                 |""".stripMargin)

    spark.sparkContext.setJobGroup("test", "test")
    spark.sql("optimize lineitem_mergetree_optimize_p")
    val job_ids = spark.sparkContext.statusTracker.getJobIdsForGroup("test")
    assert(job_ids.size == 1) // will not trigger actual merge job
    spark.sparkContext.clearJobGroup()

    val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p").collect()
    assert(ret.apply(0).get(0) == 600572)

    spark.sql("set spark.gluten.enabled=false")
    assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p")) == 22728)
    spark.sql("VACUUM lineitem_mergetree_optimize_p RETAIN 0 HOURS")
    assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p")) == 22728)
    spark.sql("set spark.gluten.enabled=true")

    val ret2 = spark.sql("select count(*) from lineitem_mergetree_optimize_p").collect()
    assert(ret2.apply(0).get(0) == 600572)
  }

  test("test mergetree optimize partitioned by one low card column") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p2
                 |USING clickhouse
                 |PARTITIONED BY (l_returnflag)
                 |LOCATION '$basePath/lineitem_mergetree_optimize_p2'
                 | as select * from lineitem
                 |""".stripMargin)

    spark.sparkContext.setJobGroup("test2", "test2")
    spark.sql("optimize lineitem_mergetree_optimize_p2")
    val job_ids = spark.sparkContext.statusTracker.getJobIdsForGroup("test2")
    assert(job_ids.size == 7) // WILL trigger actual merge job
    spark.sparkContext.clearJobGroup()

    val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p2").collect()
    assert(ret.apply(0).get(0) == 600572)

    spark.sql("set spark.gluten.enabled=false")
    assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p2")) == 815)
    spark.sql("VACUUM lineitem_mergetree_optimize_p2 RETAIN 0 HOURS")
    assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p2")) == 232)
    spark.sql("VACUUM lineitem_mergetree_optimize_p2 RETAIN 0 HOURS")
    // the second VACUUM will remove some empty folders
    assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p2")) == 220)
    spark.sql("set spark.gluten.enabled=true")

    val ret2 = spark.sql("select count(*) from lineitem_mergetree_optimize_p2").collect()
    assert(ret2.apply(0).get(0) == 600572)
  }

  test("test mergetree optimize partitioned by two low card column") {
    withSQLConf("spark.databricks.delta.vacuum.parallelDelete.enabled" -> "false") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p3;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p3
                   |USING clickhouse
                   |PARTITIONED BY (l_linenumber,l_returnflag)
                   |LOCATION '$basePath/lineitem_mergetree_optimize_p3'
                   | as select * from lineitem
                   |""".stripMargin)

      spark.sql("optimize lineitem_mergetree_optimize_p3")
      val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p3").collect()
      assert(ret.apply(0).get(0) == 600572)

      spark.sql("set spark.gluten.enabled=false")
      assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p3")) == 411)
      spark.sql("VACUUM lineitem_mergetree_optimize_p3 RETAIN 0 HOURS")
      // for tables with more than one layer of nested table (like partition + bucket, or two partition col
      // the 'tmp_merge' folder is not guarantee to be removed, causing this file number to be unstable
//      assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p3")) == 290)
      assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p3")) > 270)
      spark.sql("VACUUM lineitem_mergetree_optimize_p3 RETAIN 0 HOURS")
      assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p3")) == 270)
      spark.sql("set spark.gluten.enabled=true")

      val ret2 = spark.sql("select count(*) from lineitem_mergetree_optimize_p3").collect()
      assert(ret2.apply(0).get(0) == 600572)
    }
  }

  test("test mergetree optimize parallel delete") {
    withSQLConf("spark.databricks.delta.vacuum.parallelDelete.enabled" -> "true") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p4;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p4
                   |USING clickhouse
                   |PARTITIONED BY (l_linenumber,l_returnflag)
                   |LOCATION '$basePath/lineitem_mergetree_optimize_p4'
                   | as select * from lineitem
                   |""".stripMargin)

      spark.sql("optimize lineitem_mergetree_optimize_p4")
      val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p4").collect()
      assert(ret.apply(0).get(0) == 600572)

      spark.sql("set spark.gluten.enabled=false")
      assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p4")) == 411)
      spark.sql("VACUUM lineitem_mergetree_optimize_p4 RETAIN 0 HOURS")
//      assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p4")) == 290)
      assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p4")) > 270)
      spark.sql("VACUUM lineitem_mergetree_optimize_p4 RETAIN 0 HOURS")
      assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p4")) == 270)
      spark.sql("set spark.gluten.enabled=true")

      val ret2 = spark.sql("select count(*) from lineitem_mergetree_optimize_p4").collect()
      assert(ret2.apply(0).get(0) == 600572)
    }
  }

  test("test mergetree optimize with optimize.minFileSize and optimize.maxFileSize") {
    withSQLConf("spark.databricks.delta.optimize.minFileSize" -> "838000") {
      // 3 from 37 parts are larger than this, so after optimize there should be 4 parts:
      // 3 original parts and 1 merged part
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p5;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p5
                   |USING clickhouse
                   |LOCATION '$basePath/lineitem_mergetree_optimize_p5'
                   | as select * from lineitem
                   |""".stripMargin)

      spark.sql("optimize lineitem_mergetree_optimize_p5")

      spark.sql("set spark.gluten.enabled=false")
      spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
      spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
      assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p5")) == 99)
      spark.sql("set spark.gluten.enabled=true")

      val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p5").collect()
      assert(ret.apply(0).get(0) == 600572)
    }

    withSQLConf(
      ("spark.databricks.delta.optimize.maxFileSize" -> "10000000"),
      ("spark.databricks.delta.optimize.minFileSize" -> "838250")) {
      // of the remaing 3 original parts, 2 are less than 838250, 1 is larger (size 838255)
      // the merged part is ~27MB, so after optimize there should be 3 parts:
      // 1 merged part from 2 original parts, 1 merged part from 34 original parts
      // and 1 original part (size 838255)

      spark.sql("optimize lineitem_mergetree_optimize_p5")

      spark.sql("set spark.gluten.enabled=false")
      spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
      spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
      assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p5")) == 93)
      spark.sql("set spark.gluten.enabled=true")

      val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p5").collect()
      assert(ret.apply(0).get(0) == 600572)
    }

    // now merge all parts (testing merging from merged parts)
    spark.sql("optimize lineitem_mergetree_optimize_p5")

    spark.sql("set spark.gluten.enabled=false")
    spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
    spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
    assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p5")) == 77)
    spark.sql("set spark.gluten.enabled=true")

    val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p5").collect()
    assert(ret.apply(0).get(0) == 600572)
  }

  test("test mergetree optimize table with partition and bucket") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p6;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p6
                 |USING clickhouse
                 |PARTITIONED BY (l_returnflag)
                 |LOCATION '$basePath/lineitem_mergetree_optimize_p6'
                 | CLUSTERED BY (l_partkey)
                 | ${if (sparkVersion.equals("3.2")) "" else "SORTED BY (l_partkey)"} INTO 2 BUCKETS
                 | as select * from lineitem
                 |""".stripMargin)

    spark.sql("optimize lineitem_mergetree_optimize_p6")

    val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p6").collect()
    assert(ret.apply(0).get(0) == 600572)

    spark.sql("set spark.gluten.enabled=false")
    assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p6")) == {
      if (sparkVersion.equals("3.2")) 940 else 1023
    })
    spark.sql("VACUUM lineitem_mergetree_optimize_p6 RETAIN 0 HOURS")
    spark.sql("VACUUM lineitem_mergetree_optimize_p6 RETAIN 0 HOURS")
    assert(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p6")) == {
      if (sparkVersion.equals("3.2")) 439 else 445
    })
    spark.sql("set spark.gluten.enabled=true")

    val ret2 = spark.sql("select count(*) from lineitem_mergetree_optimize_p6").collect()
    assert(ret2.apply(0).get(0) == 600572)
  }

}
// scalastyle:off line.size.limit
