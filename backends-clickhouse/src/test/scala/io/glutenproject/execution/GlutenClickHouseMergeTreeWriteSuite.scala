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
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts

import java.io.File

import scala.io.Source

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseMergeTreeWriteSuite
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
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set("spark.ui.enabled", "true")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_settings.min_insert_block_size_rows",
        "100000")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  test("test mergetree table write") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$basePath/lineitem_mergetree'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree
                 | select * from lineitem
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    lineitem_mergetree
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) {
      df =>
        val plans = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
          case w: WholeStageTransformer => w
        }
        assert(plans.size == 4)

        val mergetreeScan = plans(3).asInstanceOf[FileSourceScanExecTransformer]
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assert(addFiles.size == 6)
        assert(
          addFiles.map(_.rows).sum
            == 600572)

        // GLUTEN-5060: check the unnecessary FilterExec
        val wholeStageTransformer = plans(2).asInstanceOf[WholeStageTransformer]
        val planNodeJson = wholeStageTransformer.substraitPlanJson
        assert(
          !planNodeJson
            .replaceAll("\\\n", "")
            .replaceAll(" ", "")
            .contains("\"input\":{\"filter\":{"))
    }

  }

  test("test mergetree insert overwrite") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_insertoverwrite;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_insertoverwrite
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$basePath/lineitem_mergetree_insertoverwrite'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_insertoverwrite
                 | select * from lineitem
                 |""".stripMargin)

    spark.sql(s"""
                 | insert overwrite table lineitem_mergetree_insertoverwrite
                 | select * from lineitem where mod(l_orderkey,2) = 1
                 |""".stripMargin)
    val sql2 =
      s"""
         | select count(*) from lineitem_mergetree_insertoverwrite
         |
         |""".stripMargin
    assert(
      // total rows should remain unchanged
      spark.sql(sql2).collect().apply(0).get(0) == 300001
    )
  }

  test("test mergetree insert overwrite partitioned table with small table, static") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_insertoverwrite2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_insertoverwrite2
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$basePath/lineitem_mergetree_insertoverwrite2'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_insertoverwrite2
                 | select * from lineitem
                 |""".stripMargin)

    spark.sql(
      s"""
         | insert overwrite table lineitem_mergetree_insertoverwrite2
         | select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
         |""".stripMargin)
    val sql2 =
      s"""
         | select count(*) from lineitem_mergetree_insertoverwrite2
         |
         |""".stripMargin
    assert(
      // total rows should remain unchanged
      spark.sql(sql2).collect().apply(0).get(0) == 2418
    )
  }

  test("test mergetree insert overwrite partitioned table with small table, dynamic") {
    withSQLConf(("spark.sql.sources.partitionOverwriteMode", "dynamic")) {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_insertoverwrite3 PURGE;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_insertoverwrite3
                   |(
                   | l_orderkey      bigint,
                   | l_partkey       bigint,
                   | l_suppkey       bigint,
                   | l_linenumber    bigint,
                   | l_quantity      double,
                   | l_extendedprice double,
                   | l_discount      double,
                   | l_tax           double,
                   | l_returnflag    string,
                   | l_linestatus    string,
                   | l_shipdate      date,
                   | l_commitdate    date,
                   | l_receiptdate   date,
                   | l_shipinstruct  string,
                   | l_shipmode      string,
                   | l_comment       string
                   |)
                   |USING clickhouse
                   |PARTITIONED BY (l_shipdate)
                   |LOCATION '$basePath/lineitem_mergetree_insertoverwrite3'
                   |""".stripMargin)

      spark.sql(s"""
                   | insert into table lineitem_mergetree_insertoverwrite3
                   | select * from lineitem
                   |""".stripMargin)

      spark.sql(
        s"""
           | insert overwrite table lineitem_mergetree_insertoverwrite3
           | select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
           |""".stripMargin)
      val sql2 =
        s"""
           | select count(*) from lineitem_mergetree_insertoverwrite3
           |
           |""".stripMargin
      assert(
        // total rows should remain unchanged
        spark.sql(sql2).collect().apply(0).get(0) == 600572
      )
    }
  }

  test("test mergetree table update") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_update;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_update
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$basePath/lineitem_mergetree_update'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_update
                 | select * from lineitem
                 |""".stripMargin)

    spark.sql(s"""
                 | update lineitem_mergetree_update set l_returnflag = 'Z' where l_orderkey = 12647
                 |""".stripMargin)

    {
      val sql1 =
        s"""
           | select count(*) from lineitem_mergetree_update where l_returnflag = 'Z'
           |
           |""".stripMargin

      val df = spark.sql(sql1)
      val result = df.collect()
      assert(
        // in test data, there are only 1 row with l_orderkey = 12647
        result.apply(0).get(0) == 1
      )
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.size == 1)

      val mergetreeScan = scanExec.head
      assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

      val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
      assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
      assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
      assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKeyOption.isEmpty)
      assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.isEmpty)
      assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
      assert(
        addFiles.map(_.rows).sum
          == 600572)

      // 4 parts belong to the first batch
      // 2 parts belong to the second batch (1 actual updated part, 1 passively updated).
      assert(addFiles.size == 6)
      val filePaths = addFiles.map(_.path).groupBy(name => name.substring(0, name.lastIndexOf("_")))
      assert(filePaths.size == 2)
      assert(Array(2, 4).sameElements(filePaths.values.map(paths => paths.size).toArray.sorted))
    }

    val sql2 =
      s"""
         | select count(*) from lineitem_mergetree_update
         |
         |""".stripMargin
    assert(
      // total rows should remain unchanged
      spark.sql(sql2).collect().apply(0).get(0) == 600572
    )
  }

  test("test mergetree table delete") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_delete;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_delete
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$basePath/lineitem_mergetree_delete'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_delete
                 | select * from lineitem
                 |""".stripMargin)
    val df1 = spark.sql(s"""
                           | delete from lineitem_mergetree_delete where l_orderkey = 12647
                           |""".stripMargin)
//    assert(
//      df1.collect().apply(0).get(0) == 1
//    )

    {
      val df = spark.sql(s"""
                            | select count(*) from lineitem_mergetree_delete
                            |""".stripMargin)
      val result = df.collect()
      assert(
        result.apply(0).get(0) == 600571
      )
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      val mergetreeScan = scanExec.head
      val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
      // 4 parts belong to the first batch
      // 2 parts belong to the second batch (1 actual updated part, 1 passively updated).
      assert(addFiles.size == 6)
      val filePaths = addFiles.map(_.path).groupBy(name => name.substring(0, name.lastIndexOf("_")))
      assert(filePaths.size == 2)
      assert(Array(2, 4).sameElements(filePaths.values.map(paths => paths.size).toArray.sorted))
    }

    {
      spark.sql(s"""
                   | delete from lineitem_mergetree_delete where mod(l_orderkey, 3) = 2
                   |""".stripMargin)
      val df3 = spark.sql(s"""
                             | select count(*) from lineitem_mergetree_delete
                             |""".stripMargin)
      assert(
        df3.collect().apply(0).get(0) == 400089
      )
    }
  }

  test("test mergetree table upsert") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_upsert;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_upsert
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$basePath/lineitem_mergetree_upsert'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_upsert
                 | select * from lineitem
                 |""".stripMargin)

    {
      val df0 = spark.sql(s"""
                             | select count(*) from lineitem_mergetree_upsert
                             |""".stripMargin)
      assert(
        df0.collect().apply(0).get(0) == 600572
      )
    }

    upsertSourceTableAndCheck("lineitem_mergetree_upsert")
  }

  private def upsertSourceTableAndCheck(tableName: String) = {
    // Why selecting l_orderkey having count(*) =1 ?
    // Answer: to avoid "org.apache.spark.sql.delta.DeltaUnsupportedOperationException:
    // Cannot perform Merge as multiple source rows matched and attempted to modify the same
    // target row in the Delta table in possibly conflicting ways."
    spark.sql(s"""
          merge into $tableName
          using (

            select l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
           'Z' as `l_returnflag`,
            l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in (select l_orderkey from lineitem group by l_orderkey having count(*) =1 ) and l_orderkey < 100000

            union

            select l_orderkey + 10000000,
            l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,
            l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in (select l_orderkey from lineitem group by l_orderkey having count(*) =1 ) and l_orderkey < 100000

          ) as updates
          on updates.l_orderkey = $tableName.l_orderkey
          when matched then update set *
          when not matched then insert *
          """.stripMargin)

    {
      val df1 = spark.sql(s"""
                             | select count(*) from $tableName
                             |""".stripMargin)
      assert(
        df1.collect().apply(0).get(0) == 600572 + 3506
      )
    }
    {
      val df2 =
        spark.sql(s"""
                     | select count(*) from $tableName where l_returnflag = 'Z'
                     |""".stripMargin)
      assert(
        df2.collect().apply(0).get(0) == 3506
      )
    }

    {
      val df3 =
        spark.sql(s"""
                     | select count(*) from $tableName where l_orderkey > 10000000
                     |""".stripMargin)
      assert(
        df3.collect().apply(0).get(0) == 3506
      )
    }
  }

  test("test mergetree write with orderby keys / primary keys") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_orderbykey;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_orderbykey
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |TBLPROPERTIES (orderByKey='l_shipdate,l_orderkey',
                 |               primaryKey='l_shipdate')
                 |LOCATION '$basePath/lineitem_mergetree_orderbykey'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_orderbykey
                 | select * from lineitem
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    lineitem_mergetree_orderbykey
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(",")
            .equals("l_shipdate,l_orderkey"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKeyOption
            .get
            .mkString(",")
            .equals("l_shipdate"))
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assert(addFiles.size == 6)
        assert(addFiles.map(_.rows).sum == 600572)
    }
  }

  test("test mergetree write with partition") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_partition;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_partition
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |PARTITIONED BY (l_shipdate, l_returnflag)
                 |TBLPROPERTIES (orderByKey='l_orderkey',
                 |               primaryKey='l_orderkey')
                 |LOCATION '$basePath/lineitem_mergetree_partition'
                 |""".stripMargin)

    // dynamic partitions
    spark.sql(s"""
                 | insert into table lineitem_mergetree_partition
                 | select * from lineitem
                 |""".stripMargin)

    // write with dataframe api
    val source = spark.sql(s"""
                              |select
                              | l_orderkey      ,
                              | l_partkey       ,
                              | l_suppkey       ,
                              | l_linenumber    ,
                              | l_quantity      ,
                              | l_extendedprice ,
                              | l_discount      ,
                              | l_tax           ,
                              | l_returnflag    ,
                              | l_linestatus    ,
                              | l_shipdate      ,
                              | l_commitdate    ,
                              | l_receiptdate   ,
                              | l_shipinstruct  ,
                              | l_shipmode      ,
                              | l_comment
                              | from lineitem
                              | where l_shipdate BETWEEN date'1993-01-01' AND date'1993-01-10'
                              |""".stripMargin)

    source.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .insertInto("lineitem_mergetree_partition")

    // static partition
    spark.sql(s"""
                 | insert into lineitem_mergetree_partition PARTITION (l_shipdate=date'1995-01-21',
                 | l_returnflag = 'A')
                 | (l_orderkey,
                 |  l_partkey,
                 |  l_suppkey,
                 |  l_linenumber,
                 |  l_quantity,
                 |  l_extendedprice,
                 |  l_discount,
                 |  l_tax,
                 |  l_linestatus,
                 |  l_commitdate,
                 |  l_receiptdate,
                 |  l_shipinstruct,
                 |  l_shipmode,
                 |  l_comment)
                 | select l_orderkey,
                 |  l_partkey,
                 |  l_suppkey,
                 |  l_linenumber,
                 |  l_quantity,
                 |  l_extendedprice,
                 |  l_discount,
                 |  l_tax,
                 |  l_linestatus,
                 |  l_commitdate,
                 |  l_receiptdate,
                 |  l_shipinstruct,
                 |  l_shipmode,
                 |  l_comment from lineitem
                 |  where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
                 |""".stripMargin)
    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    lineitem_mergetree_partition
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr, compareResult = false) {
      df =>
        val result = df.collect()
        assert(result.size == 4)
        assert(result(0).getString(0).equals("A"))
        assert(result(0).getString(1).equals("F"))
        assert(result(0).getDouble(2) == 3865234.0)

        assert(result(2).getString(0).equals("N"))
        assert(result(2).getString(1).equals("O"))
        assert(result(2).getDouble(2) == 7454519.0)

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))
        assert(mergetreeScan.metrics("numFiles").value == 3745)

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(",")
            .equals("l_orderkey"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKeyOption
            .get
            .mkString(",")
            .equals("l_orderkey"))
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size == 2)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns(0)
            .equals("l_shipdate"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns(1)
            .equals("l_returnflag"))
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assert(addFiles.size == 3836)
        assert(addFiles.map(_.rows).sum == 605363)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1992-06-01")).size == 2)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1993-01-01")).size == 4)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1995-01-21")).size == 3)
    }
  }

  test("test mergetree write with bucket table") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_bucket;
                 |""".stripMargin)

    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS lineitem_mergetree_bucket
         |(
         | l_orderkey      bigint,
         | l_partkey       bigint,
         | l_suppkey       bigint,
         | l_linenumber    bigint,
         | l_quantity      double,
         | l_extendedprice double,
         | l_discount      double,
         | l_tax           double,
         | l_returnflag    string,
         | l_linestatus    string,
         | l_shipdate      date,
         | l_commitdate    date,
         | l_receiptdate   date,
         | l_shipinstruct  string,
         | l_shipmode      string,
         | l_comment       string
         |)
         |USING clickhouse
         |PARTITIONED BY (l_shipdate)
         |CLUSTERED BY (l_orderkey)
         |${if (sparkVersion.equals("3.2")) "" else "SORTED BY (l_orderkey, l_returnflag)"} INTO 4 BUCKETS
         |LOCATION '$basePath/lineitem_mergetree_bucket'
         |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_bucket
                 | select * from lineitem
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    lineitem_mergetree_bucket
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(!ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        if (sparkVersion.equals("3.2")) {
          assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKeyOption.isEmpty)
        } else {
          assert(
            ClickHouseTableV2
              .getTable(fileIndex.deltaLog)
              .orderByKeyOption
              .get
              .mkString(",")
              .equals("l_orderkey,l_returnflag"))
        }
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size == 1)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns(0)
            .equals("l_shipdate"))
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assert(addFiles.size == 10089)
        assert(addFiles.map(_.rows).sum == 600572)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1992-06-01")).size == 4)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1993-01-01")).size == 4)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1995-01-21")).size == 4)
        assert(
          addFiles
            .filter(
              f =>
                f.partitionValues.get("l_shipdate").get.equals("1995-01-21") && f.bucketNum.equals(
                  "00000"))
            .size == 1)
    }
    // check part pruning effect of filter on bucket column
    val df = spark.sql(s"""
                          | select * from lineitem_mergetree_bucket where l_orderkey = 12647
                          | and l_shipdate = date'1997-06-02'
                          |""".stripMargin)
    df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val touchedParts = scanExec.head.getPartitions
      .flatMap(partition => partition.asInstanceOf[GlutenMergeTreePartition].partList)
      .map(_.name)
      .distinct
    assert(touchedParts.size == 1)

    // test upsert on partitioned & bucketed table
    upsertSourceTableAndCheck("lineitem_mergetree_bucket")

    // test insert overwrite on partitioned & bucketed table
    spark.sql("create table lineitem_3_rows like lineitem")
    spark.sql("insert into table lineitem_3_rows select * from lineitem where l_orderkey = 12643")
    spark.sql("insert overwrite table lineitem_mergetree_bucket select * from lineitem_3_rows")
    val df0 = spark.sql(s"""
                           | select count(*) from lineitem_mergetree_bucket
                           |""".stripMargin)
    assert(
      df0.collect().apply(0).get(0) == 3
    )

  }

  test("GLUTEN-4749: Support to purge mergetree data for CH backend") {
    def createAndDropTable(
        tableName: String,
        tableLocation: String,
        isExternal: Boolean = false,
        purgeTable: Boolean = false): Unit = {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS $tableName;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE ${if (isExternal) "EXTERNAL" else ""} TABLE IF NOT EXISTS $tableName
                   |(
                   | l_orderkey      bigint,
                   | l_partkey       bigint,
                   | l_suppkey       bigint,
                   | l_linenumber    bigint,
                   | l_quantity      double,
                   | l_extendedprice double,
                   | l_discount      double,
                   | l_tax           double,
                   | l_returnflag    string,
                   | l_linestatus    string,
                   | l_shipdate      date,
                   | l_commitdate    date,
                   | l_receiptdate   date,
                   | l_shipinstruct  string,
                   | l_shipmode      string,
                   | l_comment       string
                   |)
                   |USING clickhouse
                   |TBLPROPERTIES (orderByKey='l_shipdate,l_orderkey',
                   |               primaryKey='l_shipdate')
                   |${if (tableLocation.nonEmpty) "LOCATION '" + tableLocation + "'" else ""}
                   |""".stripMargin)

      spark.sql(s"""
                   | insert into table $tableName
                   | select * from lineitem
                   |""".stripMargin)

      spark.sql(s"""
                   |DROP TABLE IF EXISTS $tableName ${if (purgeTable) "PURGE" else ""};
                   |""".stripMargin)
    }

    def checkTableExists(
        tableName: String,
        tableLocation: String,
        exceptedExists: Boolean): Unit = {
      val tableList = spark
        .sql(s"""
                |show tables;
                |""".stripMargin)
        .collect()
      assert(!tableList.exists(_.getString(1).equals(tableName)))

      val deletedPathStr = if (tableLocation.nonEmpty) {
        tableLocation
      } else {
        warehouse + "/" + tableName
      }
      val deletedPath = new File(deletedPathStr)
      assert(deletedPath.exists() == exceptedExists)
    }

    // test non external table
    var tableName = "lineitem_mergetree_drop"
    var tableLocation = ""
    createAndDropTable(tableName, tableLocation)
    checkTableExists(tableName, tableLocation, false)

    // test external table
    tableName = "lineitem_mergetree_external_drop"
    createAndDropTable(tableName, tableLocation, true)
    checkTableExists(tableName, tableLocation, false)

    // test table with the specified location
    tableName = "lineitem_mergetree_location_drop"
    tableLocation = basePath + "/" + tableName
    createAndDropTable(tableName, tableLocation)
    checkTableExists(tableName, tableLocation, true)

    tableName = "lineitem_mergetree_external_location_drop"
    tableLocation = basePath + "/" + tableName
    createAndDropTable(tableName, tableLocation, true)
    checkTableExists(tableName, tableLocation, true)

    tableName = "lineitem_mergetree_location_purge"
    tableLocation = basePath + "/" + tableName
    createAndDropTable(tableName, tableLocation, purgeTable = true)
    checkTableExists(tableName, tableLocation, false)

    tableName = "lineitem_mergetree_external_location_purge"
    tableLocation = basePath + "/" + tableName
    createAndDropTable(tableName, tableLocation, true, true)
    checkTableExists(tableName, tableLocation, false)
  }

  test("test mergetree CTAS simple") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_ctas1;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE lineitem_mergetree_ctas1
                 |USING clickhouse
                 |LOCATION '$basePath/lineitem_mergetree_ctas1'
                 | as select * from lineitem
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    lineitem_mergetree_ctas1
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assert(addFiles.size == 6)
        assert(
          addFiles.map(_.rows).sum
            == 600572)
    }
  }

  test("test mergetree CTAS complex") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_ctas2;
                 |""".stripMargin)

    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS lineitem_mergetree_ctas2
         |USING clickhouse
         |PARTITIONED BY (l_shipdate)
         |CLUSTERED BY (l_orderkey)
         |${if (sparkVersion.equals("3.2")) "" else "SORTED BY (l_orderkey, l_returnflag)"} INTO 4 BUCKETS
         |LOCATION '$basePath/lineitem_mergetree_ctas2'
         | as select * from lineitem
         |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    lineitem_mergetree_ctas2
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) { _ => {} }

  }

  test("test mergetree table with low cardinality column") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_lowcard;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_lowcard
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$basePath/lineitem_mergetree_lowcard'
                 |TBLPROPERTIES('lowCardKey'='l_returnflag,L_LINESTATUS,l_quantity')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_lowcard
                 | select * from lineitem
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    lineitem_mergetree_lowcard
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) { _ => {} }
    val directory = new File(s"$basePath/lineitem_mergetree_lowcard")
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    val columnsFile = new File(partDir, "columns.txt")
    val columns = Source.fromFile(columnsFile).getLines().mkString
    assert(columns.contains("`l_returnflag` LowCardinality(Nullable(String))"))
    assert(columns.contains("`l_linestatus` LowCardinality(Nullable(String))"))

    // test low card column in measure
    val sqlStr2 =
      s"""
         |SELECT
         |  max(l_returnflag), min(l_quantity)
         |FROM
         |    lineitem_mergetree_lowcard
         |GROUP BY
         |    l_linestatus
         |  order by l_linestatus
         |
         |""".stripMargin

    assert(
      // total rows should remain unchanged
      spark.sql(sqlStr2).collect().apply(0).get(0) == "R"
    )

    // test select *
    val sqlStr3 =
      s"""
         |SELECT
         |  *
         |FROM
         |    lineitem_mergetree_lowcard limit 1
         |
         |""".stripMargin
    spark.sql(sqlStr3).collect()
  }

  test("test mergetree with primary keys filter") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_orderbykey2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_orderbykey2
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |TBLPROPERTIES (orderByKey='l_shipdate,l_orderkey',
                 |               primaryKey='l_shipdate')
                 |LOCATION '$basePath/lineitem_mergetree_orderbykey2'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_orderbykey2
                 | select * from lineitem
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    sum(l_extendedprice * l_discount) AS revenue
         |FROM
         |    lineitem_mergetree_orderbykey2
         |WHERE
         |    l_shipdate >= date'1994-01-01'
         |    AND l_shipdate < date'1994-01-01' + interval 1 year
         |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
         |    AND l_quantity < 24
         |""".stripMargin
    runTPCHQueryBySQL(6, sqlStr) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(",")
            .equals("l_shipdate,l_orderkey"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKeyOption
            .get
            .mkString(",")
            .equals("l_shipdate"))
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assert(addFiles.size == 6)
        assert(addFiles.map(_.rows).sum == 600572)

        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(plans.size == 1)
        assert(plans(0).metrics("selectedMarksPk").value === 17)
        assert(plans(0).metrics("totalMarksPk").value === 74)
    }
  }

  test(
    "GLUTEN-5061: Fix assert error when writing mergetree data with select * from table limit n") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_5061;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_5061
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$basePath/lineitem_mergetree_5061'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_5061
                 | select * from lineitem limit 10
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    count(1)
         |FROM
         |    lineitem_mergetree_5061
         |""".stripMargin
    runSql(sqlStr)(
      df => {
        val result = df.collect()
        assert(result.size == 1)
        assert(result(0).getLong(0) == 10)

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assert(addFiles.size == 1)
        assert(addFiles(0).rows == 10)
      })
  }

  test("GLUTEN-5062: Add a UT to ensure that IN filtering can apply on CH primary key") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_5062;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_5062
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |TBLPROPERTIES (orderByKey='l_returnflag,l_shipdate',
                 |               primaryKey='l_returnflag,l_shipdate')
                 |LOCATION '$basePath/lineitem_mergetree_5062'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_5062
                 | select * from lineitem
                 |""".stripMargin)

    def checkSelectedMarksCnt(df: DataFrame, exceptedCnt: Long): Unit = {
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.size == 1)

      val mergetreeScan = scanExec(0)
      assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

      val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
      assert(
        (addFiles.map(_.marks).sum - addFiles.size) == mergetreeScan.metrics("totalMarksPk").value)
      assert(mergetreeScan.metrics("selectedMarksPk").value == exceptedCnt)
    }

    val sqlStr1 =
      s"""
         |SELECT
         |    sum(l_extendedprice)
         |FROM
         |    lineitem_mergetree_5062
         |WHERE
         |    l_shipdate in (date'1998-08-15', date'1993-12-05', date'1993-03-01')
         |""".stripMargin
    runSql(sqlStr1)(
      df => {
        val result = df.collect()
        assert(result.size == 1)
        assert(result(0).getDouble(0).toString.substring(0, 6).equals("2.6480"))

        checkSelectedMarksCnt(df, 34)
      })

    val sqlStr2 =
      s"""
         |SELECT
         |    sum(l_extendedprice)
         |FROM
         |    lineitem_mergetree_5062
         |WHERE
         |    l_returnflag not in ('N', 'A')
         |""".stripMargin
    runSql(sqlStr2)(
      df => {
        val result = df.collect()
        assert(result.size == 1)
        assert(result(0).getDouble(0).toString.substring(0, 6).equals("5.3379"))

        checkSelectedMarksCnt(df, 29)
      })
  }
}
// scalastyle:off line.size.limit
