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

import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts

import _root_.org.apache.commons.io.FileUtils
import _root_.org.apache.spark.sql.SaveMode
import _root_.org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import io.minio.{BucketExistsArgs, ListObjectsArgs, MakeBucketArgs, MinioClient, RemoveBucketArgs, RemoveObjectsArgs}
import io.minio.messages.DeleteObject

import java.io.File
import java.util

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseMergeTreeWriteOnS3Suite
  extends GlutenClickHouseMergeTreeWriteOnObjectStorageAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val client = MinioClient
      .builder()
      .endpoint(MINIO_ENDPOINT)
      .credentials(S3_ACCESS_KEY, S3_SECRET_KEY)
      .build()
    if (client.bucketExists(BucketExistsArgs.builder().bucket(BUCKET_NAME).build())) {
      val results =
        client.listObjects(ListObjectsArgs.builder().bucket(BUCKET_NAME).recursive(true).build())
      val objects = new util.LinkedList[DeleteObject]()
      results.forEach(
        obj => {
          objects.add(new DeleteObject(obj.get().objectName()))
        })
      val removeResults = client.removeObjects(
        RemoveObjectsArgs.builder().bucket(BUCKET_NAME).objects(objects).build())
      removeResults.forEach(result => result.get().message())
      client.removeBucket(RemoveBucketArgs.builder().bucket(BUCKET_NAME).build())
    }
    client.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build())
    FileUtils.deleteDirectory(new File(S3_METADATA_PATH))
    FileUtils.deleteDirectory(new File(S3_CACHE_PATH))
    FileUtils.forceMkdir(new File(S3_METADATA_PATH))
    FileUtils.forceMkdir(new File(S3_CACHE_PATH))
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteDirectory(new File(S3_METADATA_PATH))
    FileUtils.deleteDirectory(new File(S3_CACHE_PATH))
  }

  test("test mergetree table write") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_s3;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_s3
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
                 |LOCATION 's3a://$BUCKET_NAME/lineitem_mergetree_s3'
                 |TBLPROPERTIES (storage_policy='__s3_main')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_s3
                 | select * from lineitem
                 |""".stripMargin)
    FileUtils.deleteDirectory(new File(S3_METADATA_PATH))
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
         |    lineitem_mergetree_s3
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

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assert(addFiles.size == 1)
        assert(addFiles.head.rows == 600572)
    }
    spark.sql("drop table lineitem_mergetree_s3") // clean up
  }

  test("test mergetree write with orderby keys / primary keys") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_orderbykey_s3;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_orderbykey_s3
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
                 |TBLPROPERTIES (storage_policy='__s3_main',
                 |               orderByKey='l_shipdate,l_orderkey',
                 |               primaryKey='l_shipdate')
                 |LOCATION 's3a://$BUCKET_NAME/lineitem_mergetree_orderbykey_s3'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_orderbykey_s3
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
         |    lineitem_mergetree_orderbykey_s3
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

        val mergetreeScan = scanExec.head
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
        assert(addFiles.size == 1)
        assert(addFiles.head.rows == 600572)
    }
    spark.sql("drop table lineitem_mergetree_orderbykey_s3")
  }

  test("test mergetree write with partition") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_partition_s3;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_partition_s3
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
                 |PARTITIONED BY (l_returnflag)
                 |TBLPROPERTIES (storage_policy='__s3_main',
                 |               orderByKey='l_orderkey',
                 |               primaryKey='l_orderkey')
                 |LOCATION 's3a://$BUCKET_NAME/lineitem_mergetree_partition_s3'
                 |""".stripMargin)

    // dynamic partitions
    spark.sql(s"""
                 | insert into table lineitem_mergetree_partition_s3
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
      .insertInto("lineitem_mergetree_partition_s3")

    // static partition
    spark.sql(s"""
                 | insert into lineitem_mergetree_partition_s3 PARTITION (l_returnflag = 'A')
                 | (l_shipdate,
                 |  l_orderkey,
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
                 | select
                 |  l_shipdate,
                 |  l_orderkey,
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
                 |  where l_returnflag = 'A'
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
         |    lineitem_mergetree_partition_s3
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
        assert(result.length == 4)
        assert(result(0).getString(0).equals("A"))
        assert(result(0).getString(1).equals("F"))
        assert(result(0).getDouble(2) == 7578058.0)

        assert(result(2).getString(0).equals("N"))
        assert(result(2).getString(1).equals("O"))
        assert(result(2).getDouble(2) == 7454519.0)

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec.head
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))
        assert(mergetreeScan.metrics("numFiles").value == 6)

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
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size == 1)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns(0)
            .equals("l_returnflag"))
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assert(addFiles.size == 6)
        assert(addFiles.map(_.rows).sum == 750735)
    }
    spark.sql("drop table lineitem_mergetree_partition_s3")

  }

  test("test mergetree write with bucket table") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_bucket_s3;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_bucket_s3
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
                 |PARTITIONED BY (l_returnflag)
                 |CLUSTERED BY (l_orderkey)
                 |${if (sparkVersion.equals("3.2")) "" else "SORTED BY (l_orderkey)"} INTO 4 BUCKETS
                 |LOCATION 's3a://$BUCKET_NAME/lineitem_mergetree_bucket_s3'
                 |TBLPROPERTIES (storage_policy='__s3_main')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_bucket_s3
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
         |    lineitem_mergetree_bucket_s3
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
              .equals("l_orderkey"))
        }
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size == 1)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns(0)
            .equals("l_returnflag"))
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assert(addFiles.size == 12)
        assert(addFiles.map(_.rows).sum == 600572)
    }
    spark.sql("drop table lineitem_mergetree_bucket_s3")
  }

}
// scalastyle:off line.size.limit
