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
package org.apache.gluten.benchmarks

import scala.collection.mutable.ArrayBuffer

object GenTPCHTableScripts {

  def main(args: Array[String]): Unit = {
    // genTPCHCSV2ParquetSQL("/data/tpch-data/csv", "/data/tpch-data/parquet")
    genTPCHMergeTreeTables("/data/tpch-data/mergetree")
    genTPCHParquetTables("/data/tpch-data/parquet")
    genTPCHParquetBucketTables("/data/tpch-data/parquet-bucket")
  }

  def genTPCHParquetBucketTables(dataPathRoot: String): ArrayBuffer[String] = {
    // scalastyle:off println
    val dbName = "tpch100bucketdb"
    val sourceDbName = "default"

    val tablePrefix = "tpch100bucketdb."
    val tableSuffix = ""

    val notNullStr = " not null"

    val customerTbl = "customer"
    val lineitemTbl = "lineitem"
    val nationTbl = "nation"
    val regionTbl = "region"
    val ordersTbl = "orders"
    val partTbl = "part"
    val partsuppTbl = "partsupp"
    val supplierTbl = "supplier"
    // scalastyle:off println
    val res = new ArrayBuffer[String]()
    res +=
      s"""
         |CREATE DATABASE IF NOT EXISTS $dbName
         |WITH DBPROPERTIES (engine='Parquet');
         |""".stripMargin

    res += s"""use $dbName;"""

    // lineitem
    res +=
      s"""
         |DROP TABLE IF EXISTS $tablePrefix$lineitemTbl$tableSuffix;
         |""".stripMargin
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$lineitemTbl$tableSuffix
         | USING PARQUET
         | LOCATION 'file://${dataPathRoot + "/" + lineitemTbl}'
         | CLUSTERED BY (l_orderkey) SORTED BY (l_shipdate, l_orderkey) INTO 24 BUCKETS
         | AS SELECT /*+ REPARTITION(3) */ * FROM $sourceDbName.${lineitemTbl}100
         |""".stripMargin

    // order
    res +=
      s"""
         |DROP TABLE IF EXISTS $tablePrefix$ordersTbl$tableSuffix;
         |""".stripMargin
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$ordersTbl$tableSuffix
         | USING PARQUET
         | LOCATION 'file://${dataPathRoot + "/" + ordersTbl}'
         | CLUSTERED BY (o_orderkey) SORTED BY (o_orderkey, o_orderdate) INTO 24 BUCKETS
         | AS SELECT /*+ REPARTITION(2) */ * FROM $sourceDbName.${ordersTbl}100
         |""".stripMargin

    // customer
    res +=
      s"""
         |DROP TABLE IF EXISTS $tablePrefix$customerTbl$tableSuffix;
         |""".stripMargin
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$customerTbl$tableSuffix
         | USING PARQUET
         | LOCATION 'file://${dataPathRoot + "/" + customerTbl}'
         | CLUSTERED BY (c_custkey) SORTED BY (c_custkey) INTO 12 BUCKETS
         | AS SELECT /*+ REPARTITION(2) */ * FROM $sourceDbName.${customerTbl}100
         |""".stripMargin

    // part
    res +=
      s"""
         |DROP TABLE IF EXISTS $tablePrefix$partTbl$tableSuffix;
         |""".stripMargin
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$partTbl$tableSuffix
         | USING PARQUET
         | LOCATION 'file://${dataPathRoot + "/" + partTbl}'
         | CLUSTERED BY (p_partkey) SORTED BY (p_partkey) INTO 12 BUCKETS
         | AS SELECT /*+ REPARTITION(2) */ * FROM $sourceDbName.${partTbl}100
         |""".stripMargin

    // partsupp
    res +=
      s"""
         |DROP TABLE IF EXISTS $tablePrefix$partsuppTbl$tableSuffix;
         |""".stripMargin
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$partsuppTbl$tableSuffix
         | USING PARQUET
         | LOCATION 'file://${dataPathRoot + "/" + partsuppTbl}'
         | CLUSTERED BY (ps_partkey) SORTED BY (ps_partkey) INTO 12 BUCKETS
         | AS SELECT /*+ REPARTITION(2) */ * FROM $sourceDbName.${partsuppTbl}100
         |""".stripMargin

    // supplier
    res +=
      s"""
         |DROP TABLE IF EXISTS $tablePrefix$supplierTbl$tableSuffix;
         |""".stripMargin
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$supplierTbl$tableSuffix
         | USING PARQUET
         | LOCATION 'file://${dataPathRoot + "/" + supplierTbl}'
         | CLUSTERED BY (s_suppkey) SORTED BY (s_suppkey) INTO 2 BUCKETS
         | AS SELECT /*+ REPARTITION(1) */ * FROM $sourceDbName.${supplierTbl}100
         |""".stripMargin

    // nation
    res +=
      s"""
         |DROP TABLE IF EXISTS $tablePrefix$nationTbl$tableSuffix;
         |""".stripMargin
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$nationTbl$tableSuffix
         | USING PARQUET
         | LOCATION 'file://${dataPathRoot + "/" + nationTbl}'
         | AS SELECT /*+ REPARTITION(1) */ * FROM $sourceDbName.${nationTbl}100
         |""".stripMargin

    // region
    res +=
      s"""
         |DROP TABLE IF EXISTS $tablePrefix$regionTbl$tableSuffix;
         |""".stripMargin
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$regionTbl$tableSuffix
         | USING PARQUET
         | LOCATION 'file://${dataPathRoot + "/" + regionTbl}'
         | AS SELECT /*+ REPARTITION(1) */ * FROM $sourceDbName.${regionTbl}100
         |""".stripMargin
    // scalastyle:on println
    res
  }

  def genTPCHMergeTreeTables(dataPathRoot: String): ArrayBuffer[String] = {
    // scalastyle:off println
    val dbName = "default"

    val tablePrefix = "default."
    val tableSuffix = ""

    val notNullStr = " not null"

    val customerTbl = "customer"
    val lineitemTbl = "lineitem"
    val nationTbl = "nation"
    val regionTbl = "region"
    val ordersTbl = "orders"
    val partTbl = "part"
    val partsuppTbl = "partsupp"
    val supplierTbl = "supplier"

    val res = new ArrayBuffer[String]()

    res += s"""
              |CREATE DATABASE IF NOT EXISTS $dbName
              |WITH DBPROPERTIES (engine='MergeTree');
              |""".stripMargin
    res += s"""use $dbName;"""

    // customer
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$customerTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$customerTbl$tableSuffix (
              | c_custkey    bigint $notNullStr,
              | c_name       string $notNullStr,
              | c_address    string $notNullStr,
              | c_nationkey  bigint $notNullStr,
              | c_phone      string $notNullStr,
              | c_acctbal    double $notNullStr,
              | c_mktsegment string $notNullStr,
              | c_comment    string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION 'file://${dataPathRoot + "/" + customerTbl}';
              |""".stripMargin

    // lineitem
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$lineitemTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$lineitemTbl$tableSuffix (
              | l_orderkey      bigint $notNullStr,
              | l_partkey       bigint $notNullStr,
              | l_suppkey       bigint $notNullStr,
              | l_linenumber    bigint $notNullStr,
              | l_quantity      double $notNullStr,
              | l_extendedprice double $notNullStr,
              | l_discount      double $notNullStr,
              | l_tax           double $notNullStr,
              | l_returnflag    string $notNullStr,
              | l_linestatus    string $notNullStr,
              | l_shipdate      date $notNullStr,
              | l_commitdate    date $notNullStr,
              | l_receiptdate   date $notNullStr,
              | l_shipinstruct  string $notNullStr,
              | l_shipmode      string $notNullStr,
              | l_comment       string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION 'file://${dataPathRoot + "/" + lineitemTbl}';
              |""".stripMargin

    // nation
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$nationTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$nationTbl$tableSuffix (
              | n_nationkey bigint $notNullStr,
              | n_name      string $notNullStr,
              | n_regionkey bigint $notNullStr,
              | n_comment   string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION 'file://${dataPathRoot + "/" + nationTbl}';
              |""".stripMargin

    // region
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$regionTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$regionTbl$tableSuffix (
              | r_regionkey bigint $notNullStr,
              | r_name      string $notNullStr,
              | r_comment   string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION 'file://${dataPathRoot + "/" + regionTbl}';
              |""".stripMargin

    // orders
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$ordersTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$ordersTbl$tableSuffix (
              | o_orderkey      bigint $notNullStr,
              | o_custkey       bigint $notNullStr,
              | o_orderstatus   string $notNullStr,
              | o_totalprice    double $notNullStr,
              | o_orderdate     date $notNullStr,
              | o_orderpriority string $notNullStr,
              | o_clerk         string $notNullStr,
              | o_shippriority  bigint $notNullStr,
              | o_comment       string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION 'file://${dataPathRoot + "/" + ordersTbl}';
              |""".stripMargin

    // part
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$partTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$partTbl$tableSuffix (
              | p_partkey     bigint $notNullStr,
              | p_name        string $notNullStr,
              | p_mfgr        string $notNullStr,
              | p_brand       string $notNullStr,
              | p_type        string $notNullStr,
              | p_size        bigint $notNullStr,
              | p_container   string $notNullStr,
              | p_retailprice double $notNullStr,
              | p_comment     string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION 'file://${dataPathRoot + "/" + partTbl}';
              |""".stripMargin

    // partsupp
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$partsuppTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$partsuppTbl$tableSuffix (
              | ps_partkey    bigint $notNullStr,
              | ps_suppkey    bigint $notNullStr,
              | ps_availqty   bigint $notNullStr,
              | ps_supplycost double $notNullStr,
              | ps_comment    string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION 'file://${dataPathRoot + "/" + partsuppTbl}';
              |""".stripMargin

    // supplier
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$supplierTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$supplierTbl$tableSuffix (
              | s_suppkey   bigint $notNullStr,
              | s_name      string $notNullStr,
              | s_address   string $notNullStr,
              | s_nationkey bigint $notNullStr,
              | s_phone     string $notNullStr,
              | s_acctbal   double $notNullStr,
              | s_comment   string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION 'file://${dataPathRoot + "/" + supplierTbl}';
              |""".stripMargin
    // scalastyle:on println
    res
  }

  def genTPCHParquetTables(dataPathRoot: String): ArrayBuffer[String] = {
    // scalastyle:off println
    val dbName = "default"

    val tablePrefix = "default."
    val tableSuffix = ""

    val customerTbl = "customer"
    val lineitemTbl = "lineitem"
    val nationTbl = "nation"
    val regionTbl = "region"
    val ordersTbl = "orders"
    val partTbl = "part"
    val partsuppTbl = "partsupp"
    val supplierTbl = "supplier"

    val res = new ArrayBuffer[String]()

    res += s"""
              |CREATE DATABASE IF NOT EXISTS $dbName
              |WITH DBPROPERTIES (engine='Parquet');
              |""".stripMargin
    res += s"""use $dbName;"""

    // customer
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$customerTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$customerTbl$tableSuffix (
              | c_custkey    bigint,
              | c_name       string,
              | c_address    string,
              | c_nationkey  bigint,
              | c_phone      string,
              | c_acctbal    double,
              | c_mktsegment string,
              | c_comment    string)
              | USING PARQUET LOCATION 'file://${dataPathRoot + "/" + customerTbl}';
              |""".stripMargin

    // lineitem
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$lineitemTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$lineitemTbl$tableSuffix (
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
              | l_comment       string)
              | USING PARQUET LOCATION 'file://${dataPathRoot + "/" + lineitemTbl}';
              |""".stripMargin

    // nation
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$nationTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$nationTbl$tableSuffix (
              | n_nationkey bigint,
              | n_name      string,
              | n_regionkey bigint,
              | n_comment   string)
              | USING PARQUET LOCATION 'file://${dataPathRoot + "/" + nationTbl}';
              |""".stripMargin

    // region
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$regionTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$regionTbl$tableSuffix (
              | r_regionkey bigint,
              | r_name      string,
              | r_comment   string)
              | USING PARQUET LOCATION 'file://${dataPathRoot + "/" + regionTbl}';
              |""".stripMargin

    // orders
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$ordersTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$ordersTbl$tableSuffix (
              | o_orderkey      bigint,
              | o_custkey       bigint,
              | o_orderstatus   string,
              | o_totalprice    double,
              | o_orderdate     date,
              | o_orderpriority string,
              | o_clerk         string,
              | o_shippriority  bigint,
              | o_comment       string)
              | USING PARQUET LOCATION 'file://${dataPathRoot + "/" + ordersTbl}';
              |""".stripMargin

    // part
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$partTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$partTbl$tableSuffix (
              | p_partkey     bigint,
              | p_name        string,
              | p_mfgr        string,
              | p_brand       string,
              | p_type        string,
              | p_size        bigint,
              | p_container   string,
              | p_retailprice double,
              | p_comment     string)
              | USING PARQUET LOCATION 'file://${dataPathRoot + "/" + partTbl}';
              |""".stripMargin

    // partsupp
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$partsuppTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$partsuppTbl$tableSuffix (
              | ps_partkey    bigint,
              | ps_suppkey    bigint,
              | ps_availqty   bigint,
              | ps_supplycost double,
              | ps_comment    string)
              | USING PARQUET LOCATION 'file://${dataPathRoot + "/" + partsuppTbl}';
              |""".stripMargin

    // supplier
    res += s"""
              |DROP TABLE IF EXISTS $tablePrefix$supplierTbl$tableSuffix;
              |""".stripMargin
    res += s"""
              |CREATE EXTERNAL TABLE IF NOT EXISTS $tablePrefix$supplierTbl$tableSuffix (
              | s_suppkey   bigint,
              | s_name      string,
              | s_address   string,
              | s_nationkey bigint,
              | s_phone     string,
              | s_acctbal   double,
              | s_comment   string)
              | USING PARQUET LOCATION 'file://${dataPathRoot + "/" + supplierTbl}';
              |""".stripMargin
    // scalastyle:on println
    res
  }

  def genTPCHCSV2ParquetSQL(csvPathRoot: String, parquetPathRoot: String): ArrayBuffer[String] = {
    // scalastyle:off println
    val dbName = "gendb"

    val tablePrefix = "default."
    val tableSuffix = ""

    val customerTbl = "customer"
    val customerParts = "/*+ REPARTITION(24) */"
    val lineitemTbl = "lineitem"
    val lineitemParts = "/*+ REPARTITION(720) */"
    val nationTbl = "nation"
    val nationParts = ""
    val regionTbl = "region"
    val regionParts = ""
    val ordersTbl = "orders"
    val ordersParts = "/*+ REPARTITION(180) */"
    val partTbl = "part"
    val partParts = "/*+ REPARTITION(40) */"
    val partsuppTbl = "partsupp"
    val partsuppParts = "/*+ REPARTITION(120) */"
    val supplierTbl = "supplier"
    val supplierParts = "/*+ REPARTITION(2) */"

    val res = new ArrayBuffer[String]()

    res += s"""CREATE DATABASE IF NOT EXISTS $dbName;"""
    res += s"""use $dbName;"""

    // customer
    res += s"""DROP TABLE IF EXISTS ${customerTbl}_csv;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS ${customerTbl}_csv (
              | c_custkey    bigint,
              | c_name       string,
              | c_address    string,
              | c_nationkey  bigint,
              | c_phone      string,
              | c_acctbal    double,
              | c_mktsegment string,
              | c_comment    string)
              | USING csv
              | OPTIONS (
              |  path '${csvPathRoot + "/" + customerTbl + "/"}',
              |  header false,
              |  sep '|'
              | );
              |""".stripMargin
    res += s"""DROP TABLE IF EXISTS $tablePrefix$customerTbl$tableSuffix;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS $tablePrefix$customerTbl$tableSuffix
              | STORED AS PARQUET
              | LOCATION '${parquetPathRoot + "/" + customerTbl}'
              | AS SELECT $customerParts * FROM ${customerTbl}_csv;
              |""".stripMargin

    // lineitem
    res += s"""DROP TABLE IF EXISTS ${lineitemTbl}_csv;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS ${lineitemTbl}_csv (
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
              | l_comment       string)
              | USING csv
              | OPTIONS (
              |  path '${csvPathRoot + "/" + lineitemTbl + "/"}',
              |  header false,
              |  sep '|'
              | );
              |""".stripMargin
    res += s"""DROP TABLE IF EXISTS $tablePrefix$lineitemTbl$tableSuffix;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS $tablePrefix$lineitemTbl$tableSuffix
              | STORED AS PARQUET
              | LOCATION '${parquetPathRoot + "/" + lineitemTbl}'
              | AS SELECT $lineitemParts * FROM ${lineitemTbl}_csv;
              |""".stripMargin

    // nation
    res += s"""DROP TABLE IF EXISTS ${nationTbl}_csv;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS ${nationTbl}_csv (
              | n_nationkey bigint,
              | n_name      string,
              | n_regionkey bigint,
              | n_comment   string)
              | USING csv
              | OPTIONS (
              |  path '${csvPathRoot + "/" + nationTbl + "/"}',
              |  header false,
              |  sep '|'
              | );
              |""".stripMargin
    res += s"""DROP TABLE IF EXISTS $tablePrefix$nationTbl$tableSuffix;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS $tablePrefix$nationTbl$tableSuffix
              | STORED AS PARQUET
              | LOCATION '${parquetPathRoot + "/" + nationTbl}'
              | AS SELECT $nationParts * FROM ${nationTbl}_csv;
              |""".stripMargin

    // region
    res += s"""DROP TABLE IF EXISTS ${regionTbl}_csv;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS ${regionTbl}_csv (
              | r_regionkey bigint,
              | r_name      string,
              | r_comment   string)
              | USING csv
              | OPTIONS (
              |  path '${csvPathRoot + "/" + regionTbl + "/"}',
              |  header false,
              |  sep '|'
              | );
              |""".stripMargin
    res += s"""DROP TABLE IF EXISTS $tablePrefix$regionTbl$tableSuffix;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS $tablePrefix$regionTbl$tableSuffix
              | STORED AS PARQUET
              | LOCATION '${parquetPathRoot + "/" + regionTbl}'
              | AS SELECT $regionParts * FROM ${regionTbl}_csv;
              |""".stripMargin

    // orders
    res += s"""DROP TABLE IF EXISTS ${ordersTbl}_csv;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS ${ordersTbl}_csv (
              | o_orderkey      bigint,
              | o_custkey       bigint,
              | o_orderstatus   string,
              | o_totalprice    double,
              | o_orderdate     date,
              | o_orderpriority string,
              | o_clerk         string,
              | o_shippriority  bigint,
              | o_comment       string)
              | USING csv
              | OPTIONS (
              |  path '${csvPathRoot + "/" + ordersTbl + "/"}',
              |  header false,
              |  sep '|'
              | );
              |""".stripMargin
    res += s"""DROP TABLE IF EXISTS $tablePrefix$ordersTbl$tableSuffix;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS $tablePrefix$ordersTbl$tableSuffix
              | STORED AS PARQUET
              | LOCATION '${parquetPathRoot + "/" + ordersTbl}'
              | AS SELECT $ordersParts * FROM ${ordersTbl}_csv;
              |""".stripMargin

    // part
    res += s"""DROP TABLE IF EXISTS ${partTbl}_csv;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS ${partTbl}_csv (
              | p_partkey     bigint,
              | p_name        string,
              | p_mfgr        string,
              | p_brand       string,
              | p_type        string,
              | p_size        bigint,
              | p_container   string,
              | p_retailprice double,
              | p_comment     string)
              | USING csv
              | OPTIONS (
              |  path '${csvPathRoot + "/" + partTbl + "/"}',
              |  header false,
              |  sep '|'
              | );
              |""".stripMargin
    res += s"""DROP TABLE IF EXISTS $tablePrefix$partTbl$tableSuffix;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS $tablePrefix$partTbl$tableSuffix
              | STORED AS PARQUET
              | LOCATION '${parquetPathRoot + "/" + partTbl}'
              | AS SELECT $partParts * FROM ${partTbl}_csv;
              |""".stripMargin

    // partsupp
    res += s"""DROP TABLE IF EXISTS ${partsuppTbl}_csv;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS ${partsuppTbl}_csv (
              | ps_partkey    bigint,
              | ps_suppkey    bigint,
              | ps_availqty   bigint,
              | ps_supplycost double,
              | ps_comment    string)
              | USING csv
              | OPTIONS (
              |  path '${csvPathRoot + "/" + partsuppTbl + "/"}',
              |  header false,
              |  sep '|'
              | );
              |""".stripMargin
    res += s"""DROP TABLE IF EXISTS $tablePrefix$partsuppTbl$tableSuffix;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS $tablePrefix$partsuppTbl$tableSuffix
              | STORED AS PARQUET
              | LOCATION '${parquetPathRoot + "/" + partsuppTbl}'
              | AS SELECT $partsuppParts * FROM ${partsuppTbl}_csv;
              |""".stripMargin

    // supplier
    res += s"""DROP TABLE IF EXISTS ${supplierTbl}_csv;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS ${supplierTbl}_csv (
              | s_suppkey   bigint,
              | s_name      string,
              | s_address   string,
              | s_nationkey bigint,
              | s_phone     string,
              | s_acctbal   double,
              | s_comment   string)
              | USING csv
              | OPTIONS (
              |  path '${csvPathRoot + "/" + supplierTbl + "/"}',
              |  header false,
              |  sep '|'
              | );
              |""".stripMargin
    res += s"""DROP TABLE IF EXISTS $tablePrefix$supplierTbl$tableSuffix;"""
    res += s"""
              |CREATE TABLE IF NOT EXISTS $tablePrefix$supplierTbl$tableSuffix
              | STORED AS PARQUET
              | LOCATION '${parquetPathRoot + "/" + supplierTbl}'
              | AS SELECT $supplierParts * FROM ${supplierTbl}_csv;
              |""".stripMargin
    // scalastyle:on println
    res
  }

}
