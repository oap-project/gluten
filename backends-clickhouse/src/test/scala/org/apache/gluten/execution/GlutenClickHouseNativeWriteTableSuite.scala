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
package org.apache.gluten.execution

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.AllDataTypesWithComplexType.genTestData
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession

import org.scalatest.BeforeAndAfterAll

class GlutenClickHouseNativeWriteTableSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper
  with SharedSparkSession
  with BeforeAndAfterAll {

  private var _hiveSpark: SparkSession = _

  override protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1073741824")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.minPartitionNum", "1")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.gluten.sql.columnar.columnartorow", "true")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.clickHouseLibPath)
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      // TODO: support default ANSI policy
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set("spark.sql.warehouse.dir", getWarehouseDir)
      .setMaster("local[1]")
  }

  private def getWarehouseDir = {
    // test non-ascii path, by the way
    // scalastyle:off nonascii
    basePath + "/中文/spark-warehouse"
  }

  override protected def spark: SparkSession = _hiveSpark

  override protected def initializeSession(): Unit = {
    if (_hiveSpark == null) {
      val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"
      _hiveSpark = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .config(
          "javax.jdo.option.ConnectionURL",
          s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true")
        .getOrCreate()
    }
  }

  private val table_name_template = "hive_%s_test"
  private val table_name_vanilla_template = "hive_%s_test_written_by_vanilla"
  private val formats = Array("orc", "parquet")

  override protected def afterAll(): Unit = {
    DeltaLog.clearCache()

    try {
      super.afterAll()
    } finally {
      try {
        if (_hiveSpark != null) {
          try {
            _hiveSpark.sessionState.catalog.reset()
          } finally {
            _hiveSpark.stop()
            _hiveSpark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  def getColumnName(s: String): String = {
    s.replaceAll("\\(", "_").replaceAll("\\)", "_")
  }
  import collection.immutable.ListMap

  import java.io.File

  def writeIntoNewTableWithSql(table_name: String, table_create_sql: String)(
      fields: Seq[String]): Unit = {
    spark.sql(table_create_sql)
    spark.sql(
      s"insert overwrite $table_name select ${fields.mkString(",")}" +
        s" from origin_table")
  }

  def writeAndCheckRead(
      table_name: String,
      write: Seq[String] => Unit,
      fields: Seq[String]): Unit = {
    val originDF = spark.createDataFrame(genTestData())
    originDF.createOrReplaceTempView("origin_table")

    spark.sql(s"drop table IF EXISTS $table_name")

    val rowsFromOriginTable =
      spark.sql(s"select ${fields.mkString(",")} from origin_table").collect()
    // write them to parquet table
    write(fields)

    val dfFromWriteTable =
      spark.sql(
        s"select " +
          s"${fields
              .map(getColumnName)
              .mkString(",")} " +
          s"from $table_name")
    checkAnswer(dfFromWriteTable, rowsFromOriginTable)
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def getSignature(format: String, filesOfNativeWriter: Array[File]): Array[(Long, Long)] = {
    filesOfNativeWriter.map(
      f => {
        val df = if (format.equals("parquet")) {
          spark.read.parquet(f.getAbsolutePath)
        } else {
          spark.read.orc(f.getAbsolutePath)
        }
        (
          df.count(),
          df.agg(("int_field", "sum")).collect().apply(0).apply(0).asInstanceOf[Long]
        )
      })
  }

  test("test insert into dir") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      for (format <- formats) {
        spark.sql(
          s"insert overwrite local directory '$basePath/test_insert_into_${format}_dir1' "
            + s"stored as $format select "
            + fields.keys.mkString(",") +
            " from origin_table cluster by (byte_field)")
        spark.sql(
          s"insert overwrite local directory '$basePath/test_insert_into_${format}_dir2' " +
            s"stored as $format " +
            "select string_field, sum(int_field) as x from origin_table group by string_field")
      }
    }
  }

  test("test insert into partition") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      ("spark.sql.orc.compression.codec", "lz4"),
      ("spark.gluten.enabled", "true")) {

      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")

        val table_create_sql =
          s"create table if not exists $table_name (" +
            fields
              .map(f => s"${f._1} ${f._2}")
              .mkString(",") +
            " ) partitioned by (another_date_field date) " +
            s"stored as $format"

        spark.sql(table_create_sql)

        spark.sql(
          s"insert into $table_name partition(another_date_field = '2020-01-01') select "
            + fields.keys.mkString(",") +
            " from origin_table")

        var files = recursiveListFiles(new File(getWarehouseDir + "/" + table_name))
          .filter(_.getName.endsWith(s".$format"))
        if (format == "orc") {
          files = files.filter(_.getName.contains(".lz4"))
        }
        assert(files.length == 1)
        assert(files.head.getAbsolutePath.contains("another_date_field=2020-01-01"))
      }
    }
  }

  test("test CTAS") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")
      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")
        val table_create_sql =
          s"create table $table_name using $format as select " +
            fields
              .map(f => s"${f._1}")
              .mkString(",") +
            " from origin_table"
        spark.sql(table_create_sql)
        spark.sql(s"drop table IF EXISTS $table_name")

        try {
          val table_create_sql =
            s"create table $table_name as select " +
              fields
                .map(f => s"${f._1}")
                .mkString(",") +
              " from origin_table"
          spark.sql(table_create_sql)
        } catch {
          case _: UnsupportedOperationException => // expected
          case _: Exception => fail("should not throw exception")
        }
      }

    }
  }

  test("test insert into partition, bigo's case which incur InsertIntoHiveTable") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      ("spark.sql.hive.convertMetastoreParquet", "false"),
      ("spark.sql.hive.convertMetastoreOrc", "false"),
      ("spark.gluten.enabled", "true")
    ) {

      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")
      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")
        val table_create_sql = s"create table if not exists $table_name (" + fields
          .map(f => s"${f._1} ${f._2}")
          .mkString(",") + " ) partitioned by (another_date_field string)" +
          s"stored as $format"

        spark.sql(table_create_sql)
        spark.sql(
          s"insert overwrite table $table_name " +
            "partition(another_date_field = '2020-01-01') select "
            + fields.keys.mkString(",") + " from (select " + fields.keys.mkString(
              ",") + ", row_number() over (order by int_field desc) as rn  " +
            "from origin_table where float_field > 3 ) tt where rn <= 100")
        val files = recursiveListFiles(new File(getWarehouseDir + "/" + table_name))
          .filter(_.getName.startsWith("part"))
        assert(files.length == 1)
        assert(files.head.getAbsolutePath.contains("another_date_field=2020-01-01"))
      }
    }
  }

  test("test 1-col partitioned table") {

    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        val table_create_sql =
          s"create table if not exists $table_name (" +
            fields
              .filterNot(e => e._1.equals("date_field"))
              .map(f => s"${f._1} ${f._2}")
              .mkString(",") +
            " ) partitioned by (date_field date) " +
            s"stored as $format"

        writeAndCheckRead(
          table_name,
          writeIntoNewTableWithSql(table_name, table_create_sql),
          fields.keys.toSeq)
      }
    }
  }

  // even if disable native writer, this UT fail, spark bug???
  ignore("test 1-col partitioned table, partitioned by already ordered column") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "false")) {
      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )
      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        val table_create_sql =
          s"create table if not exists $table_name (" +
            fields
              .filterNot(e => e._1.equals("date_field"))
              .map(f => s"${f._1} ${f._2}")
              .mkString(",") +
            " ) partitioned by (date_field date) " +
            s"stored as $format"

        spark.sql(s"drop table IF EXISTS $table_name")
        spark.sql(table_create_sql)
        spark.sql(
          s"insert overwrite $table_name select ${fields.mkString(",")}" +
            s" from origin_table order by date_field")
      }
    }
  }

  test("test 2-col partitioned table") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date"),
        ("byte_field", "byte")
      )

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        val table_create_sql =
          s"create table if not exists $table_name (" +
            fields
              .filterNot(e => e._1.equals("date_field") || e._1.equals("byte_field"))
              .map(f => s"${f._1} ${f._2}")
              .mkString(",") + " ) partitioned by (date_field date, byte_field byte) " +
            s"stored as $format"

        writeAndCheckRead(
          table_name,
          writeIntoNewTableWithSql(table_name, table_create_sql),
          fields.keys.toSeq)
      }
    }
  }

  ignore(
    "test hive parquet/orc table, all types of columns being partitioned except the date_field," +
      " ignore because takes too long") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("date_field", "date"),
        ("timestamp_field", "timestamp"),
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)")
      )

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        for (field <- fields.filterNot(e => e._1.equals("date_field"))) {
          spark.sql(s"drop table if exists $table_name")
          val table_create_sql =
            s"create table if not exists $table_name (" +
              " date_field date" + " ) partitioned by (" +
              field._1 + " " + field._2 +
              ") " +
              s"stored as $format"

          writeAndCheckRead(
            table_name,
            writeIntoNewTableWithSql(table_name, table_create_sql),
            List("date_field", field._1))
        }
      }

    }
  }

  // This test case will be failed with incorrect result randomly, ignore first.
  ignore("test hive parquet/orc table, all columns being partitioned. ") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("date_field", "date"),
        ("timestamp_field", "timestamp"),
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)")
      )

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        val table_create_sql =
          s"create table if not exists $table_name (" +
            " date_field date" + " ) partitioned by (" +
            fields
              .filterNot(e => e._1.equals("date_field"))
              .map(f => s"${f._1} ${f._2}")
              .mkString(",") +
            ") " +
            s"stored as $format"

        writeAndCheckRead(
          table_name,
          writeIntoNewTableWithSql(table_name, table_create_sql),
          fields.keys.toSeq)
      }
    }
  }

  test(("test hive parquet/orc table with aggregated results")) {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("sum(int_field)", "bigint")
      )

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        val table_create_sql =
          s"create table if not exists $table_name (" +
            fields
              .map(f => s"${getColumnName(f._1)} ${f._2}")
              .mkString(",") +
            s" ) stored as $format"

        writeAndCheckRead(
          table_name,
          writeIntoNewTableWithSql(table_name, table_create_sql),
          fields.keys.toSeq)
      }
    }
  }

  test("test 1-col partitioned + 1-col bucketed table") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      for (format <- formats) {
        // spark write does not support bucketed table
        // https://issues.apache.org/jira/browse/SPARK-19256
        val table_name = table_name_template.format(format)
        writeAndCheckRead(
          table_name,
          fields => {
            spark
              .table("origin_table")
              .select(fields.head, fields.tail: _*)
              .write
              .format(format)
              .partitionBy("date_field")
              .bucketBy(2, "byte_field")
              .saveAsTable(table_name)
          },
          fields.keys.toSeq
        )

        assert(
          new File(getWarehouseDir + "/" + table_name)
            .listFiles()
            .filter(_.isDirectory)
            .filter(!_.getName.equals("date_field=__HIVE_DEFAULT_PARTITION__"))
            .head
            .listFiles()
            .filter(!_.isHidden)
            .length == 2
        ) // 2 bucket files
      }
    }
  }

  test("test table bucketed by all typed columns") {
    val fields: ListMap[String, String] = ListMap(
      ("string_field", "string"),
      ("int_field", "int"),
      ("long_field", "long"),
      ("float_field", "float"),
      ("double_field", "double"),
      ("short_field", "short"),
      ("byte_field", "byte"),
      ("boolean_field", "boolean"),
      ("decimal_field", "decimal(23,12)"),
      ("date_field", "date"),
      ("timestamp_field", "timestamp")
    )

    for (format <- formats) {
      val table_name = table_name_template.format(format)
      withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
        writeAndCheckRead(
          table_name,
          fields => {
            spark
              .table("origin_table")
              .select(fields.head, fields.tail: _*)
              .write
              .format(format)
              .bucketBy(10, fields.head, fields.tail: _*)
              .saveAsTable(table_name)
          },
          fields.keys.toSeq
        )
      }

      val table_name_vanilla = table_name_vanilla_template.format(format)
      withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
        spark.sql(s"drop table IF EXISTS $table_name_vanilla")
        spark
          .table("origin_table")
          .select(fields.keys.toSeq.head, fields.keys.toSeq.tail: _*)
          .write
          .format(format)
          .bucketBy(10, fields.keys.toSeq.head, fields.keys.toSeq.tail: _*)
          .saveAsTable(table_name_vanilla)

        val sigsOfNativeWriter =
          getSignature(
            format,
            recursiveListFiles(new File(getWarehouseDir + "/" + table_name))
              .filter(_.getName.endsWith(s".$format"))).sorted
        val sigsOfVanillaWriter =
          getSignature(
            format,
            recursiveListFiles(new File(getWarehouseDir + "/" + table_name_vanilla))
              .filter(_.getName.endsWith(s".$format"))).sorted

        assert(sigsOfVanillaWriter.sameElements(sigsOfNativeWriter))
      }
    }
  }

  test("test 1-col partitioned + 2-col bucketed table") {
    val fields: ListMap[String, String] = ListMap(
      ("string_field", "string"),
      ("int_field", "int"),
      ("long_field", "long"),
      ("float_field", "float"),
      ("double_field", "double"),
      ("short_field", "short"),
      ("byte_field", "byte"),
      ("boolean_field", "boolean"),
      ("decimal_field", "decimal(23,12)"),
      ("date_field", "date"),
      ("array", "array<int>"),
      ("map", "map<int, long>")
    )

    for (format <- formats) {
      val table_name = table_name_template.format(format)
      withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
        writeAndCheckRead(
          table_name,
          fields => {
            spark
              .table("origin_table")
              .select(fields.head, fields.tail: _*)
              .write
              .format(format)
              .partitionBy("date_field")
              .bucketBy(10, "byte_field", "string_field")
              .saveAsTable(table_name)
          },
          fields.keys.toSeq
        )
      }

      val table_name_vanilla = table_name_vanilla_template.format(format)
      withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
        spark.sql(s"drop table IF EXISTS $table_name_vanilla")
        spark
          .table("origin_table")
          .select(fields.keys.toSeq.head, fields.keys.toSeq.tail: _*)
          .write
          .format(format)
          .partitionBy("date_field")
          .bucketBy(10, "byte_field", "string_field")
          .saveAsTable(table_name_vanilla)

        val sigsOfNativeWriter =
          getSignature(
            format,
            recursiveListFiles(new File(getWarehouseDir + "/" + table_name))
              .filter(_.getName.endsWith(s".$format"))).sorted
        val sigsOfVanillaWriter =
          getSignature(
            format,
            recursiveListFiles(new File(getWarehouseDir + "/" + table_name_vanilla))
              .filter(_.getName.endsWith(s".$format"))).sorted

        assert(sigsOfVanillaWriter.sameElements(sigsOfNativeWriter))
      }
    }
  }

  test("test consecutive blocks having same partition value") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
      for (format <- formats) {
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")

        // 8096 row per block, so there will be blocks containing all 0 in p, all 1 in p
        spark
          .range(30000)
          .selectExpr("id", "id % 2 as p")
          .orderBy("p")
          .write
          .format(format)
          .partitionBy("p")
          .saveAsTable(table_name)

        val ret = spark.sql("select sum(id) from " + table_name).collect().apply(0).apply(0)
        assert(ret == 449985000)
      }
    }
  }

  test("test decimal with rand()") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
      for (format <- formats) {
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")
        spark
          .range(200)
          .selectExpr("id", " cast((id + rand()) as decimal(23,12)) as p")
          .write
          .format(format)
          .partitionBy("p")
          .saveAsTable(table_name)
        val ret = spark.sql("select max(p) from " + table_name).collect().apply(0).apply(0)
      }
    }
  }

  test("test partitioned by constant") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
      for (format <- formats) {
        spark.sql(s"drop table IF EXISTS tmp_123_$format")
        spark.sql(
          s"create table tmp_123_$format(" +
            s"x1 string, x2 bigint,x3 string, x4 bigint, x5 string )" +
            s"partitioned by (day date) stored as $format")

        spark.sql(
          s"insert into tmp_123_$format partition(day) " +
            "select cast(id as string), id, cast(id as string), id, cast(id as string), " +
            "'2023-05-09' from range(10000000)")
      }
    }
  }

  test("test bucketed by constant") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")

        spark
          .range(10000000)
          .selectExpr("id", "cast('2020-01-01' as date) as p")
          .write
          .format(format)
          .bucketBy(2, "p")
          .saveAsTable(table_name)

        val ret = spark.sql("select count(*) from " + table_name).collect().apply(0).apply(0)
      }
    }
  }

  test("test consecutive null values being partitioned") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")

        spark
          .range(30000)
          .selectExpr("id", "cast(null as string) as p")
          .write
          .format(format)
          .partitionBy("p")
          .saveAsTable(table_name)

        val ret = spark.sql("select count(*) from " + table_name).collect().apply(0).apply(0)
      }
    }
  }

  test("test consecutive null values being bucketed") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
      for (format <- formats) {
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")

        spark
          .range(30000)
          .selectExpr("id", "cast(null as string) as p")
          .write
          .format(format)
          .bucketBy(2, "p")
          .saveAsTable(table_name)

        val ret = spark.sql("select count(*) from " + table_name).collect().apply(0).apply(0)
      }
    }
  }

  test("test native write with empty dataset") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
      for (format <- formats) {
        val table_name = "t_" + format
        spark.sql(s"drop table IF EXISTS $table_name")
        spark.sql(s"create table $table_name (id int, str string) stored as $format")
        spark.sql(
          s"insert into $table_name select id, cast(id as string) from range(10)" +
            " where id > 100")
      }
    }
  }

  test("test native write with union") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
      for (format <- formats) {
        val table_name = "t_" + format
        spark.sql(s"drop table IF EXISTS $table_name")
        spark.sql(s"create table $table_name (id int, str string) stored as $format")
        spark.sql(
          s"insert overwrite table $table_name " +
            "select id, cast(id as string) from range(10) union all " +
            "select 10, '10' from range(10)")
        spark.sql(
          s"insert overwrite table $table_name " +
            "select id, cast(id as string) from range(10) union all " +
            "select 10, cast(id as string) from range(10)")

      }
    }
  }

  test("test native write and non-native read consistency") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
      for (format <- formats) {
        val table_name = "t_" + format
        spark.sql(s"drop table IF EXISTS $table_name")
        spark.sql(s"create table $table_name (id int, name string, info char(4)) stored as $format")
        spark.sql(
          s"insert overwrite table $table_name " +
            "select id, cast(id as string), concat('aaa', cast(id as string)) from range(10)")
        compareResultsAgainstVanillaSpark(
          s"select * from $table_name",
          compareResult = true,
          _ => {})
      }
    }
  }

  test("GLUTEN-4316: fix crash on dynamic partition inserting") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {
      formats.foreach(
        format => {
          val tbl = "t_" + format
          spark.sql(s"drop table IF EXISTS $tbl")
          val sql1 =
            s"create table $tbl(a int, b map<string, string>, c struct<d:string, e:string>) " +
              s"partitioned by (day string) stored as $format"
          val sql2 = s"insert overwrite $tbl partition (day) " +
            s"select id as a, str_to_map(concat('t1:','a','&t2:','b'),'&',':'), " +
            s"struct('1', null) as c, '2024-01-08' as day from range(10)"
          spark.sql(sql1)
          spark.sql(sql2)
        })
    }
  }

}
