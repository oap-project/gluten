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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.TimestampTypes
import org.apache.spark.sql.types.{ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType}

import java.math.BigInteger
import java.sql.Timestamp
import java.time.LocalDateTime

class GlutenParquetV1PartitionDiscoverySuite
  extends ParquetV1PartitionDiscoverySuite
  with GlutenSQLTestsBaseTrait {
  test("gluten: Various partition value types") {
    Seq(TimestampTypes.TIMESTAMP_NTZ).foreach {
      tsType =>
        withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> tsType.toString) {
          val ts = if (tsType == TimestampTypes.TIMESTAMP_LTZ) {
            new Timestamp(0)
          } else {
            LocalDateTime.parse("1970-01-01T00:00:00")
          }
          val row =
            Row(
              100.toByte,
              40000.toShort,
              Int.MaxValue,
              Long.MaxValue,
              1.5.toFloat,
              4.5,
              new java.math.BigDecimal(new BigInteger("212500"), 5),
              new java.math.BigDecimal("2.125"),
              java.sql.Date.valueOf("2015-05-23"),
              ts,
              "This is a string, /[]?=:",
              "This is not a partition column"
            )

          // BooleanType is not supported yet
          val partitionColumnTypes =
            Seq(
              ByteType,
              ShortType,
              IntegerType,
              LongType,
              FloatType,
              DoubleType,
              DecimalType(10, 5),
              DecimalType.SYSTEM_DEFAULT,
              DateType,
              SQLConf.get.timestampType,
              StringType
            )

          val partitionColumns = partitionColumnTypes.zipWithIndex.map {
            case (t, index) => StructField(s"p_$index", t)
          }

          val schema = StructType(partitionColumns :+ StructField(s"i", StringType))
          val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

          withTempPath {
            dir =>
              df.write
                .format("parquet")
                .partitionBy(partitionColumns.map(_.name): _*)
                .save(dir.toString)
              val fields = schema.map(f => Column(f.name).cast(f.dataType))
              checkAnswer(spark.read.load(dir.toString).select(fields: _*), row)
          }

          withTempPath {
            dir =>
              df.write
                .option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
                .format("parquet")
                .partitionBy(partitionColumns.map(_.name): _*)
                .save(dir.toString)
              val fields = schema.map(f => Column(f.name).cast(f.dataType))
              checkAnswer(
                spark.read
                  .option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
                  .load(dir.toString)
                  .select(fields: _*),
                row)
          }
        }
    }
  }

  test("gluten: Various inferred partition value types") {
    Seq(TimestampTypes.TIMESTAMP_NTZ).foreach {
      tsType =>
        withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> tsType.toString) {
          val ts = if (tsType == TimestampTypes.TIMESTAMP_LTZ) {
            Timestamp.valueOf("1990-02-24 12:00:30")
          } else {
            LocalDateTime.parse("1990-02-24T12:00:30")
          }
          val row =
            Row(
              Long.MaxValue,
              4.5,
              new java.math.BigDecimal(new BigInteger("1" * 20)),
              java.sql.Date.valueOf("2015-05-23"),
              ts,
              "This is a string, /[]?=:",
              "This is not a partition column"
            )

          val partitionColumnTypes =
            Seq(
              LongType,
              DoubleType,
              DecimalType(20, 0),
              DateType,
              SQLConf.get.timestampType,
              StringType)

          val partitionColumns = partitionColumnTypes.zipWithIndex.map {
            case (t, index) => StructField(s"p_$index", t)
          }

          val schema = StructType(partitionColumns :+ StructField(s"i", StringType))
          val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

          withTempPath {
            dir =>
              df.write
                .format("parquet")
                .partitionBy(partitionColumns.map(_.name): _*)
                .save(dir.toString)
              val fields = schema.map(f => Column(f.name))
              checkAnswer(spark.read.load(dir.toString).select(fields: _*), row)
          }

          withTempPath {
            dir =>
              df.write
                .option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
                .format("parquet")
                .partitionBy(partitionColumns.map(_.name): _*)
                .save(dir.toString)
              val fields = schema.map(f => Column(f.name))
              checkAnswer(
                spark.read
                  .option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
                  .load(dir.toString)
                  .select(fields: _*),
                row)
          }
        }
    }
  }
}

class GlutenParquetV2PartitionDiscoverySuite
  extends ParquetV2PartitionDiscoverySuite
  with GlutenSQLTestsBaseTrait {
  test("gluten: Various partition value types") {
    Seq(TimestampTypes.TIMESTAMP_NTZ).foreach {
      tsType =>
        withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> tsType.toString) {
          val ts = if (tsType == TimestampTypes.TIMESTAMP_LTZ) {
            new Timestamp(0)
          } else {
            LocalDateTime.parse("1970-01-01T00:00:00")
          }
          val row =
            Row(
              100.toByte,
              40000.toShort,
              Int.MaxValue,
              Long.MaxValue,
              1.5.toFloat,
              4.5,
              new java.math.BigDecimal(new BigInteger("212500"), 5),
              new java.math.BigDecimal("2.125"),
              java.sql.Date.valueOf("2015-05-23"),
              ts,
              "This is a string, /[]?=:",
              "This is not a partition column"
            )

          // BooleanType is not supported yet
          val partitionColumnTypes =
            Seq(
              ByteType,
              ShortType,
              IntegerType,
              LongType,
              FloatType,
              DoubleType,
              DecimalType(10, 5),
              DecimalType.SYSTEM_DEFAULT,
              DateType,
              SQLConf.get.timestampType,
              StringType
            )

          val partitionColumns = partitionColumnTypes.zipWithIndex.map {
            case (t, index) => StructField(s"p_$index", t)
          }

          val schema = StructType(partitionColumns :+ StructField(s"i", StringType))
          val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

          withTempPath {
            dir =>
              df.write
                .format("parquet")
                .partitionBy(partitionColumns.map(_.name): _*)
                .save(dir.toString)
              val fields = schema.map(f => Column(f.name).cast(f.dataType))
              checkAnswer(spark.read.load(dir.toString).select(fields: _*), row)
          }

          withTempPath {
            dir =>
              df.write
                .option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
                .format("parquet")
                .partitionBy(partitionColumns.map(_.name): _*)
                .save(dir.toString)
              val fields = schema.map(f => Column(f.name).cast(f.dataType))
              checkAnswer(
                spark.read
                  .option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
                  .load(dir.toString)
                  .select(fields: _*),
                row)
          }
        }
    }
  }

  test("gluten: Various inferred partition value types") {
    Seq(TimestampTypes.TIMESTAMP_NTZ).foreach {
      tsType =>
        withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> tsType.toString) {
          val ts = if (tsType == TimestampTypes.TIMESTAMP_LTZ) {
            Timestamp.valueOf("1990-02-24 12:00:30")
          } else {
            LocalDateTime.parse("1990-02-24T12:00:30")
          }
          val row =
            Row(
              Long.MaxValue,
              4.5,
              new java.math.BigDecimal(new BigInteger("1" * 20)),
              java.sql.Date.valueOf("2015-05-23"),
              ts,
              "This is a string, /[]?=:",
              "This is not a partition column"
            )

          val partitionColumnTypes =
            Seq(
              LongType,
              DoubleType,
              DecimalType(20, 0),
              DateType,
              SQLConf.get.timestampType,
              StringType)

          val partitionColumns = partitionColumnTypes.zipWithIndex.map {
            case (t, index) => StructField(s"p_$index", t)
          }

          val schema = StructType(partitionColumns :+ StructField(s"i", StringType))
          val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

          withTempPath {
            dir =>
              df.write
                .format("parquet")
                .partitionBy(partitionColumns.map(_.name): _*)
                .save(dir.toString)
              val fields = schema.map(f => Column(f.name))
              checkAnswer(spark.read.load(dir.toString).select(fields: _*), row)
          }

          withTempPath {
            dir =>
              df.write
                .option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
                .format("parquet")
                .partitionBy(partitionColumns.map(_.name): _*)
                .save(dir.toString)
              val fields = schema.map(f => Column(f.name))
              checkAnswer(
                spark.read
                  .option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
                  .load(dir.toString)
                  .select(fields: _*),
                row)
          }
        }
    }
  }
}
