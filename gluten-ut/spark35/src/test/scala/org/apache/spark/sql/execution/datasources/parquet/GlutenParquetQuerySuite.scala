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
import org.apache.spark.sql.internal.SQLConf

/** A test suite that tests various Parquet queries. */
class GlutenParquetV1QuerySuite extends ParquetV1QuerySuite with GlutenSQLTestsBaseTrait {
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key + "_DISABLED"
  override protected val vectorizedReaderNestedEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key + "_DISABLED"
  override def withAllParquetReaders(code: => Unit): Unit = {
    // test the row-based reader
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false")(code)
    // Disabled: We don't yet support this case as of now
    // test the vectorized reader
    // withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true")(code)
  }

  import testImplicits._

  test(
    GlutenTestConstants.GLUTEN_TEST +
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups") {
    withAllParquetReaders {
      withTempPath {
        path =>
          // Repeated values for dictionary encoding.
          Seq(Some("A"), Some("A"), None).toDF.repartition(1).write.parquet(path.getAbsolutePath)
          val df = spark.read.parquet(path.getAbsolutePath)
          checkAnswer(stripSparkFilter(df.where("NOT (value <=> 'A')")), Seq(null: String).toDF)
      }
    }
  }
}

class GlutenParquetV2QuerySuite extends ParquetV2QuerySuite with GlutenSQLTestsBaseTrait {
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key + "_DISABLED"
  override protected val vectorizedReaderNestedEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key + "_DISABLED"
  override def withAllParquetReaders(code: => Unit): Unit = {
    // test the row-based reader
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false")(code)
    // Disabled: We don't yet support this case as of now
    // test the vectorized reader
    //    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true")(code)
  }

  import testImplicits._

  test(
    GlutenTestConstants.GLUTEN_TEST +
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups") {
    withAllParquetReaders {
      withTempPath {
        path =>
          // Repeated values for dictionary encoding.
          Seq(Some("A"), Some("A"), None).toDF.repartition(1).write.parquet(path.getAbsolutePath)
          val df = spark.read.parquet(path.getAbsolutePath)
          checkAnswer(stripSparkFilter(df.where("NOT (value <=> 'A')")), Seq(null: String).toDF)
      }
    }
  }
}
