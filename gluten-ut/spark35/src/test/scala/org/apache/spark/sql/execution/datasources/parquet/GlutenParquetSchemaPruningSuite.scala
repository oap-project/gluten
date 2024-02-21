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
import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.ExtendedSQLTest

@ExtendedSQLTest
class GlutenParquetV1SchemaPruningSuite
  extends ParquetV1SchemaPruningSuite
  with GlutenSQLTestsBaseTrait {
  // disable column reader for nested type
  override protected val vectorizedReaderNestedEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key + "_DISABLED"
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.memory.offHeap.size", "3g")
  }
}

@ExtendedSQLTest
class GlutenParquetV2SchemaPruningSuite
  extends ParquetV2SchemaPruningSuite
  with GlutenSQLTestsBaseTrait {
  override protected val vectorizedReaderNestedEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key + "_DISABLED"
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.memory.offHeap.size", "3g")
  }
}
