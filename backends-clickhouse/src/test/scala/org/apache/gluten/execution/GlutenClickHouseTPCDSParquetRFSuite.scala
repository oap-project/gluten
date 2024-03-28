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

import org.apache.spark.SparkConf

class GlutenClickHouseTPCDSParquetRFSuite extends GlutenClickHouseTPCDSAbstractSuite {

  override protected val tpcdsQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpcds-queries/tpcds.queries.original"
  override protected val queriesResults: String = rootPath + "tpcds-queries-output"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .setMaster(s"local[4]")
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.memory.offHeap.size", "8g")
      .set("spark.gluten.sql.validation.logLevel", "ERROR")
      .set("spark.gluten.sql.validation.printStackOnFailure", "true")
      // radically small threshold to force runtime bloom filter
      .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB")
      .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
  }

  executeTPCDSTest(false)
}

// scalastyle:on line.size.limit
