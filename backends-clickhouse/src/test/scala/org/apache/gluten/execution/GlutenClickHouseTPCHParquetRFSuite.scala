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

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf}

class GlutenClickHouseTPCHParquetRFSuite extends GlutenClickHouseTPCHSaltNullParquetSuite {

  protected lazy val sparkVersion: String = {
    val version = SPARK_VERSION_SHORT.split("\\.")
    version(0) + "." + version(1)
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // radically small threshold to force runtime bloom filter
      .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB")
      .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
  }

  test("GLUTEN-3779: Fix core dump when executing sql with runtime filter") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.files.maxPartitionBytes", "204800"),
      ("spark.sql.files.openCostInBytes", "102400")
    ) {
      compareResultsAgainstVanillaSpark(
        """
          |SELECT
          |    sum(l_extendedprice) / 7.0 AS avg_yearly
          |FROM
          |    lineitem,
          |    part
          |WHERE
          |    p_partkey = l_partkey
          |    AND p_size > 5
          |    AND l_quantity < (
          |        SELECT
          |            0.2 * avg(l_quantity)
          |        FROM
          |            lineitem
          |        WHERE
          |            l_partkey = p_partkey);
          |
          |""".stripMargin,
        compareResult = true,
        df => {
          if (sparkVersion.equals("3.3")) {
            val filterExecs = df.queryExecution.executedPlan.collect {
              case filter: FilterExecTransformerBase => filter
            }
            assert(filterExecs.size == 4)
            assert(
              filterExecs(0).asInstanceOf[FilterExecTransformer].toString.contains("might_contain"))
          }
        }
      )
    }
  }
}
