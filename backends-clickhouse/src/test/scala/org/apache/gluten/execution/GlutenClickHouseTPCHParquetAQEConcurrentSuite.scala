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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.DoubleType

import java.util.concurrent.ForkJoinPool

import scala.collection.parallel.ForkJoinTaskSupport

class GlutenClickHouseTPCHParquetAQEConcurrentSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
  override protected def runTPCHQuery(
      queryNum: Int,
      tpchQueries: String,
      queriesResults: String,
      compareResult: Boolean = true,
      noFallBack: Boolean = true)(customCheck: DataFrame => Unit): Unit = {
    val sqlNum = "q" + "%02d".format(queryNum)
    withDataFrame(tpchSQL(queryNum, tpchQueries)) {
      df =>
        val result = df.collect()
        if (compareResult) {
          val schema = df.schema
          if (schema.exists(_.dataType == DoubleType)) {} else {
            compareResultStr(sqlNum, result, queriesResults)
          }
        } else {
          df.collect()
        }
        customCheck(df)
    }
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  test("fix race condition at the global variable of ColumnarOverrideRules::isAdaptiveContext") {

    val queries = ((1 to 22) ++ (1 to 22) ++ (1 to 22) ++ (1 to 22)).par
    queries.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(22))
    queries.map(queryId => runTPCHQuery(queryId) { df => })

  }

}
