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

class VeloxWindowExpressionSuite extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
  }

  test("window row frame with mix preceding and following") {
    runQueryAndCompare(
      "select max(l_suppkey) over" +
        " (partition by l_suppkey order by l_orderkey " +
        "rows between 2 preceding and 1 preceding) from lineitem ") {
      checkOperatorMatch[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select max(l_suppkey) over" +
        " (partition by l_suppkey order by l_orderkey " +
        "rows between 2 following and 3 following) from lineitem ") {
      checkOperatorMatch[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select max(l_suppkey) over" +
        " (partition by l_suppkey order by l_orderkey " +
        "rows between -3 following and -2 following) from lineitem ") {
      checkOperatorMatch[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select max(l_suppkey) over" +
        " (partition by l_suppkey order by l_orderkey " +
        "rows between unbounded preceding and 3 following) from lineitem ") {
      checkOperatorMatch[WindowExecTransformer]
    }
  }
}
