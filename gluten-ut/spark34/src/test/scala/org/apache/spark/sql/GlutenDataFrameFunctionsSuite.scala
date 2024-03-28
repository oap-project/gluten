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
package org.apache.spark.sql

import org.apache.spark.sql.functions.{lit, map_concat}
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructField, StructType}

class GlutenDataFrameFunctionsSuite extends DataFrameFunctionsSuite with GlutenSQLTestsTrait {

  testGluten("map_concat function") {
    import testImplicits._
    val df1 = Seq(
      (Map[Int, Int](1 -> 100, 2 -> 200), Map[Int, Int](3 -> 300, 4 -> 400)),
      (Map[Int, Int](1 -> 100, 2 -> 200), Map[Int, Int](3 -> 300, 1 -> 400)),
      (null, Map[Int, Int](3 -> 300, 4 -> 400))
    ).toDF("map1", "map2")

    val expected1a = Seq(
      Row(Map(1 -> 100, 2 -> 200, 3 -> 300, 4 -> 400)),
      Row(Map(1 -> 400, 2 -> 200, 3 -> 300)),
      Row(null)
    )

    // Velox by default handles duplicate values and behavior is same as SQLConf.MapKeyDedupPolicy.LAST_WIN
    checkAnswer(df1.selectExpr("map_concat(map1, map2)"), expected1a)
    checkAnswer(df1.select(map_concat($"map1", $"map2")), expected1a)

    // map_concat arguments should be >= 2 in Velox
    intercept[Exception](df1.selectExpr("map_concat(map1)").collect())
    intercept[Exception](df1.select(map_concat($"map1")).collect())

    val df2 = Seq(
      (
        Map[Array[Int], Int](Array(1) -> 100, Array(2) -> 200),
        Map[String, Int]("3" -> 300, "4" -> 400)
      )
    ).toDF("map1", "map2")

    val expected2 = Seq(Row(Map()))

    // map_concat with 0 arguments falls back to spark in validation
    checkAnswer(df2.selectExpr("map_concat()"), expected2)
    checkAnswer(df2.select(map_concat()), expected2)

    val df3 = {
      val schema = StructType(
        StructField("map1", MapType(StringType, IntegerType, true), false)  ::
          StructField("map2", MapType(StringType, IntegerType, false), false) :: Nil
      )
      val data = Seq(
        Row(Map[String, Any]("a" -> 1, "b" -> null), Map[String, Any]("c" -> 3, "d" -> 4)),
        Row(Map[String, Any]("a" -> 1, "b" -> 2), Map[String, Any]("c" -> 3, "d" -> 4))
      )
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }

    val expected3 = Seq(
      Row(Map[String, Any]("a" -> 1, "b" -> null, "c" -> 3, "d" -> 4)),
      Row(Map[String, Any]("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4))
    )

    checkAnswer(df3.selectExpr("map_concat(map1, map2)"), expected3)
    checkAnswer(df3.select(map_concat($"map1", $"map2")), expected3)

    // Data type mismatch is handled by spark in analyze phase
    checkError(
      exception = intercept[AnalysisException] {
        df2.selectExpr("map_concat(map1, map2)").collect()
      },
      errorClass = "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
      sqlState = None,
      parameters = Map(
        "sqlExpr" -> "\"map_concat(map1, map2)\"",
        "dataType" -> "(\"MAP<ARRAY<INT>, INT>\" or \"MAP<STRING, INT>\")",
        "functionName" -> "`map_concat`"),
      context = ExpectedContext(
        fragment = "map_concat(map1, map2)",
        start = 0,
        stop = 21)
    )

    checkError(
      exception = intercept[AnalysisException] {
        df2.select(map_concat($"map1", $"map2")).collect()
      },
      errorClass = "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
      sqlState = None,
      parameters = Map(
        "sqlExpr" -> "\"map_concat(map1, map2)\"",
        "dataType" -> "(\"MAP<ARRAY<INT>, INT>\" or \"MAP<STRING, INT>\")",
        "functionName" -> "`map_concat`")
    )

    checkError(
      exception = intercept[AnalysisException] {
        df2.selectExpr("map_concat(map1, 12)").collect()
      },
      errorClass = "DATATYPE_MISMATCH.MAP_CONCAT_DIFF_TYPES",
      sqlState = None,
      parameters = Map(
        "sqlExpr" -> "\"map_concat(map1, 12)\"",
        "dataType" -> "[\"MAP<ARRAY<INT>, INT>\", \"INT\"]",
        "functionName" -> "`map_concat`"),
      context = ExpectedContext(
        fragment = "map_concat(map1, 12)",
        start = 0,
        stop = 19)
    )

    checkError(
      exception = intercept[AnalysisException] {
        df2.select(map_concat($"map1", lit(12))).collect()
      },
      errorClass = "DATATYPE_MISMATCH.MAP_CONCAT_DIFF_TYPES",
      sqlState = None,
      parameters = Map(
        "sqlExpr" -> "\"map_concat(map1, 12)\"",
        "dataType" -> "[\"MAP<ARRAY<INT>, INT>\", \"INT\"]",
        "functionName" -> "`map_concat`")
    )
  }
}
