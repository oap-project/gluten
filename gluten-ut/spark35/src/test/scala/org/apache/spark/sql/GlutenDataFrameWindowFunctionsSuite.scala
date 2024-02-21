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

import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class GlutenDataFrameWindowFunctionsSuite
  extends DataFrameWindowFunctionsSuite
  with GlutenSQLTestsTrait {

  import testImplicits._

  test(
    GLUTEN_TEST +
      "covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window") {
    withSQLConf(SQLConf.LEGACY_STATISTICAL_AGGREGATE.key -> "true") {
      val df = Seq(
        ("a", "p1", 10.0, 20.0),
        ("b", "p1", 20.0, 10.0),
        ("c", "p2", 20.0, 20.0),
        ("d", "p2", 20.0, 20.0),
        ("e", "p3", 0.0, 0.0),
        ("f", "p3", 6.0, 12.0),
        ("g", "p3", 6.0, 12.0),
        ("h", "p3", 8.0, 16.0)
      ).toDF("key", "partitionId", "value1", "value2")
      checkAnswer(
        df.select(
          $"key",
          covar_samp("value1", "value2").over(
            Window
              .partitionBy("partitionId")
              .orderBy("key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          var_samp("value1").over(
            Window
              .partitionBy("partitionId")
              .orderBy("key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          variance("value1").over(
            Window
              .partitionBy("partitionId")
              .orderBy("key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev_samp("value1").over(
            Window
              .partitionBy("partitionId")
              .orderBy("key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev("value1").over(
            Window
              .partitionBy("partitionId")
              .orderBy("key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
        ),
        Seq(
          Row("a", -50.0, 50.0, 50.0, 7.0710678118654755, 7.0710678118654755),
          Row("b", -50.0, 50.0, 50.0, 7.0710678118654755, 7.0710678118654755),
          Row("c", 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("d", 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("e", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("f", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("g", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("h", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544)
        )
      )
    }
  }

  test(GLUTEN_TEST + "corr, covar_pop, stddev_pop functions in specific window") {
    withSQLConf(SQLConf.LEGACY_STATISTICAL_AGGREGATE.key -> "true") {
      val df = Seq(
        ("a", "p1", 10.0, 20.0),
        ("b", "p1", 20.0, 10.0),
        ("c", "p2", 20.0, 20.0),
        ("d", "p2", 20.0, 20.0),
        ("e", "p3", 0.0, 0.0),
        ("f", "p3", 6.0, 12.0),
        ("g", "p3", 6.0, 12.0),
        ("h", "p3", 8.0, 16.0)
      ).toDF("key", "partitionId", "value1", "value2")
      checkAnswer(
        df.select(
          $"key",
          corr("value1", "value2").over(
            Window
              .partitionBy("partitionId")
              .orderBy("key")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          covar_pop("value1", "value2")
            .over(
              Window
                .partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          var_pop("value1")
            .over(
              Window
                .partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev_pop("value1")
            .over(
              Window
                .partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          var_pop("value2")
            .over(
              Window
                .partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev_pop("value2")
            .over(
              Window
                .partitionBy("partitionId")
                .orderBy("key")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
        ),

        // As stddev_pop(expr) = sqrt(var_pop(expr))
        // the "stddev_pop" column can be calculated from the "var_pop" column.
        //
        // As corr(expr1, expr2) = covar_pop(expr1, expr2) / (stddev_pop(expr1) * stddev_pop(expr2))
        // the "corr" column can be calculated from the "covar_pop" and the two "stddev_pop" columns
        Seq(
          Row("a", -1.0, -25.0, 25.0, 5.0, 25.0, 5.0),
          Row("b", -1.0, -25.0, 25.0, 5.0, 25.0, 5.0),
          Row("c", null, 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("d", null, 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("e", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("f", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("g", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("h", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0)
        )
      )
    }
  }
}
