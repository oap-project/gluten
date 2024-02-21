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

import org.apache.spark.sql.functions._

class GlutenDataFramePivotSuite extends DataFramePivotSuite with GlutenSQLTestsTrait {

  // This test is ported from vanilla spark with pos value (1-based) changed from 0 to 1 for
  // substring. In vanilla spark, pos=0 has same effectiveness as pos=1. But in velox, pos=0
  // will return an empty string as substring result.
  test("pivot with column definition in groupby - using pos=1") {
    val df = courseSales
      .groupBy(substring(col("course"), 1, 1).as("foo"))
      .pivot("year", Seq(2012, 2013))
      .sum("earnings")
      .queryExecution
      .executedPlan

    checkAnswer(
      courseSales
        .groupBy(substring(col("course"), 1, 1).as("foo"))
        .pivot("year", Seq(2012, 2013))
        .sum("earnings"),
      Row("d", 15000.0, 48000.0) :: Row("J", 20000.0, 30000.0) :: Nil
    )
  }
}
