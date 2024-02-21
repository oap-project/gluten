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
package org.apache.spark.sql.execution

import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.internal.SQLConf

class GlutenExchangeSuite extends ExchangeSuite with GlutenSQLTestsBaseTrait {

  test("Exchange reuse across the whole plan with shuffle partition 2") {
    // The shuffle exchange will be inserted between Aggregate
    // when shuffle partition is > 1.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val df = sql("""
                     |SELECT
                     |  (SELECT max(a.key) FROM testData AS a JOIN testData AS b ON b.key = a.key),
                     |  a.key
                     |FROM testData AS a
                     |JOIN testData AS b ON b.key = a.key
      """.stripMargin)

      val plan = df.queryExecution.executedPlan

      val exchangeIds = plan.collectWithSubqueries { case e: Exchange => e.id }
      val reusedExchangeIds = plan.collectWithSubqueries {
        case re: ReusedExchangeExec => re.child.id
      }

      assert(exchangeIds.size == 2, "Whole plan exchange reusing not working correctly")
      assert(reusedExchangeIds.size == 3, "Whole plan exchange reusing not working correctly")
      assert(
        reusedExchangeIds.forall(exchangeIds.contains(_)),
        "ReusedExchangeExec should reuse an existing exchange")

      val df2 = sql("""
                      |SELECT
                      |  (SELECT min(a.key) FROM testData AS a JOIN testData AS b ON b.key = a.key),
                      |  (SELECT max(a.key) FROM testData AS a JOIN testData2 AS b ON b.a = a.key)
      """.stripMargin)

      val plan2 = df2.queryExecution.executedPlan

      val exchangeIds2 = plan2.collectWithSubqueries { case e: Exchange => e.id }
      val reusedExchangeIds2 = plan2.collectWithSubqueries {
        case re: ReusedExchangeExec => re.child.id
      }

      assert(exchangeIds2.size == 4, "Whole plan exchange reusing not working correctly")
      assert(reusedExchangeIds2.size == 2, "Whole plan exchange reusing not working correctly")
      assert(
        reusedExchangeIds2.forall(exchangeIds2.contains(_)),
        "ReusedExchangeExec should reuse an existing exchange")
    }
  }
}
