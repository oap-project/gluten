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
package org.apache.spark.sql.errors

import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST

class GlutenQueryExecutionErrorsSuite
  extends QueryExecutionErrorsSuite
  with GlutenSQLTestsBaseTrait {
  override protected def getResourceParquetFilePath(name: String): String = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + name
  }

  test(
    GLUTEN_TEST + "SCALAR_SUBQUERY_TOO_MANY_ROWS: " +
      "More than one row returned by a subquery used as an expression") {
    val exception = intercept[IllegalStateException] {
      sql("select (select a from (select 1 as a union all select 2 as a) t) as b").collect()
    }
    assert(
      exception.getMessage.contains("more than one row returned by a subquery" +
        " used as an expression"))
  }
}
