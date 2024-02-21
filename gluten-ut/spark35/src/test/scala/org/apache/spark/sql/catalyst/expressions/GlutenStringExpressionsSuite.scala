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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.types._

class GlutenStringExpressionsSuite extends StringExpressionsSuite with GlutenTestsTrait {
  test(GLUTEN_TEST + "concat") {
    def testConcat(inputs: String*): Unit = {
      val expected = if (inputs.contains(null)) null else inputs.mkString
      checkEvaluation(Concat(inputs.map(Literal.create(_, StringType))), expected)
    }

    // testConcat() velox not supported
    testConcat(null)
    testConcat("")
    testConcat("ab")
    testConcat("a", "b")
    testConcat("a", "b", "C")
    testConcat("a", null, "C")
    testConcat("a", null, null)
    testConcat(null, null, null)

    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    testConcat("数据", null, "砖头")
    // scalastyle:on
  }
}
