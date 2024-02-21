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

class GlutenCollectionExpressionsSuite extends CollectionExpressionsSuite with GlutenTestsTrait {
  test(GLUTEN_TEST + "Concat") {
    // Primitive-type elements
    val ai0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq.empty[Integer], ArrayType(IntegerType, containsNull = false))
    val ai2 = Literal.create(Seq(4, null, 5), ArrayType(IntegerType, containsNull = true))
    val ai3 = Literal.create(Seq(null, null), ArrayType(IntegerType, containsNull = true))
    val ai4 = Literal.create(null, ArrayType(IntegerType, containsNull = false))

    // checkEvaluation(Concat(Seq(ai0)), Seq(1, 2, 3))
    checkEvaluation(Concat(Seq(ai0, ai1)), Seq(1, 2, 3))
    checkEvaluation(Concat(Seq(ai1, ai0)), Seq(1, 2, 3))
    checkEvaluation(Concat(Seq(ai0, ai0)), Seq(1, 2, 3, 1, 2, 3))
    checkEvaluation(Concat(Seq(ai0, ai2)), Seq(1, 2, 3, 4, null, 5))
    checkEvaluation(Concat(Seq(ai0, ai3, ai2)), Seq(1, 2, 3, null, null, 4, null, 5))
    checkEvaluation(Concat(Seq(ai4)), null)
    checkEvaluation(Concat(Seq(ai0, ai4)), null)
    checkEvaluation(Concat(Seq(ai4, ai0)), null)

    // Non-primitive-type elements
    val as0 = Literal.create(Seq("a", "b", "c"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq.empty[String], ArrayType(StringType, containsNull = false))
    val as2 = Literal.create(Seq("d", null, "e"), ArrayType(StringType, containsNull = true))
    val as3 = Literal.create(Seq(null, null), ArrayType(StringType, containsNull = true))
    val as4 = Literal.create(null, ArrayType(StringType, containsNull = false))

    val aa0 = Literal.create(
      Seq(Seq("a", "b"), Seq("c")),
      ArrayType(ArrayType(StringType, containsNull = false), containsNull = false))
    val aa1 = Literal.create(
      Seq(Seq("d"), Seq("e", "f")),
      ArrayType(ArrayType(StringType, containsNull = false), containsNull = false))
    val aa2 = Literal.create(
      Seq(Seq("g", null), null),
      ArrayType(ArrayType(StringType, containsNull = true), containsNull = true))

    // checkEvaluation(Concat(Seq(as0)), Seq("a", "b", "c"))
    checkEvaluation(Concat(Seq(as0, as1)), Seq("a", "b", "c"))
    checkEvaluation(Concat(Seq(as1, as0)), Seq("a", "b", "c"))
    checkEvaluation(Concat(Seq(as0, as0)), Seq("a", "b", "c", "a", "b", "c"))
    checkEvaluation(Concat(Seq(as0, as2)), Seq("a", "b", "c", "d", null, "e"))
    checkEvaluation(Concat(Seq(as0, as3, as2)), Seq("a", "b", "c", null, null, "d", null, "e"))
    checkEvaluation(Concat(Seq(as4)), null)
    checkEvaluation(Concat(Seq(as0, as4)), null)
    checkEvaluation(Concat(Seq(as4, as0)), null)

    checkEvaluation(Concat(Seq(aa0, aa1)), Seq(Seq("a", "b"), Seq("c"), Seq("d"), Seq("e", "f")))

    assert(Concat(Seq(ai0, ai1)).dataType.asInstanceOf[ArrayType].containsNull === false)
    assert(Concat(Seq(ai0, ai2)).dataType.asInstanceOf[ArrayType].containsNull)
    assert(Concat(Seq(as0, as1)).dataType.asInstanceOf[ArrayType].containsNull === false)
    assert(Concat(Seq(as0, as2)).dataType.asInstanceOf[ArrayType].containsNull)
    assert(
      Concat(Seq(aa0, aa1)).dataType ===
        ArrayType(ArrayType(StringType, containsNull = false), containsNull = false))
    assert(
      Concat(Seq(aa0, aa2)).dataType ===
        ArrayType(ArrayType(StringType, containsNull = true), containsNull = true))

    // force split expressions for input in generated code
    checkEvaluation(Concat(Seq.fill(100)(ai0)), Seq.fill(100)(Seq(1, 2, 3)).flatten)
  }
}
