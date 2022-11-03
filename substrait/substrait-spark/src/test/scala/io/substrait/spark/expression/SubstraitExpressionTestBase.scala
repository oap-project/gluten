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
package io.substrait.spark.expression

import io.substrait.spark.ExpressionConverter
import io.substrait.spark.ExpressionConverter.EXTENSION_COLLECTION

import org.apache.spark.sql.catalyst.expressions.Expression

import io.substrait.expression.{Expression => SExpression}
import org.scalatest.Assertions.assertResult

import scala.collection.JavaConverters

trait SubstraitExpressionTestBase {

  private val expressionConverter = new SubstraitExpressionConverter(
    BinaryExpressionConverter(JavaConverters.asScalaBuffer(EXTENSION_COLLECTION.scalarFunctions())))

  protected def runTest(expectedName: String, expression: Expression): Unit = {
    runTest(expectedName, expression, func => {}, bidirectional = true)
  }

  protected def runTest(
      expectedName: String,
      expression: Expression,
      f: SExpression.ScalarFunctionInvocation => Unit,
      bidirectional: Boolean): Unit = {
    val substraitExp = ExpressionConverter
      .defaultConverter(expression)
      .asInstanceOf[SExpression.ScalarFunctionInvocation]
    assertResult(expectedName)(substraitExp.declaration().key())
    f(substraitExp)

    if (bidirectional) {
      val convertedExpression = substraitExp.accept(expressionConverter)
      assertResult(expression)(convertedExpression)
    }
  }
}
