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
package org.apache.gluten.expression

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, DecimalType}

import com.google.common.collect.Lists

case class DecimalRoundTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Round)
  extends ExpressionTransformer {

  val toScale: Int = original.scale.eval(EmptyRow).asInstanceOf[Int]

  // Use the same result type for different Spark versions.
  val dataType: DataType = original.child.dataType match {
    case decimalType: DecimalType =>
      val p = decimalType.precision
      val s = decimalType.scale
      // After rounding we may need one more digit in the integral part,
      // e.g. `ceil(9.9, 0)` -> `10`, `ceil(99, -1)` -> `100`.
      val integralLeastNumDigits = p - s + 1
      if (toScale < 0) {
        // negative scale means we need to adjust `-scale` number of digits before the decimal
        // point, which means we need at lease `-scale + 1` digits (after rounding).
        val newPrecision = math.max(integralLeastNumDigits, -toScale + 1)
        // We have to accept the risk of overflow as we can't exceed the max precision.
        DecimalType(math.min(newPrecision, DecimalType.MAX_PRECISION), 0)
      } else {
        val newScale = math.min(s, toScale)
        // We have to accept the risk of overflow as we can't exceed the max precision.
        DecimalType(math.min(integralLeastNumDigits + newScale, 38), newScale)
      }
    case _ =>
      throw new GlutenNotSupportException(
        s"Decimal type is expected but received ${original.child.dataType.typeName}.")
  }

  override def doTransform(args: Object): ExpressionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        Seq(original.child.dataType),
        FunctionConfig.OPT))

    ExpressionBuilder.makeScalarFunction(
      functionId,
      Lists.newArrayList[ExpressionNode](
        child.doTransform(args),
        ExpressionBuilder.makeIntLiteral(toScale)),
      ConverterUtils.getTypeNode(dataType, original.nullable)
    )
  }
}
