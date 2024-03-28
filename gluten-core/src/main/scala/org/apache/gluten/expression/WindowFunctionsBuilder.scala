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
import org.apache.gluten.expression.ExpressionNames.{LAG, LEAD}
import org.apache.gluten.substrait.expression.ExpressionBuilder

import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Expression, Lag, Lead, WindowExpression, WindowFunction}

import scala.util.control.Breaks.{break, breakable}

object WindowFunctionsBuilder {
  def create(args: java.lang.Object, windowFunc: WindowFunction): Long = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val substraitFunc = windowFunc match {
      // Handle lag with negative inputOffset, e.g., converts lag(c1, -1) to lead(c1, 1).
      // Spark uses `-inputOffset` as `offset` for Lag function.
      case lag: Lag if lag.offset.eval(EmptyRow).asInstanceOf[Int] > 0 =>
        Some(LEAD)
      // Handle lead with negative offset, e.g., converts lead(c1, -1) to lag(c1, 1).
      case lead: Lead if lead.offset.eval(EmptyRow).asInstanceOf[Int] < 0 =>
        Some(LAG)
      case _ =>
        ExpressionMappings.expressionsMap.get(windowFunc.getClass)
    }
    if (substraitFunc.isEmpty) {
      throw new GlutenNotSupportException(
        s"not currently supported: ${windowFunc.getClass.getName}.")
    }

    val functionName =
      ConverterUtils.makeFuncName(substraitFunc.get, Seq(windowFunc.dataType), FunctionConfig.OPT)
    ExpressionBuilder.newScalarFunction(functionMap, functionName)
  }

  def extractWindowExpression(expr: Expression): WindowExpression = {
    expr match {
      case w: WindowExpression => w
      case other =>
        var w: WindowExpression = null
        breakable {
          other.children.foreach(
            child => {
              w = extractWindowExpression(child)
              break
            })
        }
        w
    }
  }
}
