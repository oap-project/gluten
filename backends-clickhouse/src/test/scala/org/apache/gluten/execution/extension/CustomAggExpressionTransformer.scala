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
package org.apache.gluten.execution.extension

import org.apache.gluten.expression.Sig
import org.apache.gluten.extension.ExpressionExtensionTrait

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.{DataType, LongType}

import scala.collection.mutable.ListBuffer

case class CustomAggExpressionTransformer() extends ExpressionExtensionTrait {

  lazy val expressionSigs = Seq(
    Sig[CustomSum]("custom_sum")
  )

  /** Generate the extension expressions list, format: Sig[XXXExpression]("XXXExpressionName") */
  override def expressionSigList: Seq[Sig] = expressionSigs

  /** Get the attribute index of the extension aggregate functions. */
  override def getAttrsIndexForExtensionAggregateExpr(
      aggregateFunc: AggregateFunction,
      mode: AggregateMode,
      exp: AggregateExpression,
      aggregateAttributeList: Seq[Attribute],
      aggregateAttr: ListBuffer[Attribute],
      resIndex: Int): Int = {
    var reIndex = resIndex
    aggregateFunc match {
      case CustomSum(_, _) =>
        mode match {
          // custom logic: can not support 'Partial'
          /* case Partial =>
            val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
            val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
            aggregateAttr += attr
            reIndex += 1
            reIndex */
          case Final =>
            aggregateAttr += aggregateAttributeList(reIndex)
            reIndex += 1
            reIndex
          case other =>
            throw new UnsupportedOperationException(s"Unsupported aggregate mode: $other.")
        }
    }
  }

  /** Get the custom agg function substrait name and the input types of the child */
  override def buildCustomAggregateFunction(
      aggregateFunc: AggregateFunction): (Option[String], Seq[DataType]) = {
    val substraitAggFuncName = aggregateFunc match {
      case customSum: CustomSum =>
        if (customSum.dataType.isInstanceOf[LongType]) {
          Some("custom_sum")
        } else {
          Some("custom_sum_double")
        }
      case _ =>
        extensionExpressionsMapping.get(aggregateFunc.getClass)
    }
    if (substraitAggFuncName.isEmpty) {
      throw new UnsupportedOperationException(
        s"Aggregate function ${aggregateFunc.getClass} is not supported.")
    }
    (substraitAggFuncName, aggregateFunc.children.map(child => child.dataType))
  }
}
