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
package org.apache.gluten.extension

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{ExpressionTransformer, Sig}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, AggregateMode}
import org.apache.spark.sql.types.DataType

import scala.collection.mutable.ListBuffer

trait ExpressionExtensionTrait {

  /** Generate the extension expressions list, format: Sig[XXXExpression]("XXXExpressionName") */
  def expressionSigList: Seq[Sig]

  /** Generate the extension expressions mapping map */
  def extensionExpressionsMapping: Map[Class[_], String] =
    expressionSigList.map(s => (s.expClass, s.name)).toMap[Class[_], String]

  /** Replace extension expression to transformer. */
  def replaceWithExtensionExpressionTransformer(
      substraitExprName: String,
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    throw new UnsupportedOperationException(s"${expr.getClass} or $expr is not supported.")
  }

  /** Get the attribute index of the extension aggregate functions. */
  def getAttrsIndexForExtensionAggregateExpr(
      aggregateFunc: AggregateFunction,
      mode: AggregateMode,
      exp: AggregateExpression,
      aggregateAttributeList: Seq[Attribute],
      aggregateAttr: ListBuffer[Attribute],
      resIndex: Int): Int = {
    throw new UnsupportedOperationException(
      s"Aggregate function ${aggregateFunc.getClass} is not supported.")
  }

  /** Get the custom agg function substrait name and the input types of the child */
  def buildCustomAggregateFunction(
      aggregateFunc: AggregateFunction): (Option[String], Seq[DataType]) = {
    throw new GlutenNotSupportException(
      s"Aggregate function ${aggregateFunc.getClass} is not supported.")
  }
}

case class DefaultExpressionExtensionTransformer() extends ExpressionExtensionTrait with Logging {

  /** Generate the extension expressions list, format: Sig[XXXExpression]("XXXExpressionName") */
  override def expressionSigList: Seq[Sig] = Seq.empty[Sig]
}
