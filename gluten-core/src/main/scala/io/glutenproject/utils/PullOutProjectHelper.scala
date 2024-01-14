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
package io.glutenproject.utils

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

trait PullOutProjectHelper {

  private val generatedNameIndex = new AtomicInteger(0)

  /**
   * Some Expressions support Attribute and Literal when converting them into native plans, such as
   * the child of AggregateFunction.
   */
  protected def isNotAttributeAndLiteral(expression: Expression): Boolean = expression match {
    case _: Attribute | _: Literal => false
    case _ => true
  }

  /** The majority of Expressions only support Attribute when converting them into native plans. */
  protected def isNotAttribute(expression: Expression): Boolean = expression match {
    case _: Attribute => false
    case _ => true
  }

  protected def aggNeedPreProject(
      groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[Expression]): Boolean = {
    groupingExpressions.exists(isNotAttribute) ||
    aggregateExpressions.exists(_.find {
      case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[TypedAggregateExpression] =>
        // We cannot pull out the children of TypedAggregateExpression to pre-project,
        // and Gluten cannot support TypedAggregateExpression.
        false
      case ae: AggregateExpression
          if ae.filter.exists(isNotAttribute) || ae.aggregateFunction.children.exists(
            isNotAttributeAndLiteral) =>
        true
      case _ => false
    }.isDefined)
  }

  protected def getProjectExpressionMap = new mutable.HashMap[ExpressionEquals, NamedExpression]()

  protected def getAndReplaceProjectAttribute(
      expr: Expression,
      projectExprsMap: mutable.HashMap[ExpressionEquals, NamedExpression]): Expression =
    expr match {
      case alias: Alias =>
        projectExprsMap.getOrElseUpdate(ExpressionEquals(alias.child), alias).toAttribute
      case attr: Attribute =>
        projectExprsMap.getOrElseUpdate(ExpressionEquals(attr), attr)
      case other =>
        projectExprsMap
          .getOrElseUpdate(
            ExpressionEquals(other),
            Alias(other, s"_pre_${generatedNameIndex.getAndIncrement()}")())
          .toAttribute
    }
}
