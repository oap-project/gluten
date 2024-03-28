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
package org.apache.gluten.extension.columnar

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions.{If, IsNull, Literal, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.types.IntegerType

/**
 * This Rule is used to rewrite the count for aggregates if:
 *   - the count is partial
 *   - the count has more than one child
 *
 * A rewrite count multi-children example:
 * {{{
 *   Count(c1, c2)
 *   =>
 *   Count(
 *     If(
 *       Or(IsNull(c1), IsNull(c2)),
 *       Literal(null),
 *       Literal(1)
 *     )
 *   )
 * }}}
 *
 * TODO: Remove this rule when Velox support multi-children Count
 */
object RewriteMultiChildrenCount extends Rule[SparkPlan] with PullOutProjectHelper {
  private lazy val shouldRewriteCount = BackendsApiManager.getSettings.shouldRewriteCount()

  private def extractCountForRewrite(aggExpr: AggregateExpression): Option[Count] = {
    val isPartialCountWithMoreThanOneChild = aggExpr.mode == Partial && {
      aggExpr.aggregateFunction match {
        case c: Count => c.children.size > 1
        case _ => false
      }
    }
    if (isPartialCountWithMoreThanOneChild) {
      Option(aggExpr.aggregateFunction.asInstanceOf[Count])
    } else {
      None
    }
  }

  private def shouldRewrite(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    aggregateExpressions.exists(extractCountForRewrite(_).isDefined)
  }

  private def rewriteCount(
      aggregateExpressions: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    aggregateExpressions.map {
      aggExpr =>
        val countOpt = extractCountForRewrite(aggExpr)
        countOpt
          .map {
            count =>
              // Follow vanillas Spark Count function
              val nullableChildren = count.children.filter(_.nullable)
              val newChild = if (nullableChildren.isEmpty) {
                Literal.create(1, IntegerType)
              } else {
                If(
                  nullableChildren.map(IsNull).reduce(Or),
                  Literal.create(null, IntegerType),
                  Literal.create(1, IntegerType))
              }
              val newCount = count.copy(children = newChild :: Nil)
              aggExpr.copy(aggregateFunction = newCount)
          }
          .getOrElse(aggExpr)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!shouldRewriteCount) {
      return plan
    }

    plan match {
      case agg: BaseAggregateExec if shouldRewrite(agg.aggregateExpressions) =>
        val newAggExprs = rewriteCount(agg.aggregateExpressions)
        copyBaseAggregateExec(agg)(newAggregateExpressions = newAggExprs)

      case _ => plan
    }
  }
}
