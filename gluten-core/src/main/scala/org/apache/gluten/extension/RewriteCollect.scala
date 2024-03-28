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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, If, IsNotNull, IsNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, CollectSet, Complete, Final, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.types.ArrayType

import scala.collection.mutable.ArrayBuffer

/**
 * This rule rewrite collect_set and collect_list to be compatible with vanilla Spark.
 *
 *   - Add `IsNotNull(partial_in)` to skip null value before going to native collect_set
 *   - Add `If(IsNull(result), CreateArray(Seq.empty), result)` to replace null to empty array
 *
 * TODO: remove this rule once Velox compatible with vanilla Spark.
 */
object RewriteCollect extends Rule[SparkPlan] with PullOutProjectHelper {
  private lazy val shouldRewriteCollect =
    BackendsApiManager.getSettings.shouldRewriteCollect()

  private def shouldAddIsNotNull(ae: AggregateExpression): Boolean = {
    ae.aggregateFunction match {
      case c: CollectSet if c.child.nullable =>
        ae.mode match {
          case Partial | Complete => true
          case _ => false
        }
      case _ => false
    }
  }

  private def shouldReplaceNullToEmptyArray(ae: AggregateExpression): Boolean = {
    ae.aggregateFunction match {
      case _: CollectSet | _: CollectList =>
        ae.mode match {
          case Final | Complete => true
          case _ => false
        }
      case _ => false
    }
  }

  private def shouldRewrite(agg: BaseAggregateExec): Boolean = {
    agg.aggregateExpressions.exists {
      ae => shouldAddIsNotNull(ae) || shouldReplaceNullToEmptyArray(ae)
    }
  }

  private def rewriteCollectFilter(aggExprs: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    aggExprs
      .map {
        aggExpr =>
          if (shouldAddIsNotNull(aggExpr)) {
            val newFilter =
              (aggExpr.filter ++ Seq(IsNotNull(aggExpr.aggregateFunction.children.head)))
                .reduce(And)
            aggExpr.copy(filter = Option(newFilter))
          } else {
            aggExpr
          }
      }
  }

  private def rewriteAttributesAndResultExpressions(
      agg: BaseAggregateExec): (Seq[Attribute], Seq[NamedExpression]) = {
    val rewriteAggExprIndices = agg.aggregateExpressions.zipWithIndex
      .filter(exprAndIndex => shouldReplaceNullToEmptyArray(exprAndIndex._1))
      .map(_._2)
      .toSet
    if (rewriteAggExprIndices.isEmpty) {
      return (agg.aggregateAttributes, agg.resultExpressions)
    }

    assert(agg.aggregateExpressions.size == agg.aggregateAttributes.size)
    val rewriteAggAttributes = new ArrayBuffer[Attribute]()
    val newAggregateAttributes = agg.aggregateAttributes.zipWithIndex.map {
      case (attr, index) =>
        if (rewriteAggExprIndices.contains(index)) {
          rewriteAggAttributes.append(attr)
          // We should mark attribute as withNullability since the collect_set and collect_set
          // are not nullable but velox may return null. This is to avoid potential issue when
          // the post project fallback to vanilla Spark.
          attr.withNullability(true)
        } else {
          attr
        }
    }
    val rewriteAggAttributeSet = AttributeSet(rewriteAggAttributes)
    val newResultExpressions = agg.resultExpressions.map {
      ne =>
        val rewritten = ne.transformUp {
          case attr: Attribute if rewriteAggAttributeSet.contains(attr) =>
            assert(attr.dataType.isInstanceOf[ArrayType])
            If(IsNull(attr), Literal.create(Seq.empty, attr.dataType), attr)
        }
        assert(rewritten.isInstanceOf[NamedExpression])
        rewritten.asInstanceOf[NamedExpression]
    }
    (newAggregateAttributes, newResultExpressions)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!shouldRewriteCollect) {
      return plan
    }

    plan match {
      case agg: BaseAggregateExec if shouldRewrite(agg) =>
        val newAggExprs = rewriteCollectFilter(agg.aggregateExpressions)
        val (newAttributes, newResultExprs) = rewriteAttributesAndResultExpressions(agg)
        copyBaseAggregateExec(agg)(
          newAggregateExpressions = newAggExprs,
          newAggregateAttributes = newAttributes,
          newResultExpressions = newResultExprs)

      case _ => plan
    }
  }
}
