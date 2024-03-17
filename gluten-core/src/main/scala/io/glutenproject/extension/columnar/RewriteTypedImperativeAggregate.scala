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
package io.glutenproject.extension.columnar

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.types._

object RewriteTypedImperativeAggregate extends Rule[SparkPlan] with PullOutProjectHelper {
  private lazy val shouldRewriteTypedImperativeAggregate =
    BackendsApiManager.getSettings.shouldRewriteTypedImperativeAggregate()

  def shouldRewrite(ae: AggregateExpression): Boolean = {
    ae.aggregateFunction match {
      case _: CollectList | _: CollectSet =>
        ae.mode match {
          case Partial | PartialMerge => true
          case _ => false
        }
      case _ => false
    }
  }

  def shouldRewriteForPercentileLikeExpr(ae: AggregateExpression): Boolean = {
    ae.aggregateFunction match {
      case _: ApproximatePercentile =>
        ae.mode match {
          case Partial | PartialMerge => true
          case _ => false
        }
      case _ => false
    }
  }

  def getPercentileLikeInterminateDataType(aggFunc: AggregateFunction): StructType = {
    aggFunc match {
      case a: ApproximatePercentile =>
        val childType = a.child.dataType
        StructType(
          Array(
            StructField("col1", ArrayType(DoubleType)),
            StructField("col2", BooleanType, false),
            StructField("col3", DoubleType, false),
            StructField("col4", IntegerType, false),
            StructField("col5", LongType, false),
            StructField("col6", childType, false),
            StructField("col7", childType, false),
            StructField("col8", ArrayType(childType)),
            StructField("col9", ArrayType(IntegerType))
          ))
      case f =>
        throw new IllegalArgumentException(s"Unsupported aggregate function $f")
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!shouldRewriteTypedImperativeAggregate) {
      return plan
    }

    plan match {
      case agg: BaseAggregateExec if agg.aggregateExpressions.exists(shouldRewrite) =>
        val exprMap = agg.aggregateExpressions
          .filter(shouldRewrite)
          .map(ae => ae.aggregateFunction.inputAggBufferAttributes.head -> ae)
          .toMap
        val newResultExpressions = agg.resultExpressions.map {
          case attr: AttributeReference =>
            exprMap
              .get(attr)
              .map {
                ae =>
                  attr.copy(dataType = ae.aggregateFunction.dataType)(
                    exprId = attr.exprId,
                    qualifier = attr.qualifier
                  )
              }
              .getOrElse(attr)
          case other => other
        }
        copyBaseAggregateExec(agg)(newResultExpressions = newResultExpressions)

      case agg: BaseAggregateExec
          if agg.aggregateExpressions.exists(shouldRewriteForPercentileLikeExpr) =>
        val exprMap = agg.aggregateExpressions
          .filter(shouldRewriteForPercentileLikeExpr)
          .map(ae => ae.aggregateFunction.inputAggBufferAttributes.head -> ae)
          .toMap
        val newResultExpressions = agg.resultExpressions.map {
          case attr: AttributeReference =>
            exprMap
              .get(attr)
              .map {
                ae =>
                  attr.copy(dataType = getPercentileLikeInterminateDataType(ae.aggregateFunction))(
                    exprId = attr.exprId,
                    qualifier = attr.qualifier
                  )
              }
              .getOrElse(attr)
          case other => other
        }
        copyBaseAggregateExec(agg)(newResultExpressions = newResultExpressions)

      case _ => plan
    }
  }
}
