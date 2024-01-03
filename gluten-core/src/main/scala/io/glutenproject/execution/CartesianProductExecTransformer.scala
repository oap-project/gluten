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
package io.glutenproject.execution

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.ExpressionConverter
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.rel.RelBuilder

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CartesianProductExecTransformer(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression])
  extends BaseJoinExec
  with TransformSupport {

  override def joinType: JoinType = Inner

  override def leftKeys: Seq[Expression] = Nil

  override def rightKeys: Seq[Expression] = Nil

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] =
    getColumnarInputRDDs(left) ++ getColumnarInputRDDs(right)

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genCartesianProductTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater = {
    BackendsApiManager.getMetricsApiInstance.genCartesianProductTransformerMetricsUpdater(metrics)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val leftPlanContext = left.asInstanceOf[TransformSupport].doTransform(context)
    val (inputLeftRelNode, inputLeftOutput) =
      (leftPlanContext.root, leftPlanContext.outputAttributes)

    val rightPlanContext = right.asInstanceOf[TransformSupport].doTransform(context)
    val (inputRightRelNode, inputRightOutput) =
      (rightPlanContext.root, rightPlanContext.outputAttributes)

    val expressionNode = condition.map {
      expr =>
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, inputLeftOutput ++ inputRightOutput)
          .doTransform(context.registeredFunction)
    }

    val extensionNode =
      JoinUtils.createExtensionNode(inputLeftOutput ++ inputRightOutput, validation = false)

    val operatorId = context.nextOperatorId(this.nodeName)

    val currRel = RelBuilder.makeCrossRel(
      inputLeftRelNode,
      inputRightRelNode,
      expressionNode.orNull,
      extensionNode,
      context,
      operatorId
    )
    TransformContext(inputLeftOutput ++ inputRightOutput, output, currRel)
  }

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    val expressionNode = condition.map {
      expr =>
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, left.output ++ right.output)
          .doTransform(substraitContext.registeredFunction)
    }
    val extensionNode =
      JoinUtils.createExtensionNode(left.output ++ right.output, validation = true)

    val currRel = RelBuilder.makeCrossRel(
      null,
      null,
      expressionNode.orNull,
      extensionNode,
      substraitContext,
      substraitContext.nextOperatorId(this.nodeName)
    )
    doNativeValidation(substraitContext, currRel)
  }

  override def nodeName: String = "CartesianProductExecTransformer"

  override def output: Seq[Attribute] = left.output ++ right.output

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): CartesianProductExecTransformer =
    copy(left = newLeft, right = newRight)
}
