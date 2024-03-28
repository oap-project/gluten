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
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.extension.ValidationResult
import io.glutenproject.extension.columnar.TransformHints
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SortExec, SparkPlan}

import scala.collection.JavaConverters._

case class LimitTransformer(child: SparkPlan, offset: Long, count: Long)
  extends UnaryTransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genLimitTransformerMetrics(sparkContext)

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): LimitTransformer =
    copy(child = newChild)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genLimitTransformerMetricsUpdater(metrics)

  override protected def doValidateInternal(): ValidationResult = {
    child match {
      case sort: SortExec if TransformHints.isTransformable(sort) && offset != 0 =>
        return ValidationResult.notOk(s"Native TopK does not support offset: $offset")
      case _ =>
    }

    val context = new SubstraitContext
    val operatorId = context.nextOperatorId(this.nodeName)
    val input = child match {
      // For topK case
      case c: TransformSupport => c.doTransform(context).root
      case _ => null
    }
    // If ACBO is enabled and is capable to move limit operators up and down among the plan nodes,
    // It might become possible that an independent limit gets to be transited to a order-by-limit.
    // In that case, we should tuning on the validation procedure. Either to move limit validation
    // To ACBO, or re-validate it in ACBO, or add properties or rules in ACBO to avoid such moves.
    //
    // It's not a issue for now since ACBO doesn't do such moves.
    val relNode = getRelNode(context, operatorId, offset, count, child.output, input, true)

    doNativeValidation(context, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getRelNode(context, operatorId, offset, count, child.output, childCtx.root, false)
    TransformContext(child.output, child.output, relNode)
  }

  def getRelNode(
      context: SubstraitContext,
      operatorId: Long,
      offset: Long,
      count: Long,
      inputAttributes: Seq[Attribute],
      input: RelNode,
      validation: Boolean): RelNode = {
    if (!validation) {
      RelBuilder.makeFetchRel(input, offset, count, context, operatorId)
    } else {
      val inputTypeNodes =
        inputAttributes.map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable)).asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodes).toProtobuf))
      RelBuilder.makeFetchRel(input, offset, count, extensionNode, context, operatorId)
    }
  }
}
