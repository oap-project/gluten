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
package org.apache.gluten.execution

import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.{GenerateMetricsUpdater, MetricsUpdater}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics

import scala.collection.JavaConverters._

// Transformer for GeneratorExec, which Applies a [[Generator]] to a stream of input rows.
// For clickhouse backend, it will transform Spark explode lateral view to CH array join.
case class CHGenerateExecTransformer(
    generator: Generator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    child: SparkPlan)
  extends GenerateExecTransformerBase(
    generator,
    requiredChildOutput,
    outer,
    generatorOutput,
    child) {

  @transient
  override lazy val metrics =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def metricsUpdater(): MetricsUpdater = new GenerateMetricsUpdater(metrics)
  override protected def withNewChildInternal(newChild: SparkPlan): CHGenerateExecTransformer =
    copy(generator, requiredChildOutput, outer, generatorOutput, newChild)

  override protected def doGeneratorValidate(
      generator: Generator,
      outer: Boolean): ValidationResult =
    ValidationResult.ok

  override protected def getRelNode(
      context: SubstraitContext,
      inputRel: RelNode,
      generatorNode: ExpressionNode,
      validation: Boolean): RelNode = {
    if (!validation) {
      RelBuilder.makeGenerateRel(
        inputRel,
        generatorNode,
        requiredChildOutputNodes.asJava,
        context,
        context.nextOperatorId(this.nodeName))
    } else {
      RelBuilder.makeGenerateRel(
        inputRel,
        generatorNode,
        requiredChildOutputNodes.asJava,
        getExtensionNodeForValidation,
        context,
        context.nextOperatorId(this.nodeName))
    }
  }
}
