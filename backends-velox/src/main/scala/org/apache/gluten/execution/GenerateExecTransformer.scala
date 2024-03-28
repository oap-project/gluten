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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.GenerateExecTransformer.supportsGenerate
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.{GenerateMetricsUpdater, MetricsUpdater}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}
import org.apache.gluten.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{GenerateExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.IntegerType

import com.google.protobuf.StringValue

import scala.collection.JavaConverters._
import scala.collection.mutable

case class GenerateExecTransformer(
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
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def metricsUpdater(): MetricsUpdater = new GenerateMetricsUpdater(metrics)

  override protected def withNewChildInternal(newChild: SparkPlan): GenerateExecTransformer =
    copy(generator, requiredChildOutput, outer, generatorOutput, newChild)

  override protected def doGeneratorValidate(
      generator: Generator,
      outer: Boolean): ValidationResult = {
    if (!supportsGenerate(generator, outer)) {
      ValidationResult.notOk(
        s"Velox backend does not support this generator: ${generator.getClass.getSimpleName}" +
          s", outer: $outer")
    } else {
      ValidationResult.ok
    }
  }

  override protected def getRelNode(
      context: SubstraitContext,
      inputRel: RelNode,
      generatorNode: ExpressionNode,
      validation: Boolean): RelNode = {
    val operatorId = context.nextOperatorId(this.nodeName)
    RelBuilder.makeGenerateRel(
      inputRel,
      generatorNode,
      requiredChildOutputNodes.asJava,
      getExtensionNode(validation),
      context,
      operatorId)
  }

  private def getExtensionNode(validation: Boolean): AdvancedExtensionNode = {
    if (!validation) {
      // Start with "GenerateParameters:"
      val parametersStr = new StringBuffer("GenerateParameters:")
      // isPosExplode: 1 for PosExplode, 0 for others.
      val isPosExplode = if (generator.isInstanceOf[PosExplode]) {
        "1"
      } else {
        "0"
      }
      parametersStr
        .append("isPosExplode=")
        .append(isPosExplode)
        .append("\n")
      val message = StringValue
        .newBuilder()
        .setValue(parametersStr.toString)
        .build()
      val optimization = BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
      ExtensionBuilder.makeAdvancedExtension(optimization, null)
    } else {
      getExtensionNodeForValidation
    }
  }
}

object GenerateExecTransformer {
  def supportsGenerate(generator: Generator, outer: Boolean): Boolean = {
    // TODO: supports outer and remove this param.
    if (outer) {
      false
    } else {
      generator match {
        case _: Inline | _: ExplodeBase =>
          true
        case _ =>
          false
      }
    }
  }
}

object PullOutGenerateProjectHelper extends PullOutProjectHelper {
  def pullOutPreProject(generate: GenerateExec): SparkPlan = {
    if (GenerateExecTransformer.supportsGenerate(generate.generator, generate.outer)) {
      val newGeneratorChildren = generate.generator match {
        case _: Inline | _: ExplodeBase =>
          val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
          // The new child should be either the original Attribute,
          // or an Alias to other expressions.
          val generatorAttr = replaceExpressionWithAttribute(
            generate.generator.asInstanceOf[UnaryExpression].child,
            expressionMap,
            replaceBoundReference = true)
          val newGeneratorChild = if (expressionMap.isEmpty) {
            // generator.child is Attribute
            generatorAttr.asInstanceOf[Attribute]
          } else {
            // generator.child is other expression, e.g Literal/CreateArray/CreateMap
            expressionMap.values.head
          }
          Seq(newGeneratorChild)
        case _ =>
          // Unreachable.
          throw new IllegalStateException(
            s"Generator ${generate.generator.getClass.getSimpleName} is not supported.")
      }
      // Avoid using elimainateProjectList to create the project list
      // because newGeneratorChild can be a duplicated Attribute in generate.child.output.
      // The native side identifies the last field of projection as generator's input.
      generate.copy(
        generator =
          generate.generator.withNewChildren(newGeneratorChildren).asInstanceOf[Generator],
        child = ProjectExec(generate.child.output ++ newGeneratorChildren, generate.child)
      )
    } else {
      generate
    }
  }

  def pullOutPostProject(generate: GenerateExec): SparkPlan = {
    if (GenerateExecTransformer.supportsGenerate(generate.generator, generate.outer)) {
      generate.generator match {
        case PosExplode(_) =>
          val originalOrdinal = generate.generatorOutput.head
          val ordinal = {
            val subtract = Subtract(Cast(originalOrdinal, IntegerType), Literal(1))
            Alias(subtract, generatePostAliasName)(
              originalOrdinal.exprId,
              originalOrdinal.qualifier)
          }
          val newGenerate =
            generate.copy(generatorOutput = generate.generatorOutput.tail :+ originalOrdinal)
          ProjectExec(
            (generate.requiredChildOutput :+ ordinal) ++ generate.generatorOutput.tail,
            newGenerate)
        case Inline(_) =>
          val unnestOutput = {
            val struct = CreateStruct(generate.generatorOutput)
            val alias = Alias(struct, generatePostAliasName)()
            alias.toAttribute
          }
          val newGenerate = generate.copy(generatorOutput = Seq(unnestOutput))
          val newOutput = generate.generatorOutput.zipWithIndex.map {
            case (attr, i) =>
              val getStructField = GetStructField(unnestOutput, i, Some(attr.name))
              Alias(getStructField, generatePostAliasName)(attr.exprId, attr.qualifier)
          }
          ProjectExec(generate.requiredChildOutput ++ newOutput, newGenerate)
        case _ => generate
      }
    } else {
      generate
    }
  }
}
