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
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.RelNode

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.JavaConverters._

// Transformer for GeneratorExec, which Applies a [[Generator]] to a stream of input rows.
abstract class GenerateExecTransformerBase(
    generator: Generator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    child: SparkPlan)
  extends UnaryTransformSupport {

  protected def doGeneratorValidate(generator: Generator, outer: Boolean): ValidationResult

  protected def getRelNode(
      context: SubstraitContext,
      inputRel: RelNode,
      generatorNode: ExpressionNode,
      validation: Boolean): RelNode

  protected lazy val requiredChildOutputNodes: Seq[ExpressionNode] = {
    requiredChildOutput.map {
      target =>
        val childIndex = child.output.zipWithIndex
          .collectFirst {
            case (attr, i) if attr.name == target.name => i
          }
          .getOrElse(
            throw new GlutenException(s"Can't found column ${target.name} in child output"))
        ExpressionBuilder.makeSelection(childIndex)
    }
  }

  override def output: Seq[Attribute] = requiredChildOutput ++ generatorOutput

  override def producedAttributes: AttributeSet = AttributeSet(generatorOutput)

  override protected def doValidateInternal(): ValidationResult = {
    val validationResult = doGeneratorValidate(generator, outer)
    if (!validationResult.isValid) {
      return validationResult
    }
    val context = new SubstraitContext
    val relNode =
      getRelNode(context, null, getGeneratorNode(context), validation = true)
    doNativeValidation(context, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)
    val relNode = getRelNode(context, childCtx.root, getGeneratorNode(context), validation = false)
    TransformContext(child.output, output, relNode)
  }

  protected def getExtensionNodeForValidation: AdvancedExtensionNode = {
    // Use a extension node to send the input types through Substrait plan for validation.
    val inputTypeNodeList =
      child.output.map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable)).asJava
    ExtensionBuilder.makeAdvancedExtension(
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
  }

  private def getGeneratorNode(context: SubstraitContext): ExpressionNode =
    ExpressionConverter
      .replaceWithExpressionTransformer(generator, child.output)
      .doTransform(context.registeredFunction)
}
