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
package org.apache.gluten.expression

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.google.common.collect.Lists

import scala.collection.JavaConverters._

case class InTransformer(
    value: ExpressionTransformer,
    list: Seq[Expression],
    valueType: DataType,
    original: Expression)
  extends ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    assert(list.forall(_.foldable))
    // Stores the values in a List Literal.
    val values: Set[Any] = list.map(_.eval()).toSet
    InExpressionTransformer.toTransformer(value.doTransform(args), values, valueType)
  }
}

case class InSetTransformer(
    value: ExpressionTransformer,
    hset: Set[Any],
    valueType: DataType,
    original: Expression)
  extends ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    InExpressionTransformer.toTransformer(value.doTransform(args), hset, valueType)
  }
}

object InExpressionTransformer {

  def toTransformer(
      leftNode: ExpressionNode,
      values: Set[Any],
      valueType: DataType): ExpressionNode = {
    val expressionNodes = new java.util.ArrayList[ExpressionNode](
      values.toSeq
        // Sort elements for deterministic behaviours
        .sortBy(Literal(_, valueType).toString())
        .map(value => ExpressionBuilder.makeLiteral(value, valueType, value == null))
        .asJava)

    ExpressionBuilder.makeSingularOrListNode(leftNode, expressionNodes)
  }
}

case class LikeTransformer(
    substraitExprName: String,
    left: ExpressionTransformer,
    right: ExpressionTransformer,
    original: Expression)
  extends ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode = left.doTransform(args)
    val rightNode = right.doTransform(args)
    val escapeCharNode = ExpressionBuilder.makeLiteral(
      original.asInstanceOf[Like].escapeChar.toString,
      StringType,
      false)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT))

    // CH backend does not support escapeChar, so skip it here.
    val expressionNodes =
      BackendsApiManager.getTransformerApiInstance.createLikeParamList(
        leftNode,
        rightNode,
        escapeCharNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes.toList.asJava, typeNode)
  }
}

case class DecimalArithmeticExpressionTransformer(
    substraitExprName: String,
    left: ExpressionTransformer,
    right: ExpressionTransformer,
    resultType: DecimalType,
    original: Expression)
  extends ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode = left.doTransform(args)
    val rightNode = right.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT))

    val expressionNodes = Lists.newArrayList(leftNode, rightNode)
    val typeNode = ConverterUtils.getTypeNode(resultType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}
