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
package io.glutenproject.expression

import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.expression.ExpressionConverter.replaceWithExpressionTransformer
import io.glutenproject.substrait.expression._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegerType, LongType}

import com.google.common.collect.Lists

import java.lang.{Integer => JInteger, Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

import scala.language.existentials

case class VeloxAliasTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

case class VeloxNamedStructTransformer(
    substraitExprName: String,
    original: CreateNamedStruct,
    attributeSeq: Seq[Attribute])
  extends ExpressionTransformer {
  override def doTransform(args: Object): ExpressionNode = {
    val expressionNodes = Lists.newArrayList[ExpressionNode]()
    original.valExprs.foreach(
      child =>
        expressionNodes.add(
          replaceWithExpressionTransformer(child, attributeSeq).doTransform(args)))
    val functionMap = args.asInstanceOf[JHashMap[String, JLong]]
    val functionName = ConverterUtils
      .makeFuncName(substraitExprName, Seq(original.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class VeloxGetStructFieldTransformer(
    substraitExprName: String,
    childTransformer: ExpressionTransformer,
    ordinal: Int,
    original: GetStructField)
  extends ExpressionTransformer {
  override def doTransform(args: Object): ExpressionNode = {
    val childNode = childTransformer.doTransform(args)
    childNode match {
      case node: StructLiteralNode =>
        node.getFieldLiteral(ordinal)
      case node: SelectionNode =>
        // Append the nested index to selection node.
        node.addNestedChildIdx(JInteger.valueOf(ordinal))
      case other =>
        throw new UnsupportedOperationException(s"$other is not supported.")
    }
  }
}

case class VeloxHashExpressionTransformer(
    substraitExprName: String,
    exps: Seq[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // As of Spark 3.3, there are 3 kinds of HashExpression.
    // HiveHash is not supported in native backend and will fail native validation.
    val (seedNode, seedType) = original match {
      case XxHash64(_, seed) =>
        (ExpressionBuilder.makeLongLiteral(seed), LongType)
      case Murmur3Hash(_, seed) =>
        (ExpressionBuilder.makeIntLiteral(seed), IntegerType)
      case HiveHash(_) =>
        (ExpressionBuilder.makeIntLiteral(0), IntegerType)
    }
    val nodes = new JArrayList[ExpressionNode]()
    // Seed as the first argument
    nodes.add(seedNode)
    exps.foreach(
      expression => {
        nodes.add(expression.doTransform(args))
      })
    val childrenTypes = seedType +: original.children.map(child => child.dataType)
    val functionMap = args.asInstanceOf[JHashMap[String, JLong]]
    val functionName =
      ConverterUtils.makeFuncName(substraitExprName, childrenTypes, FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, nodes, typeNode)
  }
}
