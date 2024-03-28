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

import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.google.common.collect.Lists

case class AliasTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.REQ))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class NamedLambdaVariableTransformer(
    substraitExprName: String,
    name: String,
    dataType: DataType,
    nullable: Boolean,
    exprId: ExprId)
  extends ExpressionTransformer {
  override def doTransform(args: Object): ExpressionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val namedLambdaVarFunctionName =
      ConverterUtils.makeFuncName(substraitExprName, Seq(dataType), FunctionConfig.OPT)
    val arrayAggFunctionId =
      ExpressionBuilder.newScalarFunction(functionMap, namedLambdaVarFunctionName)
    val exprNodes = Lists.newArrayList(
      ExpressionBuilder.makeLiteral(name, StringType, false).asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    // namedlambdavariable('acc')-> <Integer, notnull>
    ExpressionBuilder.makeScalarFunction(arrayAggFunctionId, exprNodes, typeNode)
  }
}

case class AttributeReferenceTransformer(
    name: String,
    ordinal: Int,
    dataType: DataType,
    nullable: Boolean = true,
    exprId: ExprId,
    qualifier: Seq[String],
    metadata: Metadata = Metadata.empty)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    ExpressionBuilder.makeSelection(ordinal.asInstanceOf[java.lang.Integer])
  }
}
