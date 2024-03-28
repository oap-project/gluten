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
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions._

import com.google.common.collect.Lists

case class CreateMapTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    useStringTypeWhenEmpty: Boolean,
    original: CreateMap)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // If children is empty,
    // transformation is only supported when useStringTypeWhenEmpty is false
    // because ClickHouse and Velox currently doesn't support this config.
    if (children.isEmpty && useStringTypeWhenEmpty) {
      throw new GlutenNotSupportException(s"$original not supported yet.")
    }

    val childNodes = new java.util.ArrayList[ExpressionNode]()
    children.foreach(
      child => {
        val childNode = child.doTransform(args)
        childNodes.add(childNode)
      })

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      substraitExprName,
      original.children.map(_.dataType),
      FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode)
  }
}

case class GetMapValueTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    key: ExpressionTransformer,
    failOnError: Boolean,
    original: GetMapValue)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (BackendsApiManager.getSettings.alwaysFailOnMapExpression()) {
      throw new GlutenNotSupportException(s"$original not supported yet.")
    }

    if (failOnError) {
      throw new GlutenNotSupportException(s"$original not supported yet.")
    }

    val childNode = child.doTransform(args)
    val keyNode = key.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      substraitExprName,
      Seq(original.child.dataType, original.key.dataType),
      FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val exprNodes = Lists.newArrayList(childNode, keyNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, exprNodes, typeNode)
  }
}
