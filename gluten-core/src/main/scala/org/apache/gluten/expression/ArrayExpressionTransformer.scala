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
import org.apache.spark.sql.types._

import com.google.common.collect.Lists

import scala.collection.JavaConverters._

case class CreateArrayTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    useStringTypeWhenEmpty: Boolean,
    original: CreateArray)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // If children is empty,
    // transformation is only supported when useStringTypeWhenEmpty is false
    // because ClickHouse and Velox currently doesn't support this config.
    if (useStringTypeWhenEmpty && children.isEmpty) {
      throw new GlutenNotSupportException(s"$original not supported yet.")
    }

    val childNodes = children.map(_.doTransform(args)).asJava

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

case class GetArrayItemTransformer(
    substraitExprName: String,
    left: ExpressionTransformer,
    right: ExpressionTransformer,
    failOnError: Boolean,
    original: GetArrayItem)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // Ignore failOnError for clickhouse backend
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val leftNode = left.doTransform(args)
    var rightNode = right.doTransform(args)

    // In Spark, the index of getarrayitem starts from 0
    // But in CH and velox, the index of arrayElement starts from 1, besides index argument must
    // So we need to do transform: rightNode = add(rightNode, 1)
    val addFunctionName = ConverterUtils.makeFuncName(
      ExpressionNames.ADD,
      Seq(IntegerType, original.right.dataType),
      FunctionConfig.OPT)
    val addFunctionId = ExpressionBuilder.newScalarFunction(functionMap, addFunctionName)
    val literalNode = ExpressionBuilder.makeLiteral(1.toInt, IntegerType, false)
    rightNode = ExpressionBuilder.makeScalarFunction(
      addFunctionId,
      Lists.newArrayList(literalNode, rightNode),
      ConverterUtils.getTypeNode(original.right.dataType, original.right.nullable))

    BackendsApiManager.getSparkPlanExecApiInstance.genGetArrayItemExpressionNode(
      substraitExprName,
      functionMap,
      leftNode,
      rightNode,
      original
    )
  }
}
