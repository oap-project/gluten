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
import org.apache.gluten.substrait.`type`.ListNode
import org.apache.gluten.substrait.`type`.MapNode
import org.apache.gluten.substrait.expression.{BooleanLiteralNode, ExpressionBuilder, ExpressionNode, IntLiteralNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.google.common.collect.Lists

case class ChildTransformer(child: ExpressionTransformer) extends ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

case class CastTransformer(
    child: ExpressionTransformer,
    datatype: DataType,
    timeZoneId: Option[String],
    original: Cast)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val typeNode = ConverterUtils.getTypeNode(datatype, original.nullable)
    ExpressionBuilder.makeCast(typeNode, child.doTransform(args), original.ansiEnabled)
  }
}

case class ExplodeTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Explode)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode: ExpressionNode = child.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, Seq(original.child.dataType)))

    val expressionNodes = Lists.newArrayList(childNode)
    val childTypeNode = ConverterUtils.getTypeNode(original.child.dataType, original.child.nullable)
    childTypeNode match {
      case l: ListNode =>
        ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, l.getNestedType)
      case m: MapNode =>
        ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, m.getNestedType)
      case _ =>
        throw new GlutenNotSupportException(s"explode($childTypeNode) not supported yet.")
    }
  }
}

case class PosExplodeTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: PosExplode,
    attributeSeq: Seq[Attribute])
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode: ExpressionNode = child.doTransform(args)

    // sequence(1, size(array_or_map))
    val startExpr = new Literal(1, IntegerType)
    val stopExpr = new Size(Size(original.child, false))
    val stepExpr = new Literal(1, IntegerType)
    val sequenceExpr = new Sequence(startExpr, stopExpr, stepExpr)
    val sequenceExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(sequenceExpr, attributeSeq)
      .doTransform(args)

    val funcMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    val mapFromArraysFuncId = ExpressionBuilder.newScalarFunction(
      funcMap,
      ConverterUtils.makeFuncName(
        ExpressionNames.MAP_FROM_ARRAYS,
        Seq(sequenceExpr.dataType, original.child.dataType),
        FunctionConfig.OPT))

    val keyType = IntegerType
    val (valType, valContainsNull) = original.child.dataType match {
      case a: ArrayType => (a.elementType, a.containsNull)
      case _ =>
        throw new GlutenNotSupportException(
          s"posexplode(${original.child.dataType}) not supported yet.")
    }
    val outputType = MapType(keyType, valType, valContainsNull)
    val mapFromArraysExprNode = ExpressionBuilder.makeScalarFunction(
      mapFromArraysFuncId,
      Lists.newArrayList(sequenceExprNode, childNode),
      ConverterUtils.getTypeNode(outputType, original.child.nullable))

    // posexplode(map_from_arrays(sequence(1, size(array_or_map)), array_or_map))
    val funcId = ExpressionBuilder.newScalarFunction(
      funcMap,
      ConverterUtils.makeFuncName(ExpressionNames.POSEXPLODE, Seq(outputType), FunctionConfig.OPT))

    val childType = original.child.dataType
    childType match {
      case a: ArrayType =>
        // Output pos, col when input is array
        val structType = StructType(
          Array(
            StructField("pos", IntegerType, false),
            StructField("col", a.elementType, a.containsNull)))
        ExpressionBuilder.makeScalarFunction(
          funcId,
          Lists.newArrayList(mapFromArraysExprNode),
          ConverterUtils.getTypeNode(structType, false))
      case m: MapType =>
        // Output pos, key, value when input is map
        val structType = StructType(
          Array(
            StructField("pos", IntegerType, false),
            StructField("key", m.keyType, false),
            StructField("value", m.valueType, m.valueContainsNull)))
        ExpressionBuilder.makeScalarFunction(
          funcId,
          Lists.newArrayList(mapFromArraysExprNode),
          ConverterUtils.getTypeNode(structType, false))
      case _ =>
        throw new GlutenNotSupportException(s"posexplode($childType) not supported yet.")
    }
  }
}

case class CheckOverflowTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: CheckOverflow)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    BackendsApiManager.getTransformerApiInstance.createCheckOverflowExprNode(
      args,
      substraitExprName,
      child.doTransform(args),
      original.dataType,
      original.nullable,
      original.nullOnOverflow)
  }
}

case class MakeDecimalTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: MakeDecimal)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        Seq(original.dataType, BooleanType),
        FunctionConfig.OPT))

    val expressionNodes =
      Lists.newArrayList(childNode, new BooleanLiteralNode(original.nullOnOverflow))
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

/**
 * User can specify a seed for this function. If lacked, spark will generate a random number as
 * seed. We also need to pass a unique partitionIndex provided by framework to native library for
 * each thread. Then, seed plus partitionIndex will be the actual seed for generator, similar to
 * vanilla spark. This is based on the fact that partitioning is deterministic and one partition is
 * corresponding to one task thread.
 */
case class RandTransformer(
    substraitExprName: String,
    explicitSeed: ExpressionTransformer,
    original: Rand)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (!original.hideSeed) {
      // TODO: for user-specified seed, we need to pass partition index to native engine.
      throw new GlutenNotSupportException("User-specified seed is not supported.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, Seq(original.child.dataType)))
    val inputNodes = Lists.newArrayList[ExpressionNode]()
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, inputNodes, typeNode)
  }
}

case class GetArrayStructFieldsTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    ordinal: Int,
    numFields: Int,
    containsNull: Boolean,
    original: GetArrayStructFields)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        Seq(original.child.dataType, IntegerType),
        FunctionConfig.OPT))
    val inputNodes =
      Lists.newArrayList(child.doTransform(args), new IntLiteralNode(ordinal))
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, inputNodes, typeNode)
  }
}
