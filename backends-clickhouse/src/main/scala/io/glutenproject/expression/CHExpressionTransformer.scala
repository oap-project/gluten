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

import io.glutenproject.backendsapi.clickhouse.CHBackendSettings
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`._
import io.glutenproject.substrait.expression._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.google.common.collect.Lists

import java.util.Locale

case class CHSizeExpressionTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Size)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // Pass legacyLiteral as second argument in substrait function
    val legacyLiteral = new Literal(original.legacySizeOfNull, BooleanType)
    val legacyTransformer = new LiteralTransformer(legacyLiteral)
    GenericExpressionTransformer(substraitExprName, Seq(child, legacyTransformer), original)
      .doTransform(args)
  }
}

case class CHTruncTimestampTransformer(
    substraitExprName: String,
    format: ExpressionTransformer,
    timestamp: ExpressionTransformer,
    timeZoneId: Option[String] = None,
    original: TruncTimestamp)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // The format must be constant string in the function date_trunc of ch.
    if (!original.format.foldable) {
      throw new UnsupportedOperationException(
        s"The format ${original.format} must be constant string.")
    }

    val formatStr = original.format.eval().asInstanceOf[UTF8String]
    if (formatStr == null) {
      throw new UnsupportedOperationException("The format is null.")
    }

    val (newFormatStr, timeZoneIgnore) = formatStr.toString.toLowerCase(Locale.ROOT) match {
      case "second" => ("second", false)
      case "minute" => ("minute", false)
      case "hour" => ("hour", false)
      case "day" | "dd" => ("day", false)
      case "week" => ("week", true)
      case "mon" | "month" | "mm" => ("month", true)
      case "quarter" => ("quarter", true)
      case "year" | "yyyy" | "yy" => ("year", true)
      // Can not support now.
      // case "microsecond" => "microsecond"
      // case "millisecond" => "millisecond"
      case _ => throw new UnsupportedOperationException(s"The format $formatStr is invalidate.")
    }

    // Currently, data_trunc function can not support to set the specified timezone,
    // which is different with session_time_zone.
    if (
      timeZoneIgnore && timeZoneId.nonEmpty &&
      !timeZoneId.get.equalsIgnoreCase(
        SQLConf.get.getConfString(
          s"${CHBackendSettings.getBackendConfigPrefix}.runtime_config.timezone")
      )
    ) {
      throw new UnsupportedOperationException(
        s"It doesn't support trunc the format $newFormatStr with the specified timezone " +
          s"${timeZoneId.get}.")
    }

    val timestampNode = timestamp.doTransform(args)
    val lowerFormatNode = ExpressionBuilder.makeStringLiteral(newFormatStr)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    val dataTypes = if (timeZoneId.nonEmpty) {
      Seq(original.format.dataType, original.timestamp.dataType, StringType)
    } else {
      Seq(original.format.dataType, original.timestamp.dataType)
    }

    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, dataTypes))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(lowerFormatNode)
    expressionNodes.add(timestampNode)
    if (timeZoneId.isDefined) {
      expressionNodes.add(ExpressionBuilder.makeStringLiteral(timeZoneId.get))
    }

    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class CHStringTranslateTransformer(
    substraitExprName: String,
    srcExpr: ExpressionTransformer,
    matchingExpr: ExpressionTransformer,
    replaceExpr: ExpressionTransformer,
    original: StringTranslate)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // In CH, translateUTF8 requires matchingExpr and replaceExpr argument have the same length
    val matchingNode = matchingExpr.doTransform(args)
    val replaceNode = replaceExpr.doTransform(args)
    if (
      !matchingNode.isInstanceOf[StringLiteralNode] ||
      !replaceNode.isInstanceOf[StringLiteralNode]
    ) {
      throw new UnsupportedOperationException(s"$original not supported yet.")
    }

    val matchingLiteral = matchingNode.asInstanceOf[StringLiteralNode].getValue
    val replaceLiteral = replaceNode.asInstanceOf[StringLiteralNode].getValue
    if (matchingLiteral.length() != replaceLiteral.length()) {
      throw new UnsupportedOperationException(s"$original not supported yet.")
    }

    GenericExpressionTransformer(
      substraitExprName,
      Seq(srcExpr, matchingExpr, replaceExpr),
      original)
      .doTransform(args)
  }
}

case class CHStringLocateTransformer(
    substraitExprName: String,
    substrExpr: ExpressionTransformer,
    strExpr: ExpressionTransformer,
    startExpr: ExpressionTransformer,
    original: StringLocate)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val substrNode = substrExpr.doTransform(args)
    val strNode = strExpr.doTransform(args)
    val startNode = startExpr.doTransform(args)

    // Special Case
    // In Spark, return 0 when start_pos is null
    // but when start_pos is not null, return null if either str or substr is null
    // so we need convert it to if(isnull(start_pos), 0, position(substr, str, start_pos)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val locateFuncName = ConverterUtils.makeFuncName(
      substraitExprName,
      original.children.map(_.dataType),
      FunctionConfig.OPT)
    val locateFuncId = ExpressionBuilder.newScalarFunction(functionMap, locateFuncName)
    val exprNodes = Lists.newArrayList(substrNode, strNode, startNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    val locateFuncNode = ExpressionBuilder.makeScalarFunction(locateFuncId, exprNodes, typeNode)

    // isnull(start_pos)
    val isnullFuncName =
      ConverterUtils.makeFuncName(ExpressionNames.IS_NULL, Seq(IntegerType), FunctionConfig.OPT)
    val isnullFuncId = ExpressionBuilder.newScalarFunction(functionMap, isnullFuncName)
    val isnullNode = ExpressionBuilder.makeScalarFunction(
      isnullFuncId,
      Lists.newArrayList(startNode),
      TypeBuilder.makeBoolean(false))

    new IfThenNode(
      Lists.newArrayList(isnullNode),
      Lists.newArrayList(new IntLiteralNode(0)),
      locateFuncNode)
  }
}

case class CHPosExplodeTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: PosExplode,
    attributeSeq: Seq[Attribute])
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode: ExpressionNode = child.doTransform(args)

    // sequence(0, size(array_or_map)-1)
    val startExpr = new Literal(0, IntegerType)
    val stopExpr = new Subtract(Size(original.child, false), Literal(1, IntegerType))
    val stepExpr = new Literal(1, IntegerType)
    val sequenceExpr = new Sequence(startExpr, stopExpr, stepExpr)
    val sequenceExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(sequenceExpr, attributeSeq)
      .doTransform(args)

    val funcMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    // map_from_arrays_unaligned(sequence(0, size(array_or_map)-1), array_or_map)
    val mapFromArraysUnalignedFuncId = ExpressionBuilder.newScalarFunction(
      funcMap,
      ConverterUtils.makeFuncName(
        "map_from_arrays_unaligned",
        Seq(sequenceExpr.dataType, original.child.dataType),
        FunctionConfig.OPT))

    // Notice that in CH mapFromArraysUnaligned accepts the second arguments as MapType or ArrayType
    // But in Spark, it accepts ArrayType.
    val keyType = IntegerType
    val (valType, valContainsNull) = original.child.dataType match {
      case a: ArrayType => (a.elementType, a.containsNull)
      case m: MapType =>
        (
          StructType(
            StructField("", m.keyType, false) ::
              StructField("", m.valueType, m.valueContainsNull) :: Nil),
          false)
      case _ =>
        throw new UnsupportedOperationException(
          s"posexplode(${original.child.dataType}) not supported yet.")
    }
    val outputType = MapType(keyType, valType, valContainsNull)
    val mapFromArraysUnalignedExprNode = ExpressionBuilder.makeScalarFunction(
      mapFromArraysUnalignedFuncId,
      Lists.newArrayList(sequenceExprNode, childNode),
      ConverterUtils.getTypeNode(outputType, original.child.nullable))

    // posexplode(map_from_arrays_unaligned(sequence(0, size(array_or_map)-1), array_or_map))
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
          Lists.newArrayList(mapFromArraysUnalignedExprNode),
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
          Lists.newArrayList(mapFromArraysUnalignedExprNode),
          ConverterUtils.getTypeNode(structType, false))
      case _ =>
        throw new UnsupportedOperationException(s"posexplode($childType) not supported yet.")
    }
  }
}
