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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.exception.GlutenNotSupportException
import io.glutenproject.execution.{ColumnarToRowExecBase, WholeStageTransformer}
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.test.TestStats
import io.glutenproject.utils.{DecimalArithmeticUtil, PlanUtil}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution.{ScalarSubquery, _}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.hive.HiveUDFTransformer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

trait Transformable extends Expression {
  def getTransformer(childrenTransformers: Seq[ExpressionTransformer]): ExpressionTransformer

  override def eval(input: InternalRow): Any = throw new UnsupportedOperationException()

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException()
}

object ExpressionConverter extends SQLConfHelper with Logging {

  def replaceWithExpressionTransformer(
      exprs: Seq[Expression],
      attributeSeq: Seq[Attribute]): Seq[ExpressionTransformer] = {
    val expressionsMap = ExpressionMappings.expressionsMap
    exprs.map {
      expr => replaceWithExpressionTransformerInternal(expr, attributeSeq, expressionsMap)
    }.toSeq
  }

  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    val expressionsMap = ExpressionMappings.expressionsMap
    replaceWithExpressionTransformerInternal(expr, attributeSeq, expressionsMap)
  }

  private def replacePythonUDFWithExpressionTransformer(
      udf: PythonUDF,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    val substraitExprName = UDFMappings.pythonUDFMap.get(udf.name)
    substraitExprName match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          udf.children.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          udf)
      case _ =>
        throw new GlutenNotSupportException(s"Not supported python udf: $udf.")
    }
  }

  private def replaceScalaUDFWithExpressionTransformer(
      udf: ScalaUDF,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    if (!udf.udfName.isDefined) {
      throw new GlutenNotSupportException("UDF name is not found!")
    }
    val substraitExprName = UDFMappings.scalaUDFMap.get(udf.udfName.get)
    substraitExprName match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          udf.children.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          udf)
      case _ =>
        throw new GlutenNotSupportException(s"Not supported scala udf: $udf.")
    }
  }

  private def replaceWithExpressionTransformerInternal(
      expr: Expression,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    logDebug(
      s"replaceWithExpressionTransformer expr: $expr class: ${expr.getClass} " +
        s"name: ${expr.prettyName}")

    expr match {
      case p: PythonUDF =>
        return replacePythonUDFWithExpressionTransformer(p, attributeSeq, expressionsMap)
      case s: ScalaUDF =>
        return replaceScalaUDFWithExpressionTransformer(s, attributeSeq, expressionsMap)
      case _ if HiveUDFTransformer.isHiveUDF(expr) =>
        return HiveUDFTransformer.replaceWithExpressionTransformer(expr, attributeSeq)
      case i: StaticInvoke =>
        val objectName = i.staticObject.getName.stripSuffix("$")
        if (objectName.endsWith("UrlCodec")) {
          val child = i.arguments(0)
          i.functionName match {
            case "decode" =>
              return GenericExpressionTransformer(
                ExpressionNames.URL_DECODE,
                child.map(
                  replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
                i)
          }
        }
      case _ =>
    }

    TestStats.addExpressionClassName(expr.getClass.getName)
    // Check whether Gluten supports this expression
    val substraitExprNameOpt = expressionsMap.get(expr.getClass)
    if (substraitExprNameOpt.isEmpty) {
      throw new GlutenNotSupportException(
        s"Not supported to map spark function name" +
          s" to substrait function name: $expr, class name: ${expr.getClass.getSimpleName}.")
    }
    val substraitExprName = substraitExprNameOpt.get

    // Check whether each backend supports this expression
    if (!BackendsApiManager.getValidatorApiInstance.doExprValidate(substraitExprName, expr)) {
      throw new GlutenNotSupportException(s"Not supported: $expr.")
    }
    expr match {
      case extendedExpr
          if ExpressionMappings.expressionExtensionTransformer.extensionExpressionsMapping.contains(
            extendedExpr.getClass) =>
        // Use extended expression transformer to replace custom expression first
        ExpressionMappings.expressionExtensionTransformer
          .replaceWithExtensionExpressionTransformer(substraitExprName, extendedExpr, attributeSeq)
      case c: CreateArray =>
        val children =
          c.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap))
        CreateArrayTransformer(substraitExprName, children, true, c)
      case g: GetArrayItem =>
        GetArrayItemTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(g.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(g.right, attributeSeq, expressionsMap),
          g.failOnError,
          g
        )
      case c: CreateMap =>
        val children =
          c.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap))
        CreateMapTransformer(substraitExprName, children, c.useStringTypeWhenEmpty, c)
      case g: GetMapValue =>
        BackendsApiManager.getSparkPlanExecApiInstance.genGetMapValueTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(g.child, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(g.key, attributeSeq, expressionsMap),
          g
        )
      case m: MapEntries =>
        BackendsApiManager.getSparkPlanExecApiInstance.genMapEntriesTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(m.child, attributeSeq, expressionsMap),
          m)
      case e: Explode =>
        ExplodeTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(e.child, attributeSeq, expressionsMap),
          e)
      case p: PosExplode =>
        BackendsApiManager.getSparkPlanExecApiInstance.genPosExplodeTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(p.child, attributeSeq, expressionsMap),
          p,
          attributeSeq)
      case i: Inline =>
        BackendsApiManager.getSparkPlanExecApiInstance.genInlineTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(i.child, attributeSeq, expressionsMap),
          i)
      case a: Alias =>
        BackendsApiManager.getSparkPlanExecApiInstance.genAliasTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.child, attributeSeq, expressionsMap),
          a)
      case a: AttributeReference =>
        if (attributeSeq == null) {
          throw new UnsupportedOperationException(s"attributeSeq should not be null.")
        }
        try {
          val bindReference =
            BindReferences.bindReference(expr, attributeSeq, allowFailures = false)
          val b = bindReference.asInstanceOf[BoundReference]
          AttributeReferenceTransformer(
            a.name,
            b.ordinal,
            a.dataType,
            b.nullable,
            a.exprId,
            a.qualifier,
            a.metadata)
        } catch {
          case e: IllegalStateException =>
            // This situation may need developers to fix, although we just throw the below
            // exception to let the corresponding operator fall back.
            throw new UnsupportedOperationException(
              s"Failed to bind reference for $expr: ${e.getMessage}")
        }
      case b: BoundReference =>
        BoundReferenceTransformer(b.ordinal, b.dataType, b.nullable)
      case l: Literal =>
        LiteralTransformer(l)
      case d: DateDiff =>
        DateDiffTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(d.endDate, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(d.startDate, attributeSeq, expressionsMap),
          d
        )
      case r: Round if r.child.dataType.isInstanceOf[DecimalType] =>
        BackendsApiManager.getSparkPlanExecApiInstance.genDecimalRoundTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(r.child, attributeSeq, expressionsMap),
          r
        )
      case t: ToUnixTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genUnixTimestampTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(t.timeExp, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(t.format, attributeSeq, expressionsMap),
          t
        )
      case u: UnixTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genUnixTimestampTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(u.timeExp, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(u.format, attributeSeq, expressionsMap),
          ToUnixTimestamp(u.timeExp, u.format, u.timeZoneId, u.failOnError)
        )
      case t: TruncTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTruncTimestampTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(t.format, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(t.timestamp, attributeSeq, expressionsMap),
          t.timeZoneId,
          t
        )
      case m: MonthsBetween =>
        MonthsBetweenTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(m.date1, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(m.date2, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(m.roundOff, attributeSeq, expressionsMap),
          m.timeZoneId,
          m
        )
      case i: If =>
        IfTransformer(
          replaceWithExpressionTransformerInternal(i.predicate, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(i.trueValue, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(i.falseValue, attributeSeq, expressionsMap),
          i
        )
      case cw: CaseWhen =>
        CaseWhenTransformer(
          cw.branches.map {
            expr =>
              {
                (
                  replaceWithExpressionTransformerInternal(expr._1, attributeSeq, expressionsMap),
                  replaceWithExpressionTransformerInternal(expr._2, attributeSeq, expressionsMap))
              }
          },
          cw.elseValue.map {
            expr =>
              {
                replaceWithExpressionTransformerInternal(expr, attributeSeq, expressionsMap)
              }
          },
          cw
        )
      case i: In =>
        if (i.list.exists(!_.foldable)) {
          throw new GlutenNotSupportException(
            s"In list option does not support non-foldable expression, ${i.list.map(_.sql)}")
        }
        InTransformer(
          replaceWithExpressionTransformerInternal(i.value, attributeSeq, expressionsMap),
          i.list,
          i.value.dataType,
          i)
      case i: InSet =>
        InSetTransformer(
          replaceWithExpressionTransformerInternal(i.child, attributeSeq, expressionsMap),
          i.hset,
          i.child.dataType,
          i)
      case s: org.apache.spark.sql.execution.ScalarSubquery =>
        ScalarSubqueryTransformer(s.plan, s.exprId, s)
      case c: Cast =>
        // Add trim node, as necessary.
        val newCast =
          BackendsApiManager.getSparkPlanExecApiInstance.genCastWithNewChild(c)
        CastTransformer(
          replaceWithExpressionTransformerInternal(newCast.child, attributeSeq, expressionsMap),
          newCast.dataType,
          newCast.timeZoneId,
          newCast)
      case s: String2TrimExpression =>
        val (srcStr, trimStr) = s match {
          case StringTrim(srcStr, trimStr) => (srcStr, trimStr)
          case StringTrimLeft(srcStr, trimStr) => (srcStr, trimStr)
          case StringTrimRight(srcStr, trimStr) => (srcStr, trimStr)
        }
        String2TrimExpressionTransformer(
          substraitExprName,
          trimStr.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          replaceWithExpressionTransformerInternal(srcStr, attributeSeq, expressionsMap),
          s
        )
      case m: HashExpression[_] =>
        BackendsApiManager.getSparkPlanExecApiInstance.genHashExpressionTransformer(
          substraitExprName,
          m.children.map(
            expr => replaceWithExpressionTransformerInternal(expr, attributeSeq, expressionsMap)),
          m)
      case getStructField: GetStructField =>
        // Different backends may have different result.
        BackendsApiManager.getSparkPlanExecApiInstance.genGetStructFieldTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(
            getStructField.child,
            attributeSeq,
            expressionsMap),
          getStructField.ordinal,
          getStructField)
      case getArrayStructFields: GetArrayStructFields =>
        GetArrayStructFieldsTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(
            getArrayStructFields.child,
            attributeSeq,
            expressionsMap),
          getArrayStructFields.ordinal,
          getArrayStructFields.numFields,
          getArrayStructFields.containsNull,
          getArrayStructFields
        )
      case t: StringTranslate =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringTranslateTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(t.srcExpr, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(t.matchingExpr, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(t.replaceExpr, attributeSeq, expressionsMap),
          t
        )
      case l: StringLocate =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringLocateTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(l.first, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(l.second, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(l.third, attributeSeq, expressionsMap),
          l
        )
      case s: StringSplit =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringSplitTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(s.str, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(s.regex, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(s.limit, attributeSeq, expressionsMap),
          s
        )
      case r: RegExpReplace =>
        BackendsApiManager.getSparkPlanExecApiInstance.genRegexpReplaceTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformerInternal(r.subject, attributeSeq, expressionsMap),
            replaceWithExpressionTransformerInternal(r.regexp, attributeSeq, expressionsMap),
            replaceWithExpressionTransformerInternal(r.rep, attributeSeq, expressionsMap),
            replaceWithExpressionTransformerInternal(r.pos, attributeSeq, expressionsMap)
          ),
          r
        )
      case equal: EqualNullSafe =>
        BackendsApiManager.getSparkPlanExecApiInstance.genEqualNullSafeTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(equal.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(equal.right, attributeSeq, expressionsMap),
          equal
        )
      case md5: Md5 =>
        BackendsApiManager.getSparkPlanExecApiInstance.genMd5Transformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(md5.child, attributeSeq, expressionsMap),
          md5)
      case sha1: Sha1 =>
        BackendsApiManager.getSparkPlanExecApiInstance.genSha1Transformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(sha1.child, attributeSeq, expressionsMap),
          sha1)
      case sha2: Sha2 =>
        BackendsApiManager.getSparkPlanExecApiInstance.genSha2Transformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(sha2.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(sha2.right, attributeSeq, expressionsMap),
          sha2
        )
      case size: Size =>
        BackendsApiManager.getSparkPlanExecApiInstance.genSizeExpressionTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(size.child, attributeSeq, expressionsMap),
          size)
      case namedStruct: CreateNamedStruct =>
        BackendsApiManager.getSparkPlanExecApiInstance.genNamedStructTransformer(
          substraitExprName,
          namedStruct.children.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          namedStruct,
          attributeSeq)
      case namedLambdaVariable: NamedLambdaVariable =>
        NamedLambdaVariableTransformer(
          substraitExprName,
          name = namedLambdaVariable.name,
          dataType = namedLambdaVariable.dataType,
          nullable = namedLambdaVariable.nullable,
          exprId = namedLambdaVariable.exprId
        )
      case lambdaFunction: LambdaFunction =>
        LambdaFunctionTransformer(
          substraitExprName,
          function = replaceWithExpressionTransformerInternal(
            lambdaFunction.function,
            attributeSeq,
            expressionsMap),
          arguments = lambdaFunction.arguments.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          hidden = false,
          original = lambdaFunction
        )
      case j: JsonTuple =>
        val children =
          j.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap))
        JsonTupleExpressionTransformer(substraitExprName, children, j)
      case l: Like =>
        LikeTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(l.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(l.right, attributeSeq, expressionsMap),
          l
        )
      case c: CheckOverflow =>
        CheckOverflowTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(c.child, attributeSeq, expressionsMap),
          c)
      case m: MakeDecimal =>
        MakeDecimalTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(m.child, attributeSeq, expressionsMap),
          m)
      case rand: Rand =>
        BackendsApiManager.getSparkPlanExecApiInstance.genRandTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(rand.child, attributeSeq, expressionsMap),
          rand)
      case _: KnownFloatingPointNormalized | _: NormalizeNaNAndZero | _: PromotePrecision =>
        ChildTransformer(
          replaceWithExpressionTransformerInternal(expr.children.head, attributeSeq, expressionsMap)
        )
      case _: GetDateField | _: GetTimeField =>
        ExtractDateTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(
            expr.children.head,
            attributeSeq,
            expressionsMap),
          expr)
      case _: StringToMap =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringToMapTransformer(
          substraitExprName,
          expr.children.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          expr)
      case b: BinaryArithmetic if DecimalArithmeticUtil.isDecimalArithmetic(b) =>
        // PrecisionLoss=true: velox support / ch not support
        // PrecisionLoss=false: velox not support / ch support
        // TODO ch support PrecisionLoss=true
        if (!BackendsApiManager.getSettings.allowDecimalArithmetic) {
          throw new GlutenNotSupportException(
            s"Not support ${SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key} " +
              s"${conf.decimalOperationsAllowPrecisionLoss} mode")
        }
        val rescaleBinary = if (BackendsApiManager.getSettings.rescaleDecimalLiteral) {
          DecimalArithmeticUtil.rescaleLiteral(b)
        } else {
          b
        }
        val (left, right) = DecimalArithmeticUtil.rescaleCastForDecimal(
          DecimalArithmeticUtil.removeCastForDecimal(rescaleBinary.left),
          DecimalArithmeticUtil.removeCastForDecimal(rescaleBinary.right))
        val leftChild = replaceWithExpressionTransformerInternal(left, attributeSeq, expressionsMap)
        val rightChild =
          replaceWithExpressionTransformerInternal(right, attributeSeq, expressionsMap)

        val resultType = DecimalArithmeticUtil.getResultTypeForOperation(
          DecimalArithmeticUtil.getOperationType(b),
          DecimalArithmeticUtil
            .getResultType(leftChild)
            .getOrElse(left.dataType.asInstanceOf[DecimalType]),
          DecimalArithmeticUtil
            .getResultType(rightChild)
            .getOrElse(right.dataType.asInstanceOf[DecimalType])
        )
        DecimalArithmeticExpressionTransformer(
          substraitExprName,
          leftChild,
          rightChild,
          resultType,
          b)
      case n: NaNvl =>
        BackendsApiManager.getSparkPlanExecApiInstance.genNaNvlTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(n.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(n.right, attributeSeq, expressionsMap),
          n
        )
      case m: MakeTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genMakeTimestampTransformer(
          substraitExprName,
          m.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          m)
      case timestampAdd if timestampAdd.getClass.getSimpleName.equals("TimestampAdd") =>
        // for spark3.3
        val extract = SparkShimLoader.getSparkShims.extractExpressionTimestampAddUnit(timestampAdd)
        if (extract.isEmpty) {
          throw new UnsupportedOperationException(s"Not support expression TimestampAdd.")
        }
        val add = timestampAdd.asInstanceOf[BinaryExpression]
        TimestampAddTransform(
          substraitExprName,
          extract.get.head,
          replaceWithExpressionTransformerInternal(add.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(add.right, attributeSeq, expressionsMap),
          extract.get.last,
          add.dataType,
          add.nullable
        )
      case e: Transformable =>
        val childrenTransformers =
          e.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap))
        e.getTransformer(childrenTransformers)
      case u: Uuid =>
        BackendsApiManager.getSparkPlanExecApiInstance.genUuidTransformer(substraitExprName, u)
      case expr =>
        GenericExpressionTransformer(
          substraitExprName,
          expr.children.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          expr
        )
    }
  }

  /**
   * Transform BroadcastExchangeExec to ColumnarBroadcastExchangeExec in DynamicPruningExpression.
   *
   * @param partitionFilters
   *   The partition filter of Scan
   * @return
   *   Transformed partition filter
   */
  def transformDynamicPruningExpr(partitionFilters: Seq[Expression]): Seq[Expression] = {

    def convertBroadcastExchangeToColumnar(
        exchange: BroadcastExchangeExec): ColumnarBroadcastExchangeExec = {
      val newChild = exchange.child match {
        // get WholeStageTransformer directly
        case c2r: ColumnarToRowExecBase => c2r.child
        // in fallback case
        case plan: UnaryExecNode if !PlanUtil.isGlutenColumnarOp(plan) =>
          plan.child match {
            case _: ColumnarToRowExec =>
              val wholeStageTransformer = exchange.find(_.isInstanceOf[WholeStageTransformer])
              wholeStageTransformer.getOrElse(
                BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(plan))
            case _ =>
              BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(plan)
          }
      }
      ColumnarBroadcastExchangeExec(exchange.mode, newChild)
    }

    if (
      GlutenConfig.getConf.enableScanOnly || !GlutenConfig.getConf.enableColumnarBroadcastExchange
    ) {
      // Disable ColumnarSubqueryBroadcast for scan-only execution
      // or ColumnarBroadcastExchange was disabled.
      partitionFilters
    } else {
      val newPartitionFilters = partitionFilters.map {
        case dynamicPruning: DynamicPruningExpression =>
          dynamicPruning.transform {
            // Lookup inside subqueries for duplicate exchanges.
            case in: InSubqueryExec =>
              in.plan match {
                case s: SubqueryBroadcastExec =>
                  val newIn = s
                    .transform {
                      case exchange: BroadcastExchangeExec =>
                        convertBroadcastExchangeToColumnar(exchange)
                    }
                    .asInstanceOf[SubqueryBroadcastExec]
                  val transformSubqueryBroadcast = ColumnarSubqueryBroadcastExec(
                    newIn.name,
                    newIn.index,
                    newIn.buildKeys,
                    newIn.child)

                  // When AQE is on, spark will apply ReuseAdaptiveSubquery rule first,
                  // it will reuse vanilla SubqueryBroadcastExec,
                  // and then use gluten ColumnarOverrides rule to transform Subquery,
                  // so all the SubqueryBroadcastExec in the ReusedSubqueryExec will be transformed
                  // to a new ColumnarSubqueryBroadcastExec for each SubqueryBroadcastExec,
                  // which will lead to execute ColumnarSubqueryBroadcastExec.relationFuture
                  // repeatedly even in the ReusedSubqueryExec.
                  //
                  // On the other hand, it needs to use
                  // the AdaptiveSparkPlanExec.AdaptiveExecutionContext to hold the reused map
                  // for each query.
                  newIn.child match {
                    case a: AdaptiveSparkPlanExec if SQLConf.get.subqueryReuseEnabled =>
                      // When AQE is on and reuseSubquery is on.
                      a.context.subqueryCache
                        .update(newIn.canonicalized, transformSubqueryBroadcast)
                    case _ =>
                  }
                  in.copy(plan = transformSubqueryBroadcast.asInstanceOf[BaseSubqueryExec])
                case r: ReusedSubqueryExec if r.child.isInstanceOf[SubqueryBroadcastExec] =>
                  val newIn = r.child
                    .transform {
                      case exchange: BroadcastExchangeExec =>
                        convertBroadcastExchangeToColumnar(exchange)
                    }
                    .asInstanceOf[SubqueryBroadcastExec]
                  newIn.child match {
                    case a: AdaptiveSparkPlanExec =>
                      // Only when AQE is on, it needs to replace SubqueryBroadcastExec
                      // with reused ColumnarSubqueryBroadcastExec
                      val cachedSubquery = a.context.subqueryCache.get(newIn.canonicalized)
                      if (cachedSubquery.isDefined) {
                        in.copy(plan = ReusedSubqueryExec(cachedSubquery.get))
                      } else {
                        val errMsg = "Can not get the reused ColumnarSubqueryBroadcastExec" +
                          "by the ${newIn.canonicalized}"
                        logWarning(errMsg)
                        throw new UnsupportedOperationException(errMsg)
                      }
                    case _ =>
                      val errMsg = "Can not get the reused ColumnarSubqueryBroadcastExec" +
                        "by the ${newIn.canonicalized}"
                      logWarning(errMsg)
                      throw new UnsupportedOperationException(errMsg)
                  }
                case _ => in
              }
          }
        case e: Expression => e
      }
      updateSubqueryResult(newPartitionFilters)
      newPartitionFilters
    }
  }

  private def updateSubqueryResult(partitionFilters: Seq[Expression]): Unit = {
    // When it includes some DynamicPruningExpression,
    // it needs to execute InSubqueryExec first,
    // because doTransform path can't execute 'doExecuteColumnar' which will
    // execute prepare subquery first.
    partitionFilters.foreach {
      case DynamicPruningExpression(inSubquery: InSubqueryExec) =>
        if (inSubquery.values().isEmpty) inSubquery.updateResult()
      case e: Expression =>
        e.foreach {
          case s: ScalarSubquery => s.updateResult()
          case _ =>
        }
      case _ =>
    }
  }
}
