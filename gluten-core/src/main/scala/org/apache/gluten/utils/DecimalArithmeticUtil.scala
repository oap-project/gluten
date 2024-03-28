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
package org.apache.gluten.utils

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{CheckOverflowTransformer, ChildTransformer, DecimalArithmeticExpressionTransformer, ExpressionTransformer}

import org.apache.spark.sql.catalyst.analysis.DecimalPrecision
import org.apache.spark.sql.catalyst.expressions.{Add, BinaryArithmetic, Cast, Divide, Expression, Literal, Multiply, Pmod, PromotePrecision, Remainder, Subtract}
import org.apache.spark.sql.types.{ByteType, Decimal, DecimalType, IntegerType, LongType, ShortType}

object DecimalArithmeticUtil {

  object OperationType extends Enumeration {
    type Config = Value
    val ADD, SUBTRACT, MULTIPLY, DIVIDE, MOD = Value
  }

  val MIN_ADJUSTED_SCALE = 6
  val MAX_PRECISION = 38

  // Returns the result decimal type of a decimal arithmetic computing.
  def getResultTypeForOperation(
      operationType: OperationType.Config,
      type1: DecimalType,
      type2: DecimalType): DecimalType = {
    var resultScale = 0
    var resultPrecision = 0
    operationType match {
      case OperationType.ADD =>
        resultScale = Math.max(type1.scale, type2.scale)
        resultPrecision =
          resultScale + Math.max(type1.precision - type1.scale, type2.precision - type2.scale) + 1
      case OperationType.SUBTRACT =>
        resultScale = Math.max(type1.scale, type2.scale)
        resultPrecision =
          resultScale + Math.max(type1.precision - type1.scale, type2.precision - type2.scale) + 1
      case OperationType.MULTIPLY =>
        resultScale = type1.scale + type2.scale
        resultPrecision = type1.precision + type2.precision + 1
      case OperationType.DIVIDE =>
        resultScale = Math.max(MIN_ADJUSTED_SCALE, type1.scale + type2.precision + 1)
        resultPrecision = type1.precision - type1.scale + type2.scale + resultScale
      case OperationType.MOD =>
        resultScale = Math.max(type1.scale, type2.scale)
        resultPrecision =
          Math.min(type1.precision - type1.scale, type2.precision - type2.scale + resultScale)
      case other =>
        throw new GlutenNotSupportException(s"$other is not supported.")
    }
    adjustScaleIfNeeded(resultPrecision, resultScale)
  }

  // Returns the adjusted decimal type when the precision is larger the maximum.
  def adjustScaleIfNeeded(precision: Int, scale: Int): DecimalType = {
    var typePrecision = precision
    var typeScale = scale
    if (precision > MAX_PRECISION) {
      val minScale = Math.min(scale, MIN_ADJUSTED_SCALE)
      val delta = precision - MAX_PRECISION
      typePrecision = MAX_PRECISION
      typeScale = Math.max(scale - delta, minScale)
    }
    DecimalType(typePrecision, typeScale)
  }

  // If casting between DecimalType, unnecessary cast is skipped to avoid data loss,
  // because argument input type of "cast" is actually the res type of "+-*/".
  // Cast will use a wider input type, then calculates result type with less scale than expected.
  def isDecimalArithmetic(b: BinaryArithmetic): Boolean = {
    if (
      b.left.dataType.isInstanceOf[DecimalType] &&
      b.right.dataType.isInstanceOf[DecimalType]
    ) {
      b match {
        case _: Divide | _: Multiply | _: Add | _: Subtract | _: Remainder | _: Pmod => true
        case _ => false
      }
    } else false
  }

  // Returns the operation type of a binary arithmetic expression.
  def getOperationType(b: BinaryArithmetic): OperationType.Config = {
    b match {
      case _: Add => OperationType.ADD
      case _: Subtract => OperationType.SUBTRACT
      case _: Multiply => OperationType.MULTIPLY
      case _: Divide => OperationType.DIVIDE
      case other =>
        throw new GlutenNotSupportException(s"$other is not supported.")
    }
  }

  // For decimal * 10 case, dec will be Decimal(38, 18), then the result precision is wrong,
  // so here we will get the real precision and scale of the literal.
  private def getNewPrecisionScale(dec: Decimal): (Integer, Integer) = {
    val input = dec.abs.toString()
    val dotIndex = input.indexOf(".")
    if (dotIndex == -1) {
      return (input.length, 0)
    }
    if (dec.toBigDecimal.isValidLong) {
      return (dotIndex, 0)
    }
    (dec.precision, dec.scale)
  }

  // Change the precision and scale to the actual precision and scale of a literal,
  // otherwise the result precision loses.
  def rescaleLiteral(arithmeticExpr: BinaryArithmetic): BinaryArithmetic = {
    if (
      arithmeticExpr.left.isInstanceOf[PromotePrecision] &&
      arithmeticExpr.right.isInstanceOf[Literal]
    ) {
      val lit = arithmeticExpr.right.asInstanceOf[Literal]
      lit.value match {
        case decLit: Decimal =>
          val (precision, scale) = getNewPrecisionScale(decLit)
          if (precision != decLit.precision || scale != decLit.scale) {
            arithmeticExpr
              .withNewChildren(Seq(arithmeticExpr.left, Cast(lit, DecimalType(precision, scale))))
              .asInstanceOf[BinaryArithmetic]
          } else arithmeticExpr
        case _ => arithmeticExpr
      }
    } else if (
      arithmeticExpr.right.isInstanceOf[PromotePrecision]
      && arithmeticExpr.left.isInstanceOf[Literal]
    ) {
      val lit = arithmeticExpr.left.asInstanceOf[Literal]
      lit.value match {
        case decLit: Decimal =>
          val (precision, scale) = getNewPrecisionScale(decLit)
          if (precision != decLit.precision || scale != decLit.scale) {
            arithmeticExpr
              .withNewChildren(Seq(Cast(lit, DecimalType(precision, scale)), arithmeticExpr.right))
              .asInstanceOf[BinaryArithmetic]
          } else arithmeticExpr
        case _ => arithmeticExpr
      }
    } else {
      arithmeticExpr
    }
  }

  // Returns whether the input expression is a combination of PromotePrecision(Cast as DecimalType).
  private def isPromoteCast(expr: Expression): Boolean = {
    expr match {
      case precision: PromotePrecision =>
        precision.child match {
          case cast: Cast if cast.dataType.isInstanceOf[DecimalType] => true
          case _ => false
        }
      case _ => false
    }
  }

  def rescaleCastForDecimal(left: Expression, right: Expression): (Expression, Expression) = {
    if (!BackendsApiManager.getSettings.rescaleDecimalIntegralExpression()) {
      return (left, right)
    }
    // Decimal * cast int.
    if (!isPromoteCast(left)) {
      // Have removed PromotePrecision(Cast(DecimalType)).
      if (isPromoteCastIntegral(right)) {
        val newRight = rescaleCastForOneSide(right)
        val isWiderType = checkIsWiderType(
          left.dataType.asInstanceOf[DecimalType],
          newRight.dataType.asInstanceOf[DecimalType],
          right.dataType.asInstanceOf[DecimalType])
        if (isWiderType) {
          (left, newRight)
        } else {
          (left, right)
        }
      } else {
        (left, right)
      }
      // Cast int * decimal.
    } else if (!isPromoteCast(right)) {
      if (isPromoteCastIntegral(left)) {
        val newLeft = rescaleCastForOneSide(left)
        val isWiderType = checkIsWiderType(
          newLeft.dataType.asInstanceOf[DecimalType],
          right.dataType.asInstanceOf[DecimalType],
          left.dataType.asInstanceOf[DecimalType])
        if (isWiderType) {
          (newLeft, right)
        } else {
          (left, right)
        }
      } else {
        (left, right)
      }
    } else {
      // Cast int * cast int. Usually user defined cast.
      (left, right)
    }
  }

  /**
   * Remove the Cast when child is PromotePrecision and PromotePrecision is Cast(Decimal, Decimal)
   *
   * @param arithmeticExpr
   *   BinaryArithmetic left or right
   * @return
   *   expression removed child PromotePrecision->Cast
   */
  def removeCastForDecimal(arithmeticExpr: Expression): Expression = {
    arithmeticExpr match {
      case precision: PromotePrecision =>
        precision.child match {
          case cast: Cast
              if cast.dataType.isInstanceOf[DecimalType]
                && cast.child.dataType.isInstanceOf[DecimalType] =>
            cast.child
          case _ => arithmeticExpr
        }
      case _ => arithmeticExpr
    }
  }

  def getResultType(transformer: ExpressionTransformer): Option[DecimalType] = {
    transformer match {
      case ChildTransformer(child) =>
        getResultType(child)
      case CheckOverflowTransformer(_, _, original) =>
        Some(original.dataType)
      case DecimalArithmeticExpressionTransformer(_, _, _, resultType, _) =>
        Some(resultType)
      case _ => None
    }
  }

  private def isPromoteCastIntegral(expr: Expression): Boolean = {
    expr match {
      case precision: PromotePrecision =>
        precision.child match {
          case cast: Cast if cast.dataType.isInstanceOf[DecimalType] =>
            cast.child.dataType match {
              case IntegerType | ByteType | ShortType | LongType => true
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  private def rescaleCastForOneSide(expr: Expression): Expression = {
    expr match {
      case precision: PromotePrecision =>
        precision.child match {
          case castInt: Cast
              if castInt.dataType.isInstanceOf[DecimalType] &&
                BackendsApiManager.getSettings.rescaleDecimalIntegralExpression() =>
            castInt.child.dataType match {
              case IntegerType | ByteType | ShortType =>
                precision.withNewChildren(Seq(Cast(castInt.child, DecimalType(10, 0))))
              case LongType =>
                precision.withNewChildren(Seq(Cast(castInt.child, DecimalType(20, 0))))
              case _ => expr
            }
          case _ => expr
        }
      case _ => expr
    }
  }

  private def checkIsWiderType(
      left: DecimalType,
      right: DecimalType,
      wider: DecimalType): Boolean = {
    val widerType = DecimalPrecision.widerDecimalType(left, right)
    widerType.equals(wider)
  }
}
