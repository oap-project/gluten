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
package org.apache.spark.sql.extension

import org.apache.gluten.GlutenConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.lang.IllegalArgumentException

// For readable, people usually convert a unix timestamp into date, and compare it with another
// date. For example
//    select * from table where '2023-11-02' >= from_unixtime(unix_timestamp, 'yyyy-MM-dd')
// There are performance shortcomings
// 1. convert a unix timestamp into date is expensive
// 2. comparisoin with date or string is not efficient.
//
// This rule try to make the filter condition into integer comparison, which is more efficient.
// The above example will be rewritten into
//    select * from table where to_unixtime('2023-11-02', 'yyyy-MM-dd') >= unix_timestamp
class RewriteDateTimestampComparisonRule(session: SparkSession, conf: SQLConf)
  extends Rule[LogicalPlan]
  with Logging {

  object TimeUnit extends Enumeration {
    val SECOND, MINUTE, HOUR, DAY, MONTH, YEAR = Value
  }

  val supportedDateTimeFormats = Set(
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd HH",
    "yyyy-MM-dd",
    "yyyy-MM",
    "yyyy"
  )

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      plan.resolved &&
      GlutenConfig.getConf.enableGluten &&
      GlutenConfig.getConf.enableRewriteDateTimestampComparison
    ) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  private def visitPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case filter: Filter =>
      val newCondition = visitExpression(filter.condition)
      val newFilter = Filter(newCondition, filter.child)
      newFilter
    case other =>
      val children = other.children.map(visitPlan)
      other.withNewChildren(children)
  }

  private def visitExpression(expression: Expression): Expression = expression match {
    case cmp: BinaryComparison =>
      if (isConstDateExpression(cmp.left) && isDateFromUnixTimestamp(cmp.right)) {
        rewriteComparisionBetweenTimestampAndDate(exchangeOperators(cmp))
      } else if (isConstDateExpression(cmp.right) && isDateFromUnixTimestamp(cmp.left)) {
        rewriteComparisionBetweenTimestampAndDate(cmp)
      } else {
        val children = expression.children.map(visitExpression)
        expression.withNewChildren(children)
      }
    case literal: Literal =>
      expression
    case _ =>
      val children = expression.children.map(visitExpression)
      expression.withNewChildren(children)
  }

  private def isConstDateExpression(expression: Expression): Boolean = {
    def allConstExpression(expr: Expression): Boolean = expr match {
      case literal: Literal => true
      case attr: Attribute => false
      case _ => expr.children.forall(allConstExpression)
    }
    if (
      !expression.dataType.isInstanceOf[DateType] && !expression.dataType.isInstanceOf[StringType]
    ) {
      return false
    }
    allConstExpression(expression)
  }

  private def isDateFromUnixTimestamp(expr: Expression): Boolean =
    expr match {
      case toDate: ParseToDate =>
        isDateFromUnixTimestamp(toDate.left)
      case fromUnixTime: FromUnixTime =>
        true
      case cast: Cast =>
        if (cast.child.dataType.isInstanceOf[DateType]) {
          isDateFromUnixTimestamp(cast.child)
        } else {
          false
        }
      case _ => false
    }

  private def getDateTimeUnitFromConstDateExpression(expr: Expression): Option[TimeUnit.Value] = {
    if (expr.dataType.isInstanceOf[DateType]) {
      Some(TimeUnit.DAY)
    } else if (expr.isInstanceOf[Literal] && expr.dataType.isInstanceOf[StringType]) {
      val dateStr = expr.asInstanceOf[Literal].value.asInstanceOf[UTF8String].toString
      val fullFormat = "yyyy-MM-dd HH:mm:ss"
      if (dateStr.length > fullFormat.length) {
        None
      } else {
        getDateTimeUnitFromString(fullFormat.substring(0, dateStr.length()))
      }
    } else {
      None
    }
  }

  private def getDateTimeUnitFromUnixTimeExpression(expr: Expression): Option[TimeUnit.Value] = {
    expr match {
      case toDate: ParseToDate =>
        val timeUnit = if (toDate.format.isEmpty) {
          Some(TimeUnit.DAY)
        } else {
          getDateTimeUnitFromLiteral(toDate.format)
        }
        val nestedTimeUnit = getDateTimeUnitFromUnixTimeExpression(toDate.left)
        if (nestedTimeUnit.isEmpty) {
          timeUnit
        } else {
          if (nestedTimeUnit.get > timeUnit.get) {
            nestedTimeUnit
          } else {
            timeUnit
          }
        }
      case cast: Cast =>
        getDateTimeUnitFromUnixTimeExpression(cast.child)
      case fromUnixTime: FromUnixTime =>
        getDateTimeUnitFromLiteral(Some(fromUnixTime.format))
      case _ => None
    }
  }

  private def getDateTimeUnitFromString(formatStr: String): Option[TimeUnit.Value] = {
    if (!supportedDateTimeFormats.contains(formatStr)) {
      return None
    }
    if (formatStr.contains("ss")) {
      Some(TimeUnit.SECOND)
    } else if (formatStr.contains("mm")) {
      Some(TimeUnit.MINUTE)
    } else if (formatStr.contains("HH")) {
      Some(TimeUnit.HOUR)
    } else if (formatStr.contains("dd")) {
      Some(TimeUnit.DAY)
    } else if (formatStr.contains("MM")) {
      Some(TimeUnit.MONTH)
    } else if (formatStr.contains("yyyy")) {
      Some(TimeUnit.YEAR)
    } else {
      None
    }
  }

  private def getDateTimeUnitFromLiteral(expr: Option[Expression]): Option[TimeUnit.Value] = {
    if (expr.isEmpty) {
      Some(TimeUnit.SECOND)
    } else if (
      !expr.get.isInstanceOf[Literal] || !expr.get
        .asInstanceOf[Literal]
        .dataType
        .isInstanceOf[StringType]
    ) {
      None
    } else {
      val formatExpr = expr.get.asInstanceOf[Literal]
      val formatStr = formatExpr.value.asInstanceOf[UTF8String].toString
      getDateTimeUnitFromString(formatStr)
    }
  }

  private def getTimeZoneId(expr: Expression): Option[String] = {
    expr match {
      case toDate: ParseToDate =>
        getTimeZoneId(toDate.left)
      case fromUnixTime: FromUnixTime =>
        fromUnixTime.timeZoneId
      case _ => None
    }
  }

  private def timeUnitToFormat(timeUnit: TimeUnit.Value): String = {
    timeUnit match {
      case TimeUnit.SECOND => "yyyy-MM-dd HH:mm:ss"
      case TimeUnit.MINUTE => "yyyy-MM-dd HH:mm"
      case TimeUnit.HOUR => "yyyy-MM-dd HH"
      case TimeUnit.DAY => "yyyy-MM-dd"
      case TimeUnit.MONTH => "yyyy-MM"
      case TimeUnit.YEAR => "yyyy"
    }
  }

  private def rewriteConstDate(
      expr: Expression,
      timeUnit: TimeUnit.Value,
      zoneId: Option[String],
      adjustedOffset: Long): Expression = {
    val formatExpr = Literal(UTF8String.fromString(timeUnitToFormat(timeUnit)), StringType)
    val adjustExpr = Literal(adjustedOffset, LongType)
    val toUnixTimestampExpr = ToUnixTimestamp(expr, formatExpr, zoneId)
    Add(toUnixTimestampExpr, adjustExpr)
  }

  // rewrite an expressiont that converts unix timestamp to date back to unix timestamp
  private def rewriteUnixTimestampToDate(expr: Expression): Expression = {
    expr match {
      case toDate: ParseToDate =>
        rewriteUnixTimestampToDate(toDate.left)
      case fromUnixTime: FromUnixTime =>
        fromUnixTime.sec
      case cast: Cast =>
        rewriteUnixTimestampToDate(cast.child)
      case _ => throw new IllegalArgumentException(s"Invalid expression: $expr")
    }
  }

  private def exchangeOperators(cmp: BinaryComparison): BinaryComparison = {
    cmp match {
      case gt: GreaterThan =>
        LessThan(cmp.right, cmp.left)
      case gte: GreaterThanOrEqual =>
        LessThanOrEqual(cmp.right, cmp.left)
      case lt: LessThan =>
        GreaterThan(cmp.right, cmp.left)
      case lte: LessThanOrEqual =>
        GreaterThanOrEqual(cmp.right, cmp.left)
      case eq: EqualTo =>
        EqualTo(cmp.right, cmp.left)
      case eqn: EqualNullSafe =>
        EqualNullSafe(cmp.right, cmp.left)
    }
  }

  private def rewriteComparisionBetweenTimestampAndDate(cmp: BinaryComparison): Expression = {
    val res = cmp match {
      case gt: GreaterThan =>
        rewriteGreaterThen(gt)
      case gte: GreaterThanOrEqual =>
        rewriteGreaterThanOrEqual(gte)
      case lt: LessThan =>
        rewriteLessThen(lt)
      case lte: LessThanOrEqual =>
        rewriteLessThenOrEqual(lte)
      case eq: EqualTo =>
        rewriteEqualTo(eq)
      case eqn: EqualNullSafe =>
        rewriteEqualNullSafe(eqn)
    }
    logInfo(s"rewrite expresion $cmp to $res")
    res
  }

  def TimeUnitToSeconds(timeUnit: TimeUnit.Value): Long = timeUnit match {
    case TimeUnit.SECOND => 1
    case TimeUnit.MINUTE => 60
    case TimeUnit.HOUR => 3600
    case TimeUnit.DAY => 86400
    case TimeUnit.MONTH => 2592000
    case TimeUnit.YEAR => 31536000
  }

  private def checkDateTimeUnits(a: Option[TimeUnit.Value], b: Option[TimeUnit.Value]): Boolean = {
    if (a.isEmpty || b.isEmpty) {
      return false
    }
    return a.get == b.get
  }

  private def rewriteGreaterThen(cmp: GreaterThan): Expression = {
    val timeUnit = getDateTimeUnitFromUnixTimeExpression(cmp.left)
    val adjustedTimeUnit = getDateTimeUnitFromConstDateExpression(cmp.right)
    if (!checkDateTimeUnits(timeUnit, adjustedTimeUnit)) {
      return cmp
    }
    val zoneId = getTimeZoneId(cmp.left)
    val adjustedOffset = TimeUnitToSeconds(adjustedTimeUnit.get)
    val newLeft = rewriteUnixTimestampToDate(cmp.left)
    val newRight = rewriteConstDate(cmp.right, adjustedTimeUnit.get, zoneId, adjustedOffset)
    GreaterThanOrEqual(newLeft, newRight)
  }

  private def rewriteGreaterThanOrEqual(cmp: GreaterThanOrEqual): Expression = {
    val timeUnit = getDateTimeUnitFromUnixTimeExpression(cmp.left)
    val adjustedTimeUnit = getDateTimeUnitFromConstDateExpression(cmp.right)
    if (!checkDateTimeUnits(timeUnit, adjustedTimeUnit)) {
      return cmp
    }
    val zoneId = getTimeZoneId(cmp.left)
    val adjustedOffset = 0
    val newLeft = rewriteUnixTimestampToDate(cmp.left)
    val newRight = rewriteConstDate(cmp.right, adjustedTimeUnit.get, zoneId, adjustedOffset)
    GreaterThanOrEqual(newLeft, newRight)
  }

  private def rewriteLessThen(cmp: LessThan): Expression = {
    val timeUnit = getDateTimeUnitFromUnixTimeExpression(cmp.left)
    val adjustedTimeUnit = getDateTimeUnitFromConstDateExpression(cmp.right)
    if (!checkDateTimeUnits(timeUnit, adjustedTimeUnit)) {
      return cmp
    }
    val zoneId = getTimeZoneId(cmp.left)
    val adjustedOffset = 0
    val newLeft = rewriteUnixTimestampToDate(cmp.left)
    val newRight = rewriteConstDate(cmp.right, adjustedTimeUnit.get, zoneId, adjustedOffset)
    LessThan(newLeft, newRight)
  }

  private def rewriteLessThenOrEqual(cmp: LessThanOrEqual): Expression = {
    val timeUnit = getDateTimeUnitFromUnixTimeExpression(cmp.left)
    val adjustedTimeUnit = getDateTimeUnitFromConstDateExpression(cmp.right)
    if (!checkDateTimeUnits(timeUnit, adjustedTimeUnit)) {
      return cmp
    }
    val zoneId = getTimeZoneId(cmp.left)
    val adjustedOffset = TimeUnitToSeconds(adjustedTimeUnit.get)
    val newLeft = rewriteUnixTimestampToDate(cmp.left)
    val newRight = rewriteConstDate(cmp.right, adjustedTimeUnit.get, zoneId, adjustedOffset)
    LessThan(newLeft, newRight)
  }

  private def rewriteEqualTo(cmp: EqualTo): Expression = {
    val timeUnit = getDateTimeUnitFromUnixTimeExpression(cmp.left)
    val adjustedTimeUnit = getDateTimeUnitFromConstDateExpression(cmp.right)
    if (!checkDateTimeUnits(timeUnit, adjustedTimeUnit)) {
      return cmp
    }
    val zoneId = getTimeZoneId(cmp.left)
    val newLeft = rewriteUnixTimestampToDate(cmp.left)
    val adjustedOffset = Literal(TimeUnitToSeconds(adjustedTimeUnit.get), newLeft.dataType)
    val newRight = rewriteConstDate(cmp.right, adjustedTimeUnit.get, zoneId, 0)
    val leftBound = GreaterThanOrEqual(newLeft, newRight)
    val rigtBound = LessThan(newLeft, Add(newRight, adjustedOffset))
    And(leftBound, rigtBound)
  }

  private def rewriteEqualNullSafe(cmp: EqualNullSafe): Expression = {
    cmp
  }
}
