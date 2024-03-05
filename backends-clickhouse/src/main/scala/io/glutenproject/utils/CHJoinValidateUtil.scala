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
package io.glutenproject.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Not, Or}
import org.apache.spark.sql.catalyst.plans.JoinType

/**
 * The logic here is that if it is not an equi-join spark will create BNLJ, which will fallback, if
 * it is an equi-join, spark will create BroadcastHashJoin or ShuffleHashJoin, for these join types,
 * we need to filter For cases that cannot be handled by the backend, 1 there are at least two
 * different tables column and Literal in the condition Or condition for comparison, for example: (a
 * join b on a.a1 = b.b1 and (a.a2 > 1 or b.b2 < 2) ) 2 tow join key for inequality comparison (!= ,
 * > , <), for example: (a join b on a.a1 > b.b1) There will be a fallback for Nullaware Jion For
 * Existence Join which is just an optimization of exist subquery, it will also fallback
 */

object CHJoinValidateUtil extends Logging {
  def hasTwoTableColumn(
      leftOutputSet: AttributeSet,
      rightOutputSet: AttributeSet,
      l: Expression,
      r: Expression): Boolean = {
    val allReferences = l.references ++ r.references
    !(allReferences.subsetOf(leftOutputSet) || allReferences.subsetOf(rightOutputSet))
  }

  def shouldFallback(
      joinType: JoinType,
      leftOutputSet: AttributeSet,
      rightOutputSet: AttributeSet,
      condition: Option[Expression],
      isSMJ: Boolean = false): Boolean = {
    var shouldFallback = false
    if (joinType.toString.contains("ExistenceJoin")) {
      return true
    }
    if (joinType.sql.equals("INNER")) {
      return shouldFallback
    }
    if (isSMJ) {
      if (joinType.sql.contains("SEMI")
      || joinType.sql.contains("ANTI")) {
        return true
      }
    }
    if (condition.isDefined) {
      condition.get.transform {
        case Or(l, r) =>
          if (hasTwoTableColumn(leftOutputSet, rightOutputSet, l, r)) {
            shouldFallback = true
          }
          Or(l, r)
        case Not(EqualTo(l, r)) =>
          if (hasTwoTableColumn(leftOutputSet, rightOutputSet, l, r)) {
            shouldFallback = true
          }
          Not(EqualTo(l, r))
        case LessThan(l, r) =>
          if (hasTwoTableColumn(leftOutputSet, rightOutputSet, l, r)) {
            shouldFallback = true
          }
          LessThan(l, r)
        case LessThanOrEqual(l, r) =>
          if (hasTwoTableColumn(leftOutputSet, rightOutputSet, l, r)) {
            shouldFallback = true
          }
          LessThanOrEqual(l, r)
        case GreaterThan(l, r) =>
          if (hasTwoTableColumn(leftOutputSet, rightOutputSet, l, r)) {
            shouldFallback = true
          }
          GreaterThan(l, r)
        case GreaterThanOrEqual(l, r) =>
          if (hasTwoTableColumn(leftOutputSet, rightOutputSet, l, r)) {
            shouldFallback = true
          }
          GreaterThanOrEqual(l, r)
        case In(l, r) =>
          r.foreach(
            e => {
              if (hasTwoTableColumn(leftOutputSet, rightOutputSet, l, e)) {
                shouldFallback = true
              }
            })
          In(l, r)
        case EqualTo(l, r) =>
          if (hasTwoTableColumn(leftOutputSet, rightOutputSet, l, r)) {
            shouldFallback = true
          }
          EqualTo(l, r)
      }
    }
    shouldFallback
  }
}
