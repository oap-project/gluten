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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.DecimalType

object ExpressionConverter extends Logging {
  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): Expression =
    expr match {
      case a: Alias =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        new AliasTransformer(
          replaceWithExpressionTransformer(
            a.child,
            attributeSeq),
          a.name)(a.exprId, a.qualifier, a.explicitMetadata)
      case a: AttributeReference =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        if (attributeSeq == null) {
          throw new UnsupportedOperationException(s"attributeSeq should not be null.")
        }
        val bindReference =
          BindReferences.bindReference(expr, attributeSeq, allowFailures = true)
        if (bindReference == expr) {
          // This means bind failure.
          throw new UnsupportedOperationException(s"attribute binding failed.")
        } else {
          val b = bindReference.asInstanceOf[BoundReference]
          new AttributeReferenceTransformer(
            a.name, b.ordinal, a.dataType, a.nullable, a.metadata)(a.exprId, a.qualifier)
        }
      case l: Literal =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        new LiteralTransformer(l)
      case b: BinaryArithmetic =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        BinaryArithmeticTransformer.create(
          replaceWithExpressionTransformer(
            b.left,
            attributeSeq),
          replaceWithExpressionTransformer(
            b.right,
            attributeSeq),
          expr)
      case b: BoundReference =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        new BoundReferenceTransformer(b.ordinal, b.dataType, b.nullable)
      case b: BinaryOperator =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        BinaryOperatorTransformer.create(
          replaceWithExpressionTransformer(
            b.left,
            attributeSeq),
          replaceWithExpressionTransformer(
            b.right,
            attributeSeq),
          expr)
      case b: BinaryExpression =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        BinaryExpressionTransformer.create(
          replaceWithExpressionTransformer(
            b.left,
            attributeSeq),
          replaceWithExpressionTransformer(
            b.right,
            attributeSeq),
          expr)
      case i: If =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        IfOperatorTransformer.create(
          replaceWithExpressionTransformer(
            i.predicate,
            attributeSeq),
          replaceWithExpressionTransformer(
            i.trueValue,
            attributeSeq),
          replaceWithExpressionTransformer(
            i.falseValue,
            attributeSeq),
          expr)
      case cw: CaseWhen =>
        logInfo(s"${expr.getClass} ${expr} is supportedn.")
        val colBranches = cw.branches.map { expr => {(
              replaceWithExpressionTransformer(
                expr._1,
                attributeSeq),
              replaceWithExpressionTransformer(
                expr._2,
                attributeSeq))
          }
        }
        val colElseValue = cw.elseValue.map { expr => {
            replaceWithExpressionTransformer(
              expr,
              attributeSeq)
          }
        }
        logDebug(s"colBanches: $colBranches")
        logDebug(s"colElseValue: $colElseValue")
        CaseWhenOperatorTransformer.create(colBranches, colElseValue, expr)
      case c: Coalesce =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        val exprs = c.children.map { expr =>
          replaceWithExpressionTransformer(
            expr,
            attributeSeq)
        }
        CoalesceOperatorTransformer.create(exprs, expr)
      case i: In =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        InOperatorTransformer.create(
          replaceWithExpressionTransformer(
            i.value,
            attributeSeq),
          i.list,
          expr)
      case i: InSet =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        InSetOperatorTransformer.create(
          replaceWithExpressionTransformer(
            i.child,
            attributeSeq),
          i.hset,
          expr)
      case ss: Substring =>
        logInfo(s"${expr.getClass} ${expr} is supported.")
        TernaryOperatorTransformer.create(
          replaceWithExpressionTransformer(
            ss.str,
            attributeSeq),
          replaceWithExpressionTransformer(
            ss.pos,
            attributeSeq),
          replaceWithExpressionTransformer(
            ss.len,
            attributeSeq),
          expr)
      case u: UnaryExpression =>
        logInfo(s"${expr.getClass} $expr is supported")
        if (!u.isInstanceOf[CheckOverflow] || !u.child.isInstanceOf[Divide]) {
          UnaryOperatorTransformer.create(
            replaceWithExpressionTransformer(
              u.child,
              attributeSeq),
            expr)
        } else {
          // CheckOverflow[Divide]: pass resType to Divide to avoid precision loss.
          val divide = u.child.asInstanceOf[Divide]
          val columnarDivide = BinaryArithmeticTransformer.createDivide(
            replaceWithExpressionTransformer(
              divide.left,
              attributeSeq),
            replaceWithExpressionTransformer(
              divide.right,
              attributeSeq),
            divide,
            u.dataType.asInstanceOf[DecimalType])
          UnaryOperatorTransformer.create(
            columnarDivide,
            expr)
        }
      case c: Concat =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        val exprs = c.children.map { expr =>
          replaceWithExpressionTransformer(
            expr,
            attributeSeq)
        }
        ConcatOperatorTransformer.create(exprs, expr)
      case r: Round =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        RoundOperatorTransformer.create(
          replaceWithExpressionTransformer(
            r.child,
            attributeSeq),
          replaceWithExpressionTransformer(
            r.scale,
            attributeSeq),
          expr)
      case expr =>
        throw new UnsupportedOperationException(
          s"${expr.getClass} or ${expr} is not currently supported.")
    }

  def containsSubquery(expr: Expression): Boolean =
    expr match {
      case a: AttributeReference =>
        return false
      case lit: Literal =>
        return false
      case b: BoundReference =>
        return false
      case u: UnaryExpression =>
        containsSubquery(u.child)
      case b: BinaryOperator =>
        containsSubquery(b.left) || containsSubquery(b.right)
      case i: If =>
        containsSubquery(i.predicate) || containsSubquery(i.trueValue) || containsSubquery(
          i.falseValue)
      case cw: CaseWhen =>
        cw.branches.exists(p => containsSubquery(p._1) || containsSubquery(p._2)) ||
                           cw.elseValue.exists(containsSubquery)
      case c: Coalesce =>
        c.children.exists(containsSubquery)
      case i: In =>
        containsSubquery(i.value)
      case ss: Substring =>
        containsSubquery(ss.str) || containsSubquery(ss.pos) || containsSubquery(ss.len)
      case s: org.apache.spark.sql.execution.ScalarSubquery =>
        return true
      case c: Concat =>
        c.children.exists(containsSubquery)
      case b: BinaryExpression =>
        containsSubquery(b.left) || containsSubquery(b.right)
      case expr =>
        throw new UnsupportedOperationException(
          s" --> ${expr.getClass} | ${expr} is not currently supported.")
    }
}
