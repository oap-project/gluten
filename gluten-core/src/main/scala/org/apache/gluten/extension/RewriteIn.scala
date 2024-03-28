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
package org.apache.gluten.extension

import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, In, Or}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, SparkPlan}
import org.apache.spark.sql.types.StructType

/**
 * This rule rewrites the [[In]] if the list option contains non-foldable value. A rewrite example:
 *
 * {{{
 *   SELECT * FROM t WHERE c in (1, 2, c2)
 *   =>
 *   SELECT * FROM t WHERE c in (1, 2) or c = c2
 * }}}
 *
 * TODO: Remove this rule once Velox support the list option in `In` is not literal.
 */
object RewriteIn extends Rule[SparkPlan] {

  private def shouldRewrite(e: Expression): Boolean = {
    e match {
      case in: In if !in.value.dataType.isInstanceOf[StructType] && in.list.exists(!_.foldable) =>
        // `EqualTo` does not allow struct type
        true
      case _ => false
    }
  }

  private def rewriteIn(condition: Expression): Expression = {
    condition.transform {
      case in: In if shouldRewrite(in) =>
        val (foldableExprs, nonFoldableExprs) = in.list.partition(_.foldable)
        assert(nonFoldableExprs.nonEmpty)
        val rewrittenExpr = nonFoldableExprs.map(expr => EqualTo(in.value, expr)).reduce(Or)
        val newIn = if (foldableExprs.isEmpty) {
          None
        } else {
          Some(in.copy(list = foldableExprs))
        }
        (newIn ++ Seq(rewrittenExpr)).reduce(Or)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    plan match {
      // TODO: Support datasource v2
      case scan: FileSourceScanExec if scan.dataFilters.exists(_.find(shouldRewrite).isDefined) =>
        val newDataFilters = scan.dataFilters.map(rewriteIn)
        scan.copy(dataFilters = newDataFilters)
      case f: FilterExec if f.condition.find(shouldRewrite).isDefined =>
        val newCondition = rewriteIn(f.condition)
        f.copy(condition = newCondition)
      case _ => plan
    }
  }
}
