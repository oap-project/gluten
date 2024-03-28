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
package io.glutenproject.extension.columnar

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ApplyColumnarRulesAndInsertTransitions, ColumnarToRowExec, ColumnarToRowTransition, RowToColumnarExec, RowToColumnarTransition, SparkPlan}

/** See rule code from vanilla Spark: [[ApplyColumnarRulesAndInsertTransitions]]. */
case class InsertTransitions(outputsColumnar: Boolean) extends Rule[SparkPlan] {
  private object RemoveRedundantTransitions extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
      case ColumnarToRowExec(RowToColumnarExec(child)) => child
      case RowToColumnarExec(ColumnarToRowExec(child)) => child
    }
  }

  private val rules = List(
    ApplyColumnarRulesAndInsertTransitions(List(), outputsColumnar),
    RemoveRedundantTransitions)
  override def apply(plan: SparkPlan): SparkPlan = rules.foldLeft(plan) {
    case (p, r) => r.apply(p)
  }
}

object RemoveTransitions extends Rule[SparkPlan] {
  import ColumnarTransitions._
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case ColumnarToRowLike(child) => child
    case RowToColumnarLike(child) => child
  }
}

object ColumnarTransitions {
  def insertTransitions(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    InsertTransitions(outputsColumnar).apply(plan)
  }

  // Extractor for Spark/Gluten's C2R
  object ColumnarToRowLike {
    def unapply(plan: SparkPlan): Option[SparkPlan] = {
      plan match {
        case c2r: ColumnarToRowTransition =>
          Some(c2r.child)
        case _ => None
      }
    }
  }

  // Extractor for Spark/Gluten's R2C
  object RowToColumnarLike {
    def unapply(plan: SparkPlan): Option[SparkPlan] = {
      plan match {
        case c2r: RowToColumnarTransition =>
          Some(c2r.child)
        case _ => None
      }
    }
  }
}
