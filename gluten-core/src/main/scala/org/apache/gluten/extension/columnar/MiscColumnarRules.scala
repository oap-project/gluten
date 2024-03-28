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
package org.apache.gluten.extension.columnar

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.ColumnarToRowLike
import org.apache.gluten.extension.columnar.transform.{ImplementAggregate, ImplementExchange, ImplementFilter, ImplementJoin, ImplementOthers, ImplementSingleNode}
import org.apache.gluten.utils.{LogLevelUtil, PlanUtil}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}

object MiscColumnarRules {
  object TransformPreOverrides {
    def apply(): TransformPreOverrides = {
      TransformPreOverrides(
        List(ImplementFilter()),
        List(
          ImplementOthers(),
          ImplementAggregate(),
          ImplementExchange(),
          ImplementJoin()
        )
      )
    }
  }

  // This rule will conduct the conversion from Spark plan to the plan transformer.
  case class TransformPreOverrides(
      topDownRules: Seq[ImplementSingleNode],
      bottomUpRules: Seq[ImplementSingleNode])
    extends Rule[SparkPlan]
    with LogLevelUtil {
    @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

    def apply(plan: SparkPlan): SparkPlan = {
      val plan0 =
        topDownRules.foldLeft(plan)((p, rule) => p.transformDown { case p => rule.impl(p) })
      val plan1 =
        bottomUpRules.foldLeft(plan0)((p, rule) => p.transformUp { case p => rule.impl(p) })
      planChangeLogger.logRule(ruleName, plan, plan1)
      plan1
    }
  }

  // This rule will try to convert the row-to-columnar and columnar-to-row
  // into native implementations.
  case class TransformPostOverrides() extends Rule[SparkPlan] {
    @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

    def replaceWithTransformerPlan(plan: SparkPlan): SparkPlan = plan.transformDown {
      case RowToColumnarExec(child) =>
        logDebug(s"ColumnarPostOverrides RowToColumnarExec(${child.getClass})")
        BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(child)
      case c2r @ ColumnarToRowExec(child) if PlanUtil.outputNativeColumnarData(child) =>
        logDebug(s"ColumnarPostOverrides ColumnarToRowExec(${child.getClass})")
        val nativeC2r = BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToRowExec(child)
        if (nativeC2r.doValidate().isValid) {
          nativeC2r
        } else {
          c2r
        }
    }

    // apply for the physical not final plan
    def apply(plan: SparkPlan): SparkPlan = {
      val newPlan = replaceWithTransformerPlan(plan)
      planChangeLogger.logRule(ruleName, plan, newPlan)
      newPlan
    }
  }

  // Remove topmost columnar-to-row otherwise AQE throws error.
  // See: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec#newQueryStage
  //
  // The rule is basically a workaround because of the limited compatibility between Spark's AQE
  // and columnar API.
  case class RemoveTopmostColumnarToRow(session: SparkSession, isAdaptiveContext: Boolean)
    extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      if (!isAdaptiveContext) {
        // The rule only applies in AQE. If AQE is off the topmost C2R will be strictly required
        // by Spark.
        return plan
      }
      plan match {
        // See: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec#newQueryStage
        case ColumnarToRowLike(child: ShuffleExchangeLike) => child
        case ColumnarToRowLike(child: BroadcastExchangeLike) => child
        // See: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec#getFinalPhysicalPlan
        //  BroadQueryStageExec could be inside a C2R which may cause check failures. E.g.,
        //  org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec#doExecuteBroadcast
        // Note: To avoid the similar issue with AQE=off, we don't remove the C2R on
        //  ShuffleQueryStageExec. Also there is not check like the one for BroadcastQueryStageExec
        //  so it's safe to keep it.
        case ColumnarToRowLike(child: BroadcastQueryStageExec) => child
        case other => other
      }
    }
  }

  // `InMemoryTableScanExec` internally supports ColumnarToRow.
  case class RemoveGlutenTableCacheColumnarToRow(session: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
      case ColumnarToRowLike(child) if PlanUtil.isGlutenTableCache(child) =>
        child
    }
  }
}
