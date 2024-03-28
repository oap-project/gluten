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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}

/**
 * Merge two phase hash-based aggregate into one aggregate in the spark plan if there is no shuffle:
 *
 * Merge HashAggregate(t1.i, SUM, final) + HashAggregate(t1.i, SUM, partial) into
 * HashAggregate(t1.i, SUM, complete)
 *
 * Note: this rule must be applied before the `PullOutPreProject` rule, because the
 * `PullOutPreProject` rule will modify the attributes in some cases.
 */
case class MergeTwoPhasesHashBaseAggregate(session: SparkSession) extends Rule[SparkPlan] {

  val columnarConf: GlutenConfig = GlutenConfig.getConf
  val scanOnly: Boolean = columnarConf.enableScanOnly
  val enableColumnarHashAgg: Boolean = !scanOnly && columnarConf.enableColumnarHashAgg
  val replaceSortAggWithHashAgg = BackendsApiManager.getSettings.replaceSortAggWithHashAgg

  private def isPartialAgg(partialAgg: BaseAggregateExec, finalAgg: BaseAggregateExec): Boolean = {
    // TODO: now it can not support to merge agg which there are the filters in the aggregate exprs.
    if (
      partialAgg.aggregateExpressions.forall(x => x.mode == Partial && x.filter.isEmpty) &&
      finalAgg.aggregateExpressions.forall(x => x.mode == Final && x.filter.isEmpty)
    ) {
      (finalAgg.logicalLink, partialAgg.logicalLink) match {
        case (Some(agg1), Some(agg2)) => agg1.sameResult(agg2)
        case _ => false
      }
    } else {
      false
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (
      !enableColumnarHashAgg || !BackendsApiManager.getSettings
        .mergeTwoPhasesHashBaseAggregateIfNeed()
    ) {
      plan
    } else {
      plan.transformDown {
        case hashAgg @ HashAggregateExec(
              _,
              isStreaming,
              _,
              _,
              aggregateExpressions,
              aggregateAttributes,
              _,
              resultExpressions,
              child: HashAggregateExec) if !isStreaming && isPartialAgg(child, hashAgg) =>
          // convert to complete mode aggregate expressions
          val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
          hashAgg.copy(
            requiredChildDistributionExpressions = None,
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = completeAggregateExpressions,
            initialInputBufferOffset = 0,
            child = child.child
          )
        case objectHashAgg @ ObjectHashAggregateExec(
              _,
              isStreaming,
              _,
              _,
              aggregateExpressions,
              aggregateAttributes,
              _,
              resultExpressions,
              child: ObjectHashAggregateExec)
            if !isStreaming && isPartialAgg(child, objectHashAgg) =>
          // convert to complete mode aggregate expressions
          val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
          objectHashAgg.copy(
            requiredChildDistributionExpressions = None,
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = completeAggregateExpressions,
            initialInputBufferOffset = 0,
            child = child.child
          )
        case sortAgg @ SortAggregateExec(
              _,
              isStreaming,
              _,
              _,
              aggregateExpressions,
              aggregateAttributes,
              _,
              resultExpressions,
              child: SortAggregateExec)
            if replaceSortAggWithHashAgg && !isStreaming && isPartialAgg(child, sortAgg) =>
          // convert to complete mode aggregate expressions
          val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
          sortAgg.copy(
            requiredChildDistributionExpressions = None,
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = completeAggregateExpressions,
            initialInputBufferOffset = 0,
            child = child.child
          )
        case plan: SparkPlan => plan
      }
    }
  }
}
