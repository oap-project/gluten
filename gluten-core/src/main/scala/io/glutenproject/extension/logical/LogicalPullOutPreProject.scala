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
package io.glutenproject.extension.logical

import io.glutenproject.GlutenConfig
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.utils.{LogicalPlanSelector, PullOutProjectHelper}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, FILTER}
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression

import scala.collection.mutable

/**
 * Pulling out pre-projects in the logical plan can reduce the number of pre-projects in some cases.
 * For example, in agg operation that include distinct, we can reduce two expression evaluations and
 * pre-project if we pull out project at logical side. Additionally, for operations like joins that
 * may require adding broadcast exchanges or sort operations in the child plans, handling them in
 * the logical plan can be simpler. However, logical rules may not be able to handle all scenarios,
 * so we still need physical rules to handle any remaining cases.
 */
case class LogicalPullOutPreProject(session: SparkSession)
  extends Rule[LogicalPlan]
  with PullOutProjectHelper {

  private def needsPreProject(plan: LogicalPlan): Boolean = plan match {
    case Aggregate(groupingExpressions, aggregateExpressions, _) =>
      groupingExpressions.exists(isNotAttribute) ||
      aggregateExpressions.exists(_.find {
        case ae: AggregateExpression
            if ae.aggregateFunction.isInstanceOf[TypedAggregateExpression] =>
          // We cannot pull out the children of TypedAggregateExpression to pre-project,
          // and Gluten cannot support TypedAggregateExpression.
          false
        case ae: AggregateExpression
            if ae.filter.exists(isNotAttribute) || ae.aggregateFunction.children.exists(
              isNotAttributeAndLiteral) =>
          true
        case _ => false
      }.isDefined)
    case _ => false
  }

  private def transformAgg(agg: Aggregate): LogicalPlan = {
    val expressionMap = new mutable.HashMap[Expression, NamedExpression]()

    // Handle groupingExpressions.
    val newGroupingExpressions =
      agg.groupingExpressions.toIndexedSeq.map(replaceExpressionWithAttribute(_, expressionMap))

    // Handle aggregateExpressions
    val newAggregateExpressions = agg.aggregateExpressions.toIndexedSeq.map {
      _.transformDown {
        case ae: AggregateExpression => rewriteAggregateExpression(ae, expressionMap)

        case a: Alias if expressionMap.contains(a.child.canonicalized) =>
          val findAlias = expressionMap(a.child.canonicalized)
          if (a.semanticEquals(findAlias)) {
            // We need to preserve the Alias in AggregateExpressions, but when the Alias
            // in groupingExpressions and aggExpressions are consistent, we can replace
            // them with the corresponding Attribute, because the name hasn't changed.
            findAlias.toAttribute
          } else {
            a.withNewChildren(Seq(findAlias.toAttribute))
          }

        case e if expressionMap.contains(e.canonicalized) =>
          expressionMap(e.canonicalized).toAttribute
      }
    }

    agg.copy(
      groupingExpressions = newGroupingExpressions,
      aggregateExpressions = newAggregateExpressions.asInstanceOf[Seq[NamedExpression]],
      child =
        Project(eliminateProjectList(agg.child.outputSet, expressionMap.values.toSeq), agg.child)
    )
  }

  override def apply(plan: LogicalPlan): LogicalPlan = LogicalPlanSelector.maybe(session, plan) {
    if (GlutenConfig.getConf.enableAnsiMode) {
      // Gluten not support Ansi Mode, not pull out pre-project
      return plan
    }
    plan.transformUpWithPruning(_.containsAnyPattern(AGGREGATE, FILTER)) {
      case filter: Filter
          if SparkShimLoader.getSparkShims.needsPreProjectForBloomFilterAgg(filter)(
            needsPreProject) =>
        SparkShimLoader.getSparkShims.addPreProjectForBloomFilter(filter)(transformAgg)

      case agg: Aggregate if needsPreProject(agg) =>
        transformAgg(agg)
    }
  }
}
