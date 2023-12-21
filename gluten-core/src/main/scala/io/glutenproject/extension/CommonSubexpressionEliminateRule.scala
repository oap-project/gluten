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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

class CommonSubexpressionEliminateRule(session: SparkSession, conf: SQLConf)
  extends Rule[LogicalPlan]
  with Logging {

  private var lastPlan: LogicalPlan = null

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val newPlan = if (plan.resolved && !plan.fastEquals(lastPlan)) {
      lastPlan = plan
      visitPlan(plan)
    } else {
      plan
    }
    newPlan
  }

  private case class AliasAndAttribute(alias: Alias, attribute: Attribute)

  private case class RewriteContext(exprs: Seq[Expression], child: LogicalPlan)

  private def visitPlan(plan: LogicalPlan): LogicalPlan = {
    var newPlan = plan match {
      case project: Project => visitProject(project)
      // TODO: CSE in Filter doesn't work for unknown reason, need to fix it later
      // case filter: Filter => visitFilter(filter)
      case window: Window => visitWindow(window)
      case aggregate: Aggregate => visitAggregate(aggregate)
      case sort: Sort => visitSort(sort)
      case other =>
        val children = other.children.map(visitPlan)
        other.withNewChildren(children)
    }

    if (newPlan.output.size == plan.output.size) {
      return newPlan
    }

    // Add a Project to trim unnecessary attributes(which are always at the end of the output)
    val postProjectList = newPlan.output.take(plan.output.size)
    Project(postProjectList, newPlan)
  }

  private def replaceCommonExprWithAttribute(
      expr: Expression,
      commonExprMap: mutable.HashMap[ExpressionEquals, AliasAndAttribute]): Expression = {
    val exprEquals = commonExprMap.get(ExpressionEquals(expr))
    if (exprEquals.isDefined) {
      exprEquals.get.attribute
    } else {
      expr.mapChildren(replaceCommonExprWithAttribute(_, commonExprMap))
    }
  }

  private def isValidCommonExpr(expr: Expression): Boolean = {
    logDebug(s"check expr $expr class ${expr.getClass.toString}")
    if (expr.isInstanceOf[Unevaluable] || expr.isInstanceOf[AggregateFunction]) {
      return false
    }

    expr.children.forall(isValidCommonExpr(_))
  }

  private def rewrite(inputCtx: RewriteContext): RewriteContext = {
    logDebug(s"Start rewrite with input exprs:${inputCtx.exprs} input child:${inputCtx.child}")
    val equivalentExpressions = new EquivalentExpressions
    inputCtx.exprs.foreach(equivalentExpressions.addExprTree(_))

    // Get all the expressions that appear at least twice
    val newChild = visitPlan(inputCtx.child)
    val commonExprs = equivalentExpressions.getCommonSubexpressions

    // Put the common expressions into a hash map
    val commonExprMap = mutable.HashMap.empty[ExpressionEquals, AliasAndAttribute]
    commonExprs.foreach {
      expr =>
        if (!expr.foldable && !expr.isInstanceOf[Attribute] && isValidCommonExpr(expr)) {
          logDebug(s"Common expr $expr class ${expr.getClass.toString}")
          val exprEquals = ExpressionEquals(expr)
          val alias = Alias(expr, expr.toString)()
          val attribute = alias.toAttribute
          commonExprMap.put(exprEquals, AliasAndAttribute(alias, attribute))
        }
    }

    if (commonExprMap.isEmpty) {
      logDebug(s"commonExprMap is empty all exprs: ${equivalentExpressions.debugString(true)}")
      return RewriteContext(inputCtx.exprs, newChild)
    }

    // Generate pre-project as new child
    var preProjectList = newChild.output ++ commonExprMap.values.map(_.alias)
    val preProject = Project(preProjectList, newChild)
    logDebug(s"newChild: $preProject")

    // Replace the common expressions with the first expression that produces it.
    try {
      var newExprs = inputCtx.exprs
        .map(replaceCommonExprWithAttribute(_, commonExprMap))
      logDebug(s"newExprs: $newExprs")
      RewriteContext(newExprs, preProject)
    } catch {
      case e: Exception =>
        logWarning(
          s"Common subexpression eliminate failed with exception: ${e.getMessage}" +
            s" while replace ${inputCtx.exprs} with $commonExprMap, fallback now")
        RewriteContext(inputCtx.exprs, newChild)
    }
  }

  private def visitProject(project: Project): Project = {
    val inputCtx = RewriteContext(project.projectList, project.child)
    val outputCtx = rewrite(inputCtx)
    Project(outputCtx.exprs.map(_.asInstanceOf[NamedExpression]), outputCtx.child)
  }

  private def visitFilter(filter: Filter): Filter = {
    val inputCtx = RewriteContext(Seq(filter.condition), filter.child)
    val outputCtx = rewrite(inputCtx)
    Filter(outputCtx.exprs.head, outputCtx.child)
  }

  private def visitWindow(window: Window): Window = {
    val inputCtx = RewriteContext(window.windowExpressions, window.child)
    val outputCtx = rewrite(inputCtx)
    Window(
      outputCtx.exprs.map(_.asInstanceOf[NamedExpression]),
      window.partitionSpec,
      window.orderSpec,
      outputCtx.child)
  }

  private def visitAggregate(aggregate: Aggregate): Aggregate = {
    val groupingSize = aggregate.groupingExpressions.size
    val aggregateSize = aggregate.aggregateExpressions.size

    val inputCtx = RewriteContext(
      aggregate.groupingExpressions ++ aggregate.aggregateExpressions,
      aggregate.child)
    val outputCtx = rewrite(inputCtx)
    Aggregate(
      outputCtx.exprs.slice(0, groupingSize),
      outputCtx.exprs
        .slice(groupingSize, groupingSize + aggregateSize)
        .map(_.asInstanceOf[NamedExpression]),
      outputCtx.child
    )
  }

  private def visitSort(sort: Sort): Sort = {
    val exprs = sort.order.flatMap(_.children)
    val inputCtx = RewriteContext(exprs, sort.child)
    val outputCtx = rewrite(inputCtx)

    var start = 0;
    var newOrder = Seq.empty[SortOrder]
    sort.order.foreach(
      order => {
        val childrenSize = order.children.size
        val newChildren = outputCtx.exprs.slice(start, start + childrenSize)
        newOrder = newOrder :+ order.withNewChildren(newChildren).asInstanceOf[SortOrder]
        start += childrenSize
      })

    Sort(newOrder, sort.global, outputCtx.child)
  }
}
