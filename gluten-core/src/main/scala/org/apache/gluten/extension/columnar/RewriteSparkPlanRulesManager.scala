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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.window.WindowExec

case class RewrittenNodeWall(originalChild: SparkPlan) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def supportsColumnar: Boolean = originalChild.supportsColumnar
  override def output: Seq[Attribute] = originalChild.output
  override def outputOrdering: Seq[SortOrder] = originalChild.outputOrdering
  override def outputPartitioning: Partitioning = originalChild.outputPartitioning
}

/**
 * A rule that holds a batch of [[Rule]]s to rewrite spark plan. When an operator can not be
 * offloaded to native, we try to rewrite it, e.g., pull out the complex exprs, so that we have one
 * more chance to offload it. If the rewritten plan still can not be offloaded, fallback to origin.
 *
 * Note that, this rule does not touch and tag these operators who does not need to rewrite.
 */
class RewriteSparkPlanRulesManager(rewriteRules: Seq[Rule[SparkPlan]]) extends Rule[SparkPlan] {

  private def mayNeedRewrite(plan: SparkPlan): Boolean = {
    TransformHints.isTransformable(plan) && {
      plan match {
        case _: SortExec => true
        case _: TakeOrderedAndProjectExec => true
        case _: BaseAggregateExec => true
        case _: BaseJoinExec => true
        case _: WindowExec => true
        case _: FilterExec => true
        case _: FileSourceScanExec => true
        case _: ExpandExec => true
        case _: GenerateExec => true
        case _ => false
      }
    }
  }

  private def getTransformHintBack(
      origin: SparkPlan,
      rewrittenPlan: SparkPlan): Option[TransformHint] = {
    // The rewritten plan may contain more nodes than origin, here use the node name to get it back
    val target = rewrittenPlan.collect {
      case p if p.nodeName == origin.nodeName => p
    }
    assert(target.size == 1)
    TransformHints.getHintOption(target.head)
  }

  private def applyRewriteRules(origin: SparkPlan): (SparkPlan, Option[String]) = {
    try {
      val rewrittenPlan = rewriteRules.foldLeft(origin) {
        case (plan, rule) =>
          // Some rewrite rules may generate new parent plan node, we should use transform to
          // rewrite the original plan. For example, PullOutPreProject and PullOutPostProject
          // will generate post-project plan node.
          plan.transformUp { case p => rule.apply(p) }
      }
      (rewrittenPlan, None)
    } catch {
      case e: Exception => (origin, Option(e.getMessage))
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    val addHint = AddTransformHintRule()
    plan.transformUp {
      case origin if mayNeedRewrite(origin) =>
        // Add a wall to avoid transforming unnecessary nodes.
        val withWall = origin.mapChildren(RewrittenNodeWall)
        val (rewrittenPlan, error) = applyRewriteRules(withWall)
        if (error.isDefined) {
          // Return origin if there is an exception during rewriting rules.
          // Note, it is not expected, but it happens in CH backend when pulling out
          // aggregate.
          // TODO: Fix the exception and remove this branch
          TransformHints.tagNotTransformable(origin, error.get)
          origin
        } else if (withWall.fastEquals(rewrittenPlan)) {
          // Return origin if the rewrite rules do nothing.
          // We do not add tag and leave it to the outside `AddTransformHintRule`.
          origin
        } else {
          addHint.apply(rewrittenPlan)
          val hint = getTransformHintBack(origin, rewrittenPlan)
          if (hint.isDefined) {
            // If the rewritten plan is still not transformable, return the original plan.
            TransformHints.tag(origin, hint.get)
            origin
          } else {
            rewrittenPlan.transformUp {
              case wall: RewrittenNodeWall => wall.originalChild
              case p if p.logicalLink.isEmpty =>
                // Add logical link to pull out project to make fallback reason work,
                // see `GlutenFallbackReporter`.
                origin.logicalLink.foreach(p.setLogicalLink)
                p
            }
          }
        }
    }
  }
}
