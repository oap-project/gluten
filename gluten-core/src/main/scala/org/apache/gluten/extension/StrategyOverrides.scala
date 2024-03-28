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

import org.apache.gluten.{GlutenConfig, GlutenSparkExtensionsInjector}
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar.TRANSFORM_UNSUPPORTED
import org.apache.gluten.extension.columnar.TransformHints.TAG
import org.apache.gluten.utils.LogicalPlanSelector

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{joins, JoinSelectionShim, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, LogicalQueryStage}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

object StrategyOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(JoinSelectionOverrides)
  }
}

case class JoinSelectionOverrides(session: SparkSession)
  extends Strategy
  with JoinSelectionHelper
  with SQLConfHelper {

  private def isBroadcastStage(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, _: BroadcastQueryStageExec) => true
    case _ => false
  }

  def extractEqualJoinKeyCondition(
      joinType: JoinType,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      left: LogicalPlan,
      right: LogicalPlan,
      hint: JoinHint,
      forceShuffledHashJoin: Boolean): Seq[SparkPlan] = {
    if (isBroadcastStage(left) || isBroadcastStage(right)) {
      // equal condition
      val buildSide = if (isBroadcastStage(left)) BuildLeft else BuildRight
      Seq(
        BroadcastHashJoinExec(
          leftKeys,
          rightKeys,
          joinType,
          buildSide,
          condition,
          planLater(left),
          planLater(right)))
    } else {
      // non equal condition
      // Generate BHJ here, avoid to do match in `JoinSelection` again.
      val isHintEmpty = hint.leftHint.isEmpty && hint.rightHint.isEmpty
      val buildSide = getBroadcastBuildSide(left, right, joinType, hint, !isHintEmpty, conf)
      if (buildSide.isDefined) {
        return Seq(
          joins.BroadcastHashJoinExec(
            leftKeys,
            rightKeys,
            joinType,
            buildSide.get,
            condition,
            planLater(left),
            planLater(right)))
      }

      if (
        forceShuffledHashJoin &&
        !BackendsApiManager.getSparkPlanExecApiInstance.joinFallback(
          joinType,
          left.outputSet,
          right.outputSet,
          condition) &&
        !left.getTagValue(TAG).isDefined &&
        !right.getTagValue(TAG).isDefined
      ) {
        // Force use of ShuffledHashJoin in preference to SortMergeJoin. With no respect to
        // conf setting "spark.sql.join.preferSortMergeJoin".
        val (leftBuildable, rightBuildable) =
          if (BackendsApiManager.getSettings.utilizeShuffledHashJoinHint()) {
            // Currently, ClickHouse backend can not support AQE, so it needs to use join hint
            // to decide the build side, after supporting AQE, will remove this.
            val leftHintEnabled = hintToShuffleHashJoinLeft(hint)
            val rightHintEnabled = hintToShuffleHashJoinRight(hint)
            val leftHintMergeEnabled = hint.leftHint.exists(_.strategy.contains(SHUFFLE_MERGE))
            val rightHintMergeEnabled = hint.rightHint.exists(_.strategy.contains(SHUFFLE_MERGE))
            if (leftHintEnabled || rightHintEnabled) {
              (leftHintEnabled, rightHintEnabled)
            } else if (leftHintMergeEnabled || rightHintMergeEnabled) {
              // hack: when set SHUFFLE_MERGE hint, it means that
              // it don't use this side as the build side
              (!leftHintMergeEnabled, !rightHintMergeEnabled)
            } else {
              (canBuildShuffledHashJoinLeft(joinType), canBuildShuffledHashJoinRight(joinType))
            }
          } else {
            (canBuildShuffledHashJoinLeft(joinType), canBuildShuffledHashJoinRight(joinType))
          }

        if (!leftBuildable && !rightBuildable) {
          return Nil
        }
        val buildSide = if (!leftBuildable) {
          BuildRight
        } else if (!rightBuildable) {
          BuildLeft
        } else {
          getSmallerSide(left, right)
        }

        return Option(buildSide)
          .map {
            buildSide =>
              Seq(
                joins.ShuffledHashJoinExec(
                  leftKeys,
                  rightKeys,
                  joinType,
                  buildSide,
                  condition,
                  planLater(left),
                  planLater(right)))
          }
          .getOrElse(Nil)
      }
      Nil
    }
  }

  override def canBuildShuffledHashJoinLeft(joinType: JoinType): Boolean = {
    BackendsApiManager.getSettings.supportHashBuildJoinTypeOnLeft(joinType)
  }

  override def canBuildShuffledHashJoinRight(joinType: JoinType): Boolean = {
    BackendsApiManager.getSettings.supportHashBuildJoinTypeOnRight(joinType)
  }

  def existsMultiJoins(plan: LogicalPlan, count: Int = 0): Boolean = {
    plan match {
      case plan: Join =>
        if ((count + 1) >= GlutenConfig.getConf.logicalJoinOptimizationThrottle) return true
        plan.children.exists(existsMultiJoins(_, count + 1))
      case plan: Project =>
        if ((count + 1) >= GlutenConfig.getConf.logicalJoinOptimizationThrottle) return true
        plan.children.exists(existsMultiJoins(_, count + 1))
      case other => false
    }
  }

  def tagNotTransformable(plan: LogicalPlan, reason: String): LogicalPlan = {
    plan.setTagValue(TAG, TRANSFORM_UNSUPPORTED(Some(reason)))
    plan
  }

  def tagNotTransformableRecursive(plan: LogicalPlan, reason: String): LogicalPlan = {
    tagNotTransformable(
      plan.withNewChildren(plan.children.map(tagNotTransformableRecursive(_, reason))),
      reason)
  }

  def existLeftOuterJoin(plan: LogicalPlan): Boolean = {
    plan.collect {
      case join: Join if join.joinType.sql.equals("LEFT OUTER") =>
        return true
    }.size > 0
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] =
    LogicalPlanSelector.maybeNil(session, plan) {
      // Ignore forceShuffledHashJoin if exist multi continuous joins
      if (
        GlutenConfig.getConf.enableLogicalJoinOptimize &&
        existsMultiJoins(plan) && existLeftOuterJoin(plan)
      ) {
        tagNotTransformableRecursive(plan, "exist multi continuous joins")
      }
      plan match {
        // If the build side of BHJ is already decided by AQE, we need to keep the build side.
        case JoinSelectionShim.ExtractEquiJoinKeysShim(
              joinType,
              leftKeys,
              rightKeys,
              condition,
              left,
              right,
              hint) =>
          extractEqualJoinKeyCondition(
            joinType,
            leftKeys,
            rightKeys,
            condition,
            left,
            right,
            hint,
            GlutenConfig.getConf.forceShuffledHashJoin)
        case _ => Nil
      }
    }
}
