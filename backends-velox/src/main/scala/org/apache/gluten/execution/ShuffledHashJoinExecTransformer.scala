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
package org.apache.gluten.execution

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.joins.BuildSideRelation

import io.substrait.proto.JoinRel

case class ShuffledHashJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean)
  extends ShuffledHashJoinExecTransformerBase(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right,
    isSkewJoin) {

  // Used to specify the preferred build side in backend's real execution.
  object PreferredBuildSide extends Serializable {
    val LEFT = "left table"
    val RIGHT = "right table"
    val NON = "none"
  }

  /**
   * Returns whether the plan matches the condition to be preferred as build side. Currently, filter
   * and aggregation are preferred.
   * @param plan
   *   the left or right plan of join
   * @return
   *   whether the plan matches the condition
   */
  private def matchCondition(plan: SparkPlan): Boolean =
    plan.isInstanceOf[FilterExecTransformerBase] || plan.isInstanceOf[FilterExec] ||
      plan.isInstanceOf[BaseAggregateExec]

  /**
   * Returns whether a plan is preferred as the build side. If this plan or its children match the
   * condition, it will be preferred.
   * @param plan
   *   the left or right plan of join
   * @return
   *   whether the plan is preferred as the build side
   */
  private def isPreferred(plan: SparkPlan): Boolean =
    matchCondition(plan) || plan.children.exists(child => matchCondition(child))

  // Returns the preferred build side with the consideration of preferring condition.
  private lazy val preferredBuildSide: String =
    if ((isPreferred(left) && isPreferred(right)) || (!isPreferred(left) && !isPreferred(right))) {
      PreferredBuildSide.NON
    } else if (isPreferred(left)) {
      PreferredBuildSide.LEFT
    } else {
      PreferredBuildSide.RIGHT
    }

  /**
   * Returns whether the build and stream table should be exchanged with consideration of build
   * type, planned build side and the preferred build side.
   */
  override lazy val needSwitchChildren: Boolean = hashJoinType match {
    case LeftOuter | LeftSemi | ExistenceJoin(_) =>
      joinBuildSide match {
        case BuildLeft =>
          // Exchange build and stream side when left side or none is preferred as the build side,
          // and RightOuter or RightSemi wil be used.
          !(preferredBuildSide == PreferredBuildSide.RIGHT)
        case _ =>
          // Do not exchange build and stream side when right side or none is preferred
          // as the build side, and LeftOuter or LeftSemi wil be used.
          preferredBuildSide == PreferredBuildSide.LEFT
      }
    case RightOuter =>
      joinBuildSide match {
        case BuildRight =>
          // Do not exchange build and stream side when right side or none is preferred
          // as the build side, and RightOuter will be used.
          preferredBuildSide == PreferredBuildSide.LEFT
        case _ =>
          // Exchange build and stream side when left side or none is preferred as the build side,
          // and LeftOuter will be used.
          !(preferredBuildSide == PreferredBuildSide.RIGHT)
      }
    case _ =>
      joinBuildSide match {
        case BuildLeft => true
        case BuildRight => false
      }
  }

  override protected lazy val substraitJoinType: JoinRel.JoinType = joinType match {
    case _: InnerLike =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter =>
      if (needSwitchChildren) {
        JoinRel.JoinType.JOIN_TYPE_RIGHT
      } else {
        JoinRel.JoinType.JOIN_TYPE_LEFT
      }
    case RightOuter =>
      if (needSwitchChildren) {
        JoinRel.JoinType.JOIN_TYPE_LEFT
      } else {
        JoinRel.JoinType.JOIN_TYPE_RIGHT
      }
    case LeftSemi | ExistenceJoin(_) =>
      if (needSwitchChildren) {
        JoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI
      } else {
        JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
      }
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_ANTI
    case _ =>
      JoinRel.JoinType.UNRECOGNIZED
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): ShuffledHashJoinExecTransformer =
    copy(left = newLeft, right = newRight)
}

case class BroadcastHashJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean)
  extends BroadcastHashJoinExecTransformerBase(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right,
    isNullAwareAntiJoin) {

  override protected lazy val substraitJoinType: JoinRel.JoinType = joinType match {
    case _: InnerLike =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter | RightOuter =>
      // The right side is required to be used for building hash table in Substrait plan.
      // Therefore, for RightOuter Join, the left and right relations are exchanged and the
      // join type is reverted.
      JoinRel.JoinType.JOIN_TYPE_LEFT
    case LeftSemi | ExistenceJoin(_) =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_ANTI
    case _ =>
      // TODO: Support cross join with Cross Rel
      JoinRel.JoinType.UNRECOGNIZED
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): BroadcastHashJoinExecTransformer =
    copy(left = newLeft, right = newRight)

  override protected def createBroadcastBuildSideRDD(): BroadcastBuildSideRDD = {
    val broadcast = buildPlan.executeBroadcast[BuildSideRelation]()
    VeloxBroadcastBuildSideRDD(sparkContext, broadcast)
  }
}
