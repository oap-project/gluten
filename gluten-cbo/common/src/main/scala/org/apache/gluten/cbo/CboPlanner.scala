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
package org.apache.gluten.cbo

import org.apache.gluten.cbo.CboConfig.PlannerType
import org.apache.gluten.cbo.dp.DpPlanner
import org.apache.gluten.cbo.exaustive.ExhaustivePlanner
import org.apache.gluten.cbo.memo.MemoState
import org.apache.gluten.cbo.path.CboPath
import org.apache.gluten.cbo.property.PropertySet
import org.apache.gluten.cbo.vis.GraphvizVisualizer

import scala.collection.mutable

trait CboPlanner[T <: AnyRef] {
  def plan(): T
  def newState(): PlannerState[T]
}

object CboPlanner {
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      altConstraintSets: Seq[PropertySet[T]],
      constraintSet: PropertySet[T],
      plan: T): CboPlanner[T] = {
    cbo.config.plannerType match {
      case PlannerType.Exhaustive =>
        ExhaustivePlanner(cbo, altConstraintSets, constraintSet, plan)
      case PlannerType.Dp =>
        DpPlanner(cbo, altConstraintSets, constraintSet, plan)
    }
  }
}

trait Best[T <: AnyRef] {
  import Best._
  def rootGroupId(): Int
  def bestNodes(): Set[InGroupNode[T]]
  def winnerNodes(): Set[InGroupNode[T]]
  def costs(): InGroupNode[T] => Option[Cost]
  def path(): KnownCostPath[T]
}

object Best {
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      rootGroupId: Int,
      bestPath: KnownCostPath[T],
      winnerNodes: Seq[InGroupNode[T]],
      costs: InGroupNode[T] => Option[Cost]): Best[T] = {
    val bestNodes = mutable.Set[InGroupNode[T]]()

    def dfs(groupId: Int, cursor: CboPath.PathNode[T]): Unit = {
      val can = cursor.self().asCanonical()
      bestNodes += InGroupNode(groupId, can)
      cursor.zipChildrenWithGroupIds().foreach {
        case (childPathNode, childGroupId) =>
          dfs(childGroupId, childPathNode)
      }
    }

    dfs(rootGroupId, bestPath.cboPath.node())

    val winnerNodeSet = winnerNodes.toSet

    BestImpl(cbo, rootGroupId, bestPath, bestNodes.toSet, winnerNodeSet, costs)
  }

  private case class BestImpl[T <: AnyRef](
      cbo: Cbo[T],
      override val rootGroupId: Int,
      override val path: KnownCostPath[T],
      override val bestNodes: Set[InGroupNode[T]],
      override val winnerNodes: Set[InGroupNode[T]],
      override val costs: InGroupNode[T] => Option[Cost])
    extends Best[T]

  trait KnownCostPath[T <: AnyRef] {
    def cboPath: CboPath[T]
    def cost: Cost
  }

  object KnownCostPath {
    def apply[T <: AnyRef](cbo: Cbo[T], cboPath: CboPath[T]): KnownCostPath[T] = {
      KnownCostPathImpl(cboPath, cbo.costModel.costOf(cboPath.plan()))
    }

    def apply[T <: AnyRef](cboPath: CboPath[T], cost: Cost): KnownCostPath[T] = {
      KnownCostPathImpl(cboPath, cost)
    }

    private case class KnownCostPathImpl[T <: AnyRef](cboPath: CboPath[T], cost: Cost)
      extends KnownCostPath[T]
  }

  case class BestNotFoundException(message: String, cause: Exception)
    extends RuntimeException(message, cause)
  object BestNotFoundException {
    def apply(message: String): BestNotFoundException = {
      BestNotFoundException(message, null)
    }
    def apply(): BestNotFoundException = {
      BestNotFoundException(null, null)
    }
  }
}

trait PlannerState[T <: AnyRef] {
  def cbo(): Cbo[T]
  def memoState(): MemoState[T]
  def rootGroupId(): Int
  def best(): Best[T]
}

object PlannerState {
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      memoState: MemoState[T],
      rootGroupId: Int,
      best: Best[T]): PlannerState[T] = {
    PlannerStateImpl(cbo, memoState, rootGroupId, best)
  }

  implicit class PlannerStateImplicits[T <: AnyRef](state: PlannerState[T]) {
    def formatGraphviz(): String = {
      formatGraphvizWithBest()
    }

    private def formatGraphvizWithBest(): String = {
      GraphvizVisualizer(state.cbo(), state.memoState(), state.best()).format()
    }
  }

  private case class PlannerStateImpl[T <: AnyRef] private (
      override val cbo: Cbo[T],
      override val memoState: MemoState[T],
      override val rootGroupId: Int,
      override val best: Best[T])
    extends PlannerState[T]
}
