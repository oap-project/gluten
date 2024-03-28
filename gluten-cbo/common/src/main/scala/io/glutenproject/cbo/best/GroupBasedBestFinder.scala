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
package io.glutenproject.cbo.best

import io.glutenproject.cbo._
import io.glutenproject.cbo.Best.{BestNotFoundException, KnownCostPath}
import io.glutenproject.cbo.best.BestFinder.KnownCostGroup
import io.glutenproject.cbo.dp.{DpGroupAlgo, DpGroupAlgoDef}
import io.glutenproject.cbo.memo.MemoState
import io.glutenproject.cbo.path.{CboPath, PathKeySet}

// The best path's each sub-path is considered optimal in its own group.
private class GroupBasedBestFinder[T <: AnyRef](
    cbo: Cbo[T],
    memoState: MemoState[T],
    adjustment: DpGroupAlgo.Adjustment[T])
  extends BestFinder[T] {
  import GroupBasedBestFinder._

  private val allGroups = memoState.allGroups()

  override def bestOf(groupId: Int): Best[T] = {
    val group = allGroups(groupId)
    val groupToCosts = fillBests(group)
    if (!groupToCosts.contains(groupId)) {
      throw BestNotFoundException(
        s"Best path not found. Memo state (Graphviz): \n" +
          s"${memoState.formatGraphvizWithoutBest(groupId)}")
    }
    BestFinder.newBest(cbo, allGroups, group, groupToCosts)
  }

  private def fillBests(group: CboGroup[T]): Map[Int, KnownCostGroup[T]] = {
    val algoDef = new AlgoDef(cbo, memoState)
    val solution = DpGroupAlgo.resolve(memoState, algoDef, adjustment, group)
    val bests = allGroups.flatMap {
      group =>
        if (solution.isYSolved(group)) {
          solution.solutionOfY(group).flatMap(kcg => Some(group.id() -> kcg))
        } else {
          None
        }
    }.toMap
    bests
  }
}

private object GroupBasedBestFinder {
  private[best] def algoDef[T <: AnyRef](cbo: Cbo[T], memoState: MemoState[T])
      : DpGroupAlgoDef[T, Option[KnownCostPath[T]], Option[KnownCostGroup[T]]] = {
    new AlgoDef(cbo, memoState)
  }

  private class AlgoDef[T <: AnyRef](cbo: Cbo[T], memoState: MemoState[T])
    extends DpGroupAlgoDef[T, Option[KnownCostPath[T]], Option[KnownCostGroup[T]]] {
    private val allGroups = memoState.allGroups()
    private val costComparator = cbo.costModel.costComparator()

    override def solveNode(
        ign: InGroupNode[T],
        childrenGroupsOutput: CboGroup[T] => Option[KnownCostGroup[T]])
        : Option[KnownCostPath[T]] = {
      val can = ign.can
      if (can.isLeaf()) {
        val path = CboPath.one(cbo, PathKeySet.trivial, allGroups, can)
        return Some(KnownCostPath(cbo, path))
      }
      val childrenGroups = can.getChildrenGroups(allGroups).map(gn => allGroups(gn.groupId()))
      val maybeBestChildrenPaths: Seq[Option[CboPath[T]]] = childrenGroups.map {
        childGroup => childrenGroupsOutput(childGroup).map(kcg => kcg.best().cboPath)
      }
      if (maybeBestChildrenPaths.exists(_.isEmpty)) {
        // Node should only be solved when all children outputs exist.
        return None
      }
      val bestChildrenPaths = maybeBestChildrenPaths.map(_.get)
      Some(KnownCostPath(cbo, path.CboPath(cbo, can, bestChildrenPaths).get))
    }

    override def solveGroup(
        group: CboGroup[T],
        nodesOutput: InGroupNode[T] => Option[KnownCostPath[T]]): Option[KnownCostGroup[T]] = {
      val nodes = group.nodes(memoState)
      // Allow unsolved children nodes while solving group.
      val flatNodesOutput =
        nodes.flatMap(n => nodesOutput(InGroupNode(group.id(), n)).map(kcp => n -> kcp)).toMap

      if (flatNodesOutput.isEmpty) {
        return None
      }
      val bestPath = flatNodesOutput.values.reduce {
        (left, right) =>
          Ordering
            .by((cp: KnownCostPath[T]) => cp.cost)(costComparator)
            .min(left, right)
      }
      Some(KnownCostGroup(flatNodesOutput, bestPath.cboPath.node().self().asCanonical()))
    }

    override def solveNodeOnCycle(node: InGroupNode[T]): Option[KnownCostPath[T]] =
      None

    override def solveGroupOnCycle(cluster: CboGroup[T]): Option[KnownCostGroup[T]] = {
      None
    }
  }
}
