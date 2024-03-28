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
package org.apache.gluten.cbo.best

import org.apache.gluten.cbo._
import org.apache.gluten.cbo.Best.KnownCostPath
import org.apache.gluten.cbo.dp.DpGroupAlgo
import org.apache.gluten.cbo.memo.MemoState

import scala.collection.mutable

trait BestFinder[T <: AnyRef] {
  def bestOf(groupId: Int): Best[T]
}

object BestFinder {
  def apply[T <: AnyRef](cbo: Cbo[T], memoState: MemoState[T]): BestFinder[T] = {
    unsafe(cbo, memoState, DpGroupAlgo.Adjustment.none())
  }

  def unsafe[T <: AnyRef](
      cbo: Cbo[T],
      memoState: MemoState[T],
      adjustment: DpGroupAlgo.Adjustment[T]): BestFinder[T] = {
    new GroupBasedBestFinder[T](cbo, memoState, adjustment)
  }

  case class KnownCostGroup[T <: AnyRef](
      nodeToCost: Map[CanonicalNode[T], KnownCostPath[T]],
      bestNode: CanonicalNode[T]) {
    def best(): KnownCostPath[T] = nodeToCost(bestNode)
  }

  case class KnownCostCluster[T <: AnyRef](groupToCost: Map[Int, KnownCostGroup[T]])

  private[best] def newBest[T <: AnyRef](
      cbo: Cbo[T],
      allGroups: Seq[CboGroup[T]],
      group: CboGroup[T],
      groupToCosts: Map[Int, KnownCostGroup[T]]): Best[T] = {
    val bestPath = groupToCosts(group.id()).best()
    val bestRoot = bestPath.cboPath.node()
    val winnerNodes = groupToCosts.map { case (id, g) => InGroupNode(id, g.bestNode) }.toSeq
    val costsMap = mutable.Map[InGroupNode[T], Cost]()
    groupToCosts.foreach {
      case (gid, g) =>
        g.nodeToCost.foreach {
          case (n, c) =>
            costsMap += (InGroupNode(gid, n) -> c.cost)
        }
    }
    Best(cbo, group.id(), bestPath, winnerNodes, costsMap.get)
  }
}
