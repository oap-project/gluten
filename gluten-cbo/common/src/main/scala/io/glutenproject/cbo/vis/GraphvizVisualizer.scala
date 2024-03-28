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
package io.glutenproject.cbo.vis

import io.glutenproject.cbo._
import io.glutenproject.cbo.best.BestFinder
import io.glutenproject.cbo.memo.MemoState
import io.glutenproject.cbo.path._

import scala.collection.mutable

// Visualize the planning procedure using dot language.
class GraphvizVisualizer[T <: AnyRef](cbo: Cbo[T], memoState: MemoState[T], best: Best[T]) {

  private val allGroups = memoState.allGroups()
  private val allClusters = memoState.clusterLookup()

  def format(): String = {
    val rootGroupId = best.rootGroupId()
    val bestPath = best.path()
    val winnerNodes = best.winnerNodes()
    val bestNodes = best.bestNodes()
    val costs = best.costs()
    val rootGroup = allGroups(rootGroupId)

    val buf = new StringBuilder()
    buf.append("digraph G {\n")
    buf.append("  compound=true;\n")

    object IsBestNode {
      def unapply(nodeAndGroupToTest: (CanonicalNode[T], CboGroup[T])): Boolean = {
        bestNodes.contains(InGroupNode(nodeAndGroupToTest._2.id(), nodeAndGroupToTest._1))
      }
    }

    object IsWinnerNode {
      def unapply(nodeAndGroupToTest: (CanonicalNode[T], CboGroup[T])): Boolean = {
        winnerNodes.contains(InGroupNode(nodeAndGroupToTest._2.id(), nodeAndGroupToTest._1))
      }
    }

    val clusterToGroups: mutable.Map[CboClusterKey, mutable.Set[Int]] = mutable.Map()

    allGroups.foreach {
      group => clusterToGroups.getOrElseUpdate(group.clusterKey(), mutable.Set()).add(group.id())
    }

    val groupToDotClusterId: mutable.Map[Int, Int] = mutable.Map()
    var dotClusterId = 0
    allClusters.foreach {
      case (clusterKey, cluster) =>
        buf.append(s"  subgraph cluster$dotClusterId {\n")
        dotClusterId = dotClusterId + 1
        buf.append(s"    label=${'"'}${describeCluster(clusterKey)}${'"'}\n")
        clusterToGroups(clusterKey).map(allGroups(_)).foreach {
          group =>
            buf.append(s"    subgraph cluster$dotClusterId {\n")
            groupToDotClusterId += group.id() -> dotClusterId
            dotClusterId = dotClusterId + 1
            buf.append(s"      label=${'"'}${describeGroupVerbose(group)}${'"'}\n")
            group.nodes(memoState).foreach {
              node =>
                {
                  buf.append(s"      ${'"'}${describeNode(costs, group, node)}${'"'}")
                  (node, group) match {
                    case IsBestNode() =>
                      buf.append(" [style=filled, fillcolor=green] ")
                    case IsWinnerNode() =>
                      buf.append(" [style=filled, fillcolor=grey] ")
                    case _ =>
                  }
                  buf.append("\n")
                }
            }
            buf.append("    }\n")
        }
        buf.append("  }\n")
    }

    allGroups.foreach {
      group =>
        group.nodes(memoState).foreach {
          node =>
            node.getChildrenGroups(allGroups).map(_.group(allGroups)).foreach {
              childGroup =>
                val childGroupNodes = childGroup.nodes(memoState)
                if (childGroupNodes.nonEmpty) {
                  val randomChild = childGroupNodes.head
                  buf.append(
                    s"  ${'"'}${describeNode(costs, group, node)}${'"'} -> " +
                      s"${'"'}${describeNode(costs, childGroup, randomChild)}${'"'}  " +
                      s"[lhead=${'"'}cluster${groupToDotClusterId(childGroup.id())}${'"'}]\n")
                }
            }
        }
    }

    def drawBest(bestNode: CboPath.PathNode[T], bestGroup: CboGroup[T]): Unit = {
      val canonical = bestNode.self().asCanonical()
      bestNode
        .zipChildrenWithGroups(allGroups)
        .foreach {
          case (child, childGroup) =>
            val childCanonical = child.self().asCanonical()
            buf.append(s"  ${'"'}${describeNode(costs, bestGroup, canonical)}${'"'} -> ")
            buf.append(s"  ${'"'}${describeNode(costs, childGroup, childCanonical)}${'"'}")
            buf.append(s"  [penwidth=${'"'}3.0${'"'} color=${'"'}green${'"'}]")
            buf.append("\n")
            drawBest(child, childGroup)
        }
    }

    // Usually the best path should be a valid path which doesn't have group leaf end.
    // Since there might be cases that best path was not found for some reason and
    // user needs the graph for debug purpose, we loose the restriction on best path
    // here by filtering out the illegal ones.
    val rootNode = bestPath.cboPath.node()
    if (rootNode.self().isCanonical) {
      drawBest(rootNode, rootGroup)
    }

    buf.append("}\n")
    buf.toString()
  }

  private def describeCluster(cluster: CboClusterKey): String = {
    s"[Cluster $cluster]"
  }

  private def describeGroup(group: CboGroup[T]): String = {
    s"[Group ${group.id()}]"
  }

  private def describeGroupVerbose(group: CboGroup[T]): String = {
    s"[Group ${group.id()}: ${group.propSet().getMap.values.toIndexedSeq}]"
  }

  private def describeNode(
      costs: InGroupNode[T] => Option[Cost],
      group: CboGroup[T],
      node: CanonicalNode[T]): String = {
    s"${describeGroup(group)}[Cost ${costs(InGroupNode(group.id(), node))
        .map {
          case c if cbo.isInfCost(c) => "<INF>"
          case other => other
        }
        .getOrElse("N/A")}]${cbo.explain.describeNode(node.self())}"
  }
}

object GraphvizVisualizer {
  private class FakeBestFinder[T <: AnyRef](cbo: Cbo[T], allGroups: Int => CboGroup[T])
    extends BestFinder[T] {
    import FakeBestFinder._
    override def bestOf(groupId: Int): Best[T] = {
      new FakeBest(cbo, allGroups, groupId)
    }
  }

  private object FakeBestFinder {
    private class FakeBest[T <: AnyRef](
        cbo: Cbo[T],
        allGroups: Int => CboGroup[T],
        rootGroupId: Int)
      extends Best[T] {
      override def rootGroupId(): Int = {
        rootGroupId
      }
      override def bestNodes(): Set[InGroupNode[T]] = {
        Set()
      }
      override def winnerNodes(): Set[InGroupNode[T]] = {
        Set()
      }
      override def costs(): InGroupNode[T] => Option[Cost] = { _ => None }

      override def path(): Best.KnownCostPath[T] = {
        Best.KnownCostPath(
          CboPath.zero(cbo, PathKeySet.trivial, GroupNode(cbo, allGroups(rootGroupId))),
          cbo.getInfCost())
      }
    }
  }

  def apply[T <: AnyRef](
      cbo: Cbo[T],
      memoState: MemoState[T],
      rootGroupId: Int): GraphvizVisualizer[T] = {
    val fakeBestFinder = new FakeBestFinder[T](cbo, memoState.allGroups())
    val fakeBest = fakeBestFinder.bestOf(rootGroupId)
    new GraphvizVisualizer(cbo, memoState, fakeBest)
  }

  def apply[T <: AnyRef](
      cbo: Cbo[T],
      memoState: MemoState[T],
      best: Best[T]): GraphvizVisualizer[T] = {
    new GraphvizVisualizer(cbo, memoState, best)
  }
}
