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
package org.apache.gluten.cbo.specific

import org.apache.gluten.cbo._
import org.apache.gluten.cbo.best.BestFinder
import org.apache.gluten.cbo.memo.MemoState
import org.apache.gluten.cbo.mock.MockMemoState
import org.apache.gluten.cbo.path.{CboPath, PathFinder}
import org.apache.gluten.cbo.rule.CboRule

import org.scalatest.funsuite.AnyFunSuite

class GroupBastBestFinderCyclicSearchSpaceSuite extends CyclicSearchSpaceSuite {
  override protected def newBestFinder[T <: AnyRef](
      cbo: Cbo[T],
      memoState: MemoState[T]): BestFinder[T] = BestFinder(cbo, memoState)
}

abstract class CyclicSearchSpaceSuite extends AnyFunSuite {
  import CyclicSearchSpaceSuite._

  protected def newBestFinder[T <: AnyRef](cbo: Cbo[T], memoState: MemoState[T]): BestFinder[T]

  test("Cyclic - find paths, simple self cycle") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.none())

    val mock = MockMemoState.Builder(cbo)
    val cluster = mock.newCluster()

    val groupA = cluster.newGroup()

    val node1 = Unary("node1", groupA.self).asCanonical(cbo)
    val node2 = Leaf("node2", 1).asCanonical(cbo)

    groupA.add(List(node1, node2))

    cluster.addNodes(List(node1, node2))

    val mockState = mock.build()

    def find(can: CanonicalNode[TestNode], depth: Int): Iterable[CboPath[TestNode]] = {
      PathFinder.builder(cbo, mockState).depth(depth).build().find(can)
    }

    assert(find(node1, 1).map(p => p.plan()) == List(Unary("node1", Group(0))))
    assert(find(node1, 2).map(p => p.plan()) == List(Unary("node1", Leaf("node2", 1))))
    assert(find(node1, 3).map(p => p.plan()) == List(Unary("node1", Leaf("node2", 1))))
    assert(
      find(node1, CboPath.INF_DEPTH).map(p => p.plan()) == List(Unary("node1", Leaf("node2", 1))))
  }

  test("Cyclic - find best, simple self cycle") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.none())

    val mock = MockMemoState.Builder(cbo)
    val cluster = mock.newCluster()

    val groupA = cluster.newGroup()

    val node1 = Unary("node1", groupA.self).asCanonical(cbo)
    val node2 = Leaf("node2", 1).asCanonical(cbo)

    groupA.add(List(node1, node2))

    cluster.addNodes(List(node1, node2))

    val mockState = mock.build()
    val bestFinder = newBestFinder(cbo, mockState)
    val best = bestFinder.bestOf(groupA.id).path()
    assert(best.cboPath.plan() == Leaf("node2", 1))
    assert(best.cost == LongCost(1))
  }

  test("Cyclic - find best, case 1") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.none())

    val mock = MockMemoState.Builder(cbo)
    val cluster = mock.newCluster()

    val groupA = cluster.newGroup()
    val groupB = cluster.newGroup()
    val groupC = cluster.newGroup()
    val groupD = cluster.newGroup()
    val groupE = cluster.newGroup()
    val groupF = cluster.newGroup()
    val groupG = cluster.newGroup()
    val groupH = cluster.newGroup()

    val node1 = Binary("node1", groupB.self, groupC.self).asCanonical(cbo)
    val node2 = Unary("node2", groupF.self).asCanonical(cbo)
    val node3 = Binary("node3", groupD.self, groupF.self).asCanonical(cbo)
    val node4 = Binary("node4", groupG.self, groupH.self).asCanonical(cbo)
    val node5 = Unary("node5", groupC.self).asCanonical(cbo)
    val node6 = Unary("node6", groupE.self).asCanonical(cbo)
    val node7 = Leaf("node7", 1).asCanonical(cbo)
    val node8 = Leaf("node8", 1).asCanonical(cbo)
    val node9 = Leaf("node9", 1).asCanonical(cbo)
    // The best path should avoid including this node to most extent.
    val node10 = Leaf("node10", 100).asCanonical(cbo)

    groupA.add(node1)
    groupB.add(node2)
    groupC.add(List(node3, node4))
    groupD.add(node9)
    groupE.add(node5)
    groupF.add(List(node6, node10))
    groupG.add(node7)
    groupH.add(node8)

    cluster.addNodes(List(node1, node2, node3, node4, node5, node6, node7, node8, node9))

    val mockState = mock.build()

    val bestFinder = newBestFinder(cbo, mockState)

    def assertBestOf(group: CboGroup[TestNode])(assertion: Best[TestNode] => Unit): Unit = {
      val best = bestFinder.bestOf(group.id())
      assertion(best)
    }

    assertBestOf(groupA)(best => assert(best.path().cost == LongCost(10)))
    assertBestOf(groupB)(best => assert(best.path().cost == LongCost(6)))
    assertBestOf(groupC)(best => assert(best.path().cost == LongCost(3)))
    assertBestOf(groupD)(best => assert(best.path().cost == LongCost(1)))
    assertBestOf(groupE)(best => assert(best.path().cost == LongCost(4)))
    assertBestOf(groupF)(best => assert(best.path().cost == LongCost(5)))
    assertBestOf(groupG)(best => assert(best.path().cost == LongCost(1)))
    assertBestOf(groupH)(best => assert(best.path().cost == LongCost(1)))
  }

  test("Cyclic - find best, case 2") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.none())

    val mock = MockMemoState.Builder(cbo)
    val cluster = mock.newCluster()

    val groupA = cluster.newGroup()
    val groupB = cluster.newGroup()
    val groupC = cluster.newGroup()
    val groupD = cluster.newGroup()

    val node1 = Unary("node1", groupB.self).asCanonical(cbo)
    val node2 = Unary("node2", groupC.self).asCanonical(cbo)
    val node3 = Unary("node3", groupC.self).asCanonical(cbo)
    val node4 = Unary("node4", groupD.self).asCanonical(cbo)
    val node5 = Unary("node5", groupB.self).asCanonical(cbo)
    val node6 = Leaf("node6", 1).asCanonical(cbo)

    groupA.add(node1)
    groupA.add(node2)
    groupB.add(node3)
    groupB.add(node4)
    groupC.add(node5)
    groupD.add(node6)

    cluster.addNodes(List(node1, node2, node3, node4, node5, node6))

    val mockState = mock.build()

    val bestFinder = newBestFinder(cbo, mockState)
    val best = bestFinder.bestOf(groupA.id)

    assert(best.costs()(InGroupNode(groupA.id, node1)).contains(LongCost(3)))
    assert(best.costs()(InGroupNode(groupA.id, node2)).contains(LongCost(4)))
    assert(best.costs()(InGroupNode(groupB.id, node3)).isEmpty)
    assert(best.costs()(InGroupNode(groupB.id, node4)).contains(LongCost(2)))
    assert(best.costs()(InGroupNode(groupC.id, node5)).contains(LongCost(3)))
    assert(best.costs()(InGroupNode(groupD.id, node6)).contains(LongCost(1)))
  }
}

object CyclicSearchSpaceSuite extends CboSuiteBase {
  case class Leaf(name: String, override val selfCost: Long) extends LeafLike {
    override def makeCopy(): LeafLike = this
  }
  case class Unary(name: String, child: TestNode) extends UnaryLike {
    override def selfCost(): Long = 1
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }
  case class Binary(name: String, left: TestNode, right: TestNode) extends BinaryLike {
    override def selfCost(): Long = 1
    override def withNewChildren(left: TestNode, right: TestNode): BinaryLike =
      copy(left = left, right = right)
  }
}
