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
package io.glutenproject.cbo.path

import io.glutenproject.cbo.{CanonicalNode, Cbo, CboSuiteBase}
import io.glutenproject.cbo.mock.MockMemoState
import io.glutenproject.cbo.rule.CboRule

import org.scalatest.funsuite.AnyFunSuite

class PathFinderSuite extends AnyFunSuite {
  import PathFinderSuite._

  test("Base") {
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
    val n1 = "n1"
    val n2 = "n2"
    val n3 = "n3"
    val n4 = "n4"
    val n5 = "n5"
    val n6 = "n6"
    val node1 = Binary(n1, groupB.self, groupC.self).asCanonical(cbo)
    val node2 = Unary(n2, groupD.self).asCanonical(cbo)
    val node3 = Unary(n3, groupE.self).asCanonical(cbo)
    val node4 = Leaf(n4, 1).asCanonical(cbo)
    val node5 = Leaf(n5, 1).asCanonical(cbo)
    val node6 = Leaf(n6, 1).asCanonical(cbo)

    groupA.add(node1)
    groupB.add(node2)
    groupC.add(node3)
    groupD.add(node4)
    groupE.add(List(node5, node6))

    val state = mock.build()

    def find(can: CanonicalNode[TestNode], depth: Int): Iterable[CboPath[TestNode]] = {
      val finder = PathFinder.builder(cbo, state).depth(depth).build()
      finder.find(can)
    }

    val height1 = find(node1, 1).map(_.plan()).toSeq
    val height2 = find(node1, 2).map(_.plan()).toSeq
    val heightInf = find(node1, CboPath.INF_DEPTH).map(_.plan()).toSeq

    assert(height1 == List(Binary(n1, Group(1), Group(2))))
    assert(height2 == List(Binary(n1, Unary(n2, Group(3)), Unary(n3, Group(4)))))
    assert(
      heightInf == List(
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))))
  }

  test("Find - multiple depths") {
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

    val n1 = "n1"
    val n2 = "n2"
    val n3 = "n3"
    val n4 = "n4"
    val n5 = "n5"
    val n6 = "n6"
    val node1 = Binary(n1, groupB.self, groupC.self).asCanonical(cbo)
    val node2 = Unary(n2, groupD.self).asCanonical(cbo)
    val node3 = Unary(n3, groupE.self).asCanonical(cbo)
    val node4 = Leaf(n4, 1).asCanonical(cbo)
    val node5 = Leaf(n5, 1).asCanonical(cbo)
    val node6 = Leaf(n6, 1).asCanonical(cbo)

    groupA.add(node1)
    groupB.add(node2)
    groupC.add(node3)
    groupD.add(node4)
    groupE.add(List(node5, node6))

    val state = mock.build()

    val finder1 = PathFinder
      .builder(cbo, state)
      .depth(1)
      .depth(3)
      .build()

    assert(
      finder1.find(node1).map(_.plan()).toSeq == List(
        Binary(n1, Group(1), Group(2)),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))))

    val finder2 = PathFinder
      .builder(cbo, state)
      .depth(2)
      .depth(CboPath.INF_DEPTH)
      .build()

    assert(
      finder2.find(node1).map(_.plan()).toSeq == List(
        Binary(n1, Unary(n2, Group(3)), Unary(n3, Group(4))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))
      ))

    val finder3 = PathFinder
      .builder(cbo, state)
      .depth(2)
      .depth(2)
      .depth(CboPath.INF_DEPTH)
      .depth(CboPath.INF_DEPTH)
      .build()

    assert(
      finder3.find(node1).map(_.plan()).toSeq == List(
        Binary(n1, Unary(n2, Group(3)), Unary(n3, Group(4))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))
      ))
  }

  test("Dive - basic") {
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

    val n1 = "n1"
    val n2 = "n2"
    val n3 = "n3"
    val n4 = "n4"
    val n5 = "n5"
    val n6 = "n6"
    val node1 = Binary(n1, groupB.self, groupC.self).asCanonical(cbo)
    val node2 = Unary(n2, groupD.self).asCanonical(cbo)
    val node3 = Unary(n3, groupE.self).asCanonical(cbo)
    val node4 = Leaf(n4, 1).asCanonical(cbo)
    val node5 = Leaf(n5, 1).asCanonical(cbo)
    val node6 = Leaf(n6, 1).asCanonical(cbo)

    groupA.add(node1)
    groupB.add(node2)
    groupC.add(node3)
    groupD.add(node4)
    groupE.add(List(node5, node6))

    val state = mock.build()

    val path = CboPath.one(cbo, PathKeySet.trivial, state.allGroups, node1)

    assert(path.plan() == Binary(n1, Group(1), Group(2)))
    assert(
      path.dive(state, 1).map(_.plan()) == List(
        Binary(n1, Unary(n2, Group(3)), Unary(n3, Group(4)))))
    assert(
      path.dive(state, 2).map(_.plan()) == List(
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))))
    assert(
      path.dive(state, 3).map(_.plan()) == List(
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))))
    assert(
      path.dive(state, CboPath.INF_DEPTH).map(_.plan()) == List(
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))))
  }

  test("Find/Dive - binary with different children heights") {
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

    val n1 = "n1"
    val n2 = "n2"
    val n3 = "n3"
    val n4 = "n4"
    val n5 = "n5"
    val node1 = Binary(n1, groupB.self, groupC.self).asCanonical(cbo)
    val node2 = Binary(n2, groupD.self, groupE.self).asCanonical(cbo)
    val node3 = Leaf(n3, 1).asCanonical(cbo)
    val node4 = Leaf(n4, 1).asCanonical(cbo)
    val node5 = Leaf(n5, 1).asCanonical(cbo)

    groupA.add(node1)
    groupB.add(node2)
    groupC.add(node3)
    groupD.add(node4)
    groupE.add(node5)

    val state = mock.build()

    def find(can: CanonicalNode[TestNode], depth: Int): Iterable[CboPath[TestNode]] = {
      PathFinder.builder(cbo, state).depth(depth).build().find(can)
    }

    val height1 = find(node1, 1).map(_.plan()).toSeq
    val height2 = find(node1, 2).map(_.plan()).toSeq
    val height3 = find(node1, 3).map(_.plan()).toSeq
    val height4 = find(node1, 4).map(_.plan()).toSeq
    val heightInf = find(node1, CboPath.INF_DEPTH).map(_.plan()).toSeq

    assert(height1 == List(Binary(n1, Group(1), Group(2))))
    assert(height2 == List(Binary(n1, Binary(n2, Group(3), Group(4)), Leaf(n3, 1))))
    assert(height3 == List(Binary(n1, Binary(n2, Leaf(n4, 1), Leaf(n5, 1)), Leaf(n3, 1))))
    assert(height4 == List(Binary(n1, Binary(n2, Leaf(n4, 1), Leaf(n5, 1)), Leaf(n3, 1))))
    assert(heightInf == List(Binary(n1, Binary(n2, Leaf(n4, 1), Leaf(n5, 1)), Leaf(n3, 1))))

    val path = CboPath.one(cbo, PathKeySet.trivial, state.allGroups, node1)

    assert(path.plan() == Binary(n1, Group(1), Group(2)))
    assert(
      path.dive(state, 1).map(_.plan()).toSeq == List(
        Binary(n1, Binary(n2, Group(3), Group(4)), Leaf(n3, 1))))
    assert(
      path.dive(state, 2).map(_.plan()) == List(
        Binary(n1, Binary(n2, Leaf(n4, 1), Leaf(n5, 1)), Leaf(n3, 1))))
    assert(
      path.dive(state, 3).map(_.plan()) == List(
        Binary(n1, Binary(n2, Leaf(n4, 1), Leaf(n5, 1)), Leaf(n3, 1))))
    assert(
      path.dive(state, CboPath.INF_DEPTH).map(_.plan()) == List(
        Binary(n1, Binary(n2, Leaf(n4, 1), Leaf(n5, 1)), Leaf(n3, 1))))
  }
}

object PathFinderSuite extends CboSuiteBase {
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
