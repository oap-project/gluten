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

import io.glutenproject.cbo.{Cbo, CboSuiteBase}
import io.glutenproject.cbo.mock.MockMemoState
import io.glutenproject.cbo.rule.CboRule

import org.scalatest.funsuite.AnyFunSuite

class WizardSuite extends AnyFunSuite {
  import WizardSuite._

  test("None") {
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

    val n1 = "n1"
    val node1 = Leaf(n1, 1).asCanonical(cbo)

    groupA.add(node1)

    val state = mock.build()

    val finder = PathFinder.builder(cbo, state).output(OutputWizards.none()).build()
    assert(finder.find(node1).map(_.plan()).toSeq == List.empty)
  }

  test("Prune by maximum depth") {
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

    def findWithMaxDepths(maxDepths: Seq[Int]): Seq[TestNode] = {
      val builder = PathFinder.builder(cbo, state)
      val finder = maxDepths
        .foldLeft(builder) {
          case (builder, d) =>
            builder.depth(d)
        }
        .build()
      finder.find(node1).map(_.plan()).toSeq
    }

    assert(findWithMaxDepths(List(1)) == List(Binary(n1, Group(1), Group(2))))
    assert(findWithMaxDepths(List(2)) == List(Binary(n1, Unary(n2, Group(3)), Unary(n3, Group(4)))))
    assert(
      findWithMaxDepths(List(3)) == List(
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))))
    assert(
      findWithMaxDepths(List(4)) == List(
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))))
    assert(
      findWithMaxDepths(List(CboPath.INF_DEPTH)) == List(
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))))

    assert(
      findWithMaxDepths(List(1, 2)) == List(
        Binary(n1, Group(1), Group(2)),
        Binary(n1, Unary(n2, Group(3)), Unary(n3, Group(4)))))
    assert(
      findWithMaxDepths(List(2, CboPath.INF_DEPTH)) == List(
        Binary(n1, Unary(n2, Group(3)), Unary(n3, Group(4))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))
      ))
  }

  test("Prune by pattern") {
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

    def findWithPatterns(patterns: Seq[Pattern[TestNode]]): Seq[TestNode] = {
      val builder = PathFinder.builder(cbo, state)
      val finder = patterns
        .foldLeft(builder) {
          case (builder, pattern) =>
            builder.output(OutputWizards.withPattern(pattern))
        }
        .build()
      finder.find(node1).map(_.plan()).toSeq
    }

    assert(
      findWithPatterns(List(Pattern.any[TestNode].build())) == List(Binary(n1, Group(1), Group(2))))
    assert(
      findWithPatterns(
        List(
          Pattern
            .node[TestNode](
              _ => true,
              Pattern.node(_ => true, Pattern.ignore),
              Pattern.node(_ => true, Pattern.ignore))
            .build())) == List(Binary(n1, Unary(n2, Group(3)), Unary(n3, Group(4)))))

    // Pattern pruning should emit all results
    val pattern1 = Pattern
      .node[TestNode](_ => true, Pattern.node(_ => true, Pattern.ignore), Pattern.ignore)
      .build()
    val pattern2 = Pattern
      .node[TestNode](_ => true, Pattern.ignore, Pattern.node(_ => true, Pattern.ignore))
      .build()

    assert(
      findWithPatterns(List(pattern1, pattern2)) == List(
        Binary(n1, Group(1), Unary(n3, Group(4))),
        Binary(n1, Unary(n2, Group(3)), Group(2))))

    // Distinguish between ignore and any
    val pattern3 = Pattern
      .node[TestNode](_ => true, Pattern.node(_ => true, Pattern.any), Pattern.ignore)
      .build()
    val pattern4 = Pattern
      .node[TestNode](_ => true, Pattern.ignore, Pattern.node(_ => true, Pattern.any))
      .build()

    assert(
      findWithPatterns(List(pattern3, pattern4)) == List(
        Binary(n1, Group(1), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Group(1), Unary(n3, Leaf(n6, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Group(2))))

  }

  test("Prune by mask") {
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

    def findWithMask(mask: Seq[Int]): Seq[TestNode] = {
      PathFinder
        .builder(cbo, state)
        .output(OutputWizards.withMask(PathMask(mask)))
        .build()
        .find(node1)
        .map(_.plan())
        .toSeq
    }

    assert(findWithMask(List(2, -1, -1)) == List(Binary(n1, Group(1), Group(2))))
    assert(findWithMask(List(2, 1, -1, -1)) == List(Binary(n1, Unary(n2, Group(3)), Group(2))))
    assert(findWithMask(List(2, -1, 1, -1)) == List(Binary(n1, Group(1), Unary(n3, Group(4)))))
    assert(
      findWithMask(List(2, -1, 1, 0)) == List(
        Binary(n1, Group(1), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Group(1), Unary(n3, Leaf(n6, 1)))))
    assert(findWithMask(List(2, 1, 0, -1)) == List(Binary(n1, Unary(n2, Leaf(n4, 1)), Group(2))))
    assert(
      findWithMask(List(2, 1, -1, 1, 0)) == List(
        Binary(n1, Unary(n2, Group(3)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Group(3)), Unary(n3, Leaf(n6, 1)))))
    assert(
      findWithMask(List(2, 1, 0, 1, -1)) ==
        List(Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Group(4)))))
    assert(
      findWithMask(List(2, 1, 0, 1, 0)) == List(
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n5, 1))),
        Binary(n1, Unary(n2, Leaf(n4, 1)), Unary(n3, Leaf(n6, 1)))))

  }
}

object WizardSuite extends CboSuiteBase {
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
