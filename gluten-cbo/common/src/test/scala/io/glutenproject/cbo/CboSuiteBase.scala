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
package io.glutenproject.cbo

import io.glutenproject.cbo.memo.{MemoLike, MemoState}
import io.glutenproject.cbo.path.{CboPath, PathFinder}
import io.glutenproject.cbo.property.PropertySet

trait CboSuiteBase {
  trait TestNode {
    def selfCost(): Long
    def children(): Seq[TestNode]
    def withNewChildren(children: Seq[TestNode]): TestNode
  }

  trait UnaryLike extends TestNode {
    def child: TestNode
    def withNewChildren(child: TestNode): UnaryLike
    def children(): Seq[TestNode] = List(child)
    def withNewChildren(children: Seq[TestNode]): TestNode = withNewChildren(children.head)
  }

  trait BinaryLike extends TestNode {
    def left: TestNode
    def right: TestNode
    def children(): Seq[TestNode] = List(left, right)
    def withNewChildren(left: TestNode, right: TestNode): BinaryLike
    def withNewChildren(children: Seq[TestNode]): TestNode =
      withNewChildren(children.head, children(1))
  }

  trait LeafLike extends TestNode {
    def makeCopy(): LeafLike
    def children(): Seq[TestNode] = List.empty
    def withNewChildren(children: Seq[TestNode]): TestNode = this
  }

  case class Group(id: Int, propSet: PropertySet[TestNode]) extends LeafLike {
    override def selfCost(): Long = Long.MaxValue
    override def makeCopy(): LeafLike = copy()
  }

  object Group {
    def apply(id: Int): Group = {
      Group(id, PropertySet(List.empty))
    }
  }

  case class LongCost(value: Long) extends Cost

  object CostModelImpl extends CostModel[TestNode] {

    override def costComparator(): Ordering[Cost] = {
      Ordering.Long.on { case LongCost(value) => value }
    }

    private def longCostOf(node: TestNode): Long = node match {
      case n: TestNode =>
        val selfCost = n.selfCost()

        // Sum with ceil to avoid overflow.
        def safeSum(a: Long, b: Long): Long = {
          val sum = a + b
          if (sum < a || sum < b) Long.MaxValue else sum
        }

        (n.children().map(longCostOf).toSeq :+ selfCost).reduce(safeSum)
      case _ => throw new IllegalStateException()
    }

    override def costOf(node: TestNode): Cost = node match {
      case g: Group => throw new IllegalStateException()
      case n => LongCost(longCostOf(n))
    }

    override def makeInfCost(): Cost = LongCost(Long.MaxValue)
  }

  object PlanModelImpl extends PlanModel[TestNode] {
    override def childrenOf(node: TestNode): Seq[TestNode] = node match {
      case n: TestNode => n.children()
      case _ => throw new IllegalStateException()
    }

    override def withNewChildren(node: TestNode, children: Seq[TestNode]): TestNode =
      node match {
        case n: TestNode => n.withNewChildren(children)
        case _ => throw new IllegalStateException()
      }

    override def hashCode(node: TestNode): Int = {
      java.util.Objects.hashCode(node)
    }

    override def equals(one: TestNode, other: TestNode): Boolean = {
      java.util.Objects.equals(one, other)
    }

    override def newGroupLeaf(groupId: Int, propSet: PropertySet[TestNode]): TestNode =
      Group(groupId, propSet)

    override def getGroupId(node: TestNode): Int = node match {
      case ngl: Group => ngl.id
      case _ => throw new IllegalStateException()
    }

    override def isGroupLeaf(node: TestNode): Boolean = node match {
      case _: Group => true
      case _ => false
    }
  }

  object ExplainImpl extends CboExplain[TestNode] {
    override def describeNode(node: TestNode): String = node match {
      case n => n.toString
    }
  }

  object PropertyModelImpl extends PropertyModel[TestNode] {
    override def propertyDefs: Seq[PropertyDef[TestNode, _ <: Property[TestNode]]] = List.empty
    override def newEnforcerRuleFactory(propertyDef: PropertyDef[TestNode, _ <: Property[TestNode]])
        : EnforcerRuleFactory[TestNode] = (_: Property[TestNode]) => List.empty
  }

  implicit class GraphvizPrinter[T <: AnyRef](val planner: CboPlanner[T]) {
    def printGraphviz(): Unit = {
      // scalastyle:off println
      println(planner.newState().formatGraphviz())
      // scalastyle:on println
    }
  }

  implicit class MemoLikeImplicits[T <: AnyRef](val memo: MemoLike[T]) {
    def memorize(cbo: Cbo[T], node: T): CboGroup[T] = {
      memo.memorize(node, cbo.propSetsOf(node))
    }
  }

  implicit class MemoStateImplicits[T <: AnyRef](val state: MemoState[T]) {
    def getGroupCount(): Int = {
      state.allGroups().size
    }

    def collectAllPaths(depth: Int): Iterable[CboPath[T]] = {
      val allClusters = state.allClusters()
      val allGroups = state.allGroups()

      val highestFinder = PathFinder
        .builder(state.cbo(), state)
        .depth(depth)
        .build()

      allClusters
        .flatMap(c => c.nodes())
        .flatMap(
          node => {
            val highest = highestFinder.find(node).maxBy(c => c.height())
            val finder = (1 to highest.height())
              .foldLeft(PathFinder
                .builder(state.cbo(), state)) {
                case (builder, d) =>
                  builder.depth(d)
              }
              .build()
            finder.find(node)
          })
    }
  }

  implicit class TestNodeImplicits(val node: TestNode) {
    def asCanonical(cbo: Cbo[TestNode]): CanonicalNode[TestNode] = {
      CanonicalNode(cbo, node)
    }
  }

  implicit class TestNodeGroupImplicits(val group: CboGroup[TestNode]) {
    def asGroup(cbo: Cbo[TestNode]): GroupNode[TestNode] = {
      GroupNode(cbo, group)
    }
  }
}
