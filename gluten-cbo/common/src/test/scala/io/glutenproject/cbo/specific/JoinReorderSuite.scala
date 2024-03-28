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
package io.glutenproject.cbo.specific

import io.glutenproject.cbo.{Cbo, CboConfig, CboSuiteBase}
import io.glutenproject.cbo.CboConfig.PlannerType
import io.glutenproject.cbo.rule.{CboRule, Shape, Shapes}

import org.scalatest.funsuite.AnyFunSuite

class ExhaustivePlannerJoinReorderSuite extends JoinReorderSuite {
  override protected def conf: CboConfig = CboConfig(plannerType = PlannerType.Exhaustive)
}

class DpPlannerJoinReorderSuite extends JoinReorderSuite {
  override protected def conf: CboConfig = CboConfig(plannerType = PlannerType.Dp)
}

abstract class JoinReorderSuite extends AnyFunSuite {
  import JoinReorderSuite._

  protected def conf: CboConfig

  test("3 way join - dry run") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.none())
        .withNewConfig(_ => conf)
    val plan = LeftJoin(LeftJoin(Scan(50), Scan(200)), Scan(100))
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    assert(out == plan)
  }

  test("3 way join - reorder") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(JoinAssociateRule, JoinCommuteRule)))
        .withNewConfig(_ => conf)
    val plan = LeftJoin(LeftJoin(Scan(200), Scan(100)), Scan(30))
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    assert(out == LeftJoin(LeftJoin(Scan(30), Scan(200)), Scan(100)))
  }

  test("5 way join - reorder") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(JoinAssociateRule, JoinCommuteRule)))
        .withNewConfig(_ => conf)
    val plan =
      LeftJoin(LeftJoin(Scan(2000), Scan(300)), LeftJoin(LeftJoin(Scan(200), Scan(1000)), Scan(50)))
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    assert(
      out == LeftJoin(
        LeftJoin(LeftJoin(LeftJoin(Scan(50), Scan(2000)), Scan(1000)), Scan(300)),
        Scan(200)))
  }

  // too slow
  ignore("7 way join - reorder") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(JoinAssociateRule, JoinCommuteRule)))
        .withNewConfig(_ => conf)
    val plan = LeftJoin(
      LeftJoin(
        LeftJoin(Scan(2000), Scan(300)),
        LeftJoin(LeftJoin(Scan(200), Scan(1000)), Scan(50))),
      LeftJoin(Scan(700), Scan(3000)))
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    throw new UnsupportedOperationException("Not yet implemented")
  }

  // too slow
  ignore("9 way join - reorder") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(JoinAssociateRule, JoinCommuteRule)))
        .withNewConfig(_ => conf)
    val plan = LeftJoin(
      LeftJoin(
        LeftJoin(Scan(2000), Scan(300)),
        LeftJoin(LeftJoin(Scan(200), Scan(1000)), Scan(50))),
      LeftJoin(LeftJoin(Scan(700), Scan(3000)), LeftJoin(Scan(9000), Scan(1000)))
    )
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    throw new UnsupportedOperationException("Not yet implemented")
  }

  // too slow
  ignore("12 way join - reorder") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(List(JoinAssociateRule, JoinCommuteRule)))
        .withNewConfig(_ => conf)
    val plan = LeftJoin(
      LeftJoin(
        LeftJoin(
          LeftJoin(Scan(2000), Scan(300)),
          LeftJoin(LeftJoin(Scan(200), Scan(1000)), Scan(50))),
        LeftJoin(LeftJoin(Scan(700), Scan(3000)), LeftJoin(Scan(9000), Scan(1000)))
      ),
      LeftJoin(LeftJoin(Scan(5000), Scan(1200)), Scan(150))
    )
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    throw new UnsupportedOperationException("Not yet implemented")
  }

  test("2 way join - reorder, left deep only") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(leftDeepJoinRules(2)))
        .withNewConfig(_ => conf)
    val plan = LeftJoin(Scan(200), Scan(30))
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    assert(out == LeftDeepJoin(Scan(30), Scan(200)))
  }

  test("3 way join - reorder, left deep only") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(leftDeepJoinRules(3)))
        .withNewConfig(_ => conf)
    val plan = LeftJoin(LeftJoin(Scan(200), Scan(100)), Scan(30))
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    assert(out == LeftDeepJoin(LeftDeepJoin(Scan(30), Scan(200)), Scan(100)))
  }

  test("5 way join - reorder, left deep only") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(leftDeepJoinRules(5)))
        .withNewConfig(_ => conf)
    val plan =
      LeftJoin(LeftJoin(Scan(2000), Scan(300)), LeftJoin(LeftJoin(Scan(200), Scan(1000)), Scan(50)))
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    assert(
      out == LeftDeepJoin(
        LeftDeepJoin(LeftDeepJoin(LeftDeepJoin(Scan(50), Scan(2000)), Scan(1000)), Scan(300)),
        Scan(200)))
  }

  test("7 way join - reorder, left deep only") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(leftDeepJoinRules(7)))
        .withNewConfig(_ => conf)
    val plan = LeftJoin(
      LeftJoin(
        LeftJoin(Scan(2000), Scan(300)),
        LeftJoin(LeftJoin(Scan(200), Scan(1000)), Scan(50))),
      LeftJoin(Scan(700), Scan(3000)))
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    assert(
      out == LeftDeepJoin(
        LeftDeepJoin(
          LeftDeepJoin(
            LeftDeepJoin(LeftDeepJoin(LeftDeepJoin(Scan(50), Scan(3000)), Scan(2000)), Scan(1000)),
            Scan(700)),
          Scan(300)),
        Scan(200)))
  }

  test("9 way join - reorder, left deep only") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(leftDeepJoinRules(9)))
        .withNewConfig(_ => conf)
    val plan = LeftJoin(
      LeftJoin(
        LeftJoin(Scan(2000), Scan(300)),
        LeftJoin(LeftJoin(Scan(200), Scan(1000)), Scan(50))),
      LeftJoin(LeftJoin(Scan(700), Scan(3000)), LeftJoin(Scan(9000), Scan(1000)))
    )
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    assert(
      out == LeftDeepJoin(
        LeftDeepJoin(
          LeftDeepJoin(
            LeftDeepJoin(
              LeftDeepJoin(
                LeftDeepJoin(
                  LeftDeepJoin(LeftDeepJoin(Scan(50), Scan(9000)), Scan(3000)),
                  Scan(2000)),
                Scan(1000)),
              Scan(1000)),
            Scan(700)),
          Scan(300)
        ),
        Scan(200)
      ))
  }

  // too slow
  ignore("12 way join - reorder, left deep only") {
    val cbo =
      Cbo[TestNode](
        CostModelImpl,
        PlanModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        CboRule.Factory.reuse(leftDeepJoinRules(12)))
        .withNewConfig(_ => conf)
    val plan = LeftJoin(
      LeftJoin(
        LeftJoin(
          LeftJoin(Scan(2000), Scan(300)),
          LeftJoin(LeftJoin(Scan(200), Scan(1000)), Scan(50))),
        LeftJoin(LeftJoin(Scan(700), Scan(3000)), LeftJoin(Scan(9000), Scan(1000)))
      ),
      LeftJoin(LeftJoin(Scan(5000), Scan(1200)), Scan(150))
    )
    val planner = cbo.newPlanner(plan)
    val out = planner.plan()
    throw new UnsupportedOperationException("Not yet implemented")
  }
}

object JoinReorderSuite extends CboSuiteBase {

  object JoinAssociateRule extends CboRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = node match {
      case LeftJoin(LeftJoin(a, b), c) => List(LeftJoin(a, LeftJoin(b, c)))
      case LeftJoin(a, LeftJoin(b, c)) => List(LeftJoin(LeftJoin(a, b), c))
      case other => List.empty
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
  }

  object JoinCommuteRule extends CboRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = node match {
      case LeftJoin(a, b) => List(LeftJoin(b, a))
      case other => List.empty
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  trait KnownCardNode extends TestNode {
    def card(): Long
  }

  abstract class LeftJoinBase(left: TestNode, right: TestNode)
    extends BinaryLike
    with KnownCardNode {
    private def leftCard() = left match {
      case l: KnownCardNode => l.card()
      case _ => -1L
    }

    private def rightCard() = right match {
      case r: KnownCardNode => r.card()
      case _ => -1L
    }

    override def selfCost(): Long = leftCard() * leftCard() * rightCard()

    override def card(): Long = {
      leftCard().min(rightCard()) + 1
    }
  }

  case class LeftJoin(left: TestNode, right: TestNode) extends LeftJoinBase(left, right) {
    override def withNewChildren(left: TestNode, right: TestNode): BinaryLike =
      copy(left = left, right = right)
  }

  case class Scan(card: Long) extends LeafLike with KnownCardNode {
    override def selfCost(): Long = card
    override def makeCopy(): LeafLike = copy()
  }

  // Rules and node types for left deep join

  private def leftDeepJoinRules(expandThreshold: Int): List[CboRule[TestNode]] = {
    List(
      MultiJoinRule,
      MultiJoinMergeRule,
      MultiJoinToLeftDeepJoinRule(expandThreshold),
      LeftDeepJoinCommuteRule2,
      LeftDeepJoinCommuteRule3
    )
  }

  object MultiJoinRule extends CboRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = node match {
      case LeftJoin(left, right) => List(MultiJoin(List(left, right)))
      case _ => List.empty
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  object MultiJoinMergeRule extends CboRule[TestNode] {

    override def shift(node: TestNode): Iterable[TestNode] = {
      val out = shift0(node)
      out
    }

    private def shift0(node: TestNode): Iterable[TestNode] = node match {
      case MultiJoin(children) =>
        val newChildren = children.flatMap {
          case MultiJoin(c) => c
          case n => List(n)
        }
        List(MultiJoin(newChildren))
      case _ => List.empty
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
  }

  case class MultiJoinToLeftDeepJoinRule(expandThreshold: Int) extends CboRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = node match {
      case MultiJoin(children) if children.size >= expandThreshold =>
        List(children.reduce((a, b) => LeftDeepJoin(a, b)))
      case _ => List.empty
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  object LeftDeepJoinCommuteRule2 extends CboRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = node match {
      case LeftDeepJoin(s1 @ Scan(a), s2 @ Scan(b)) => List(LeftDeepJoin(s2, s1))
      case _ => List.empty
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
  }

  object LeftDeepJoinCommuteRule3 extends CboRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = node match {
      case LeftDeepJoin(LeftDeepJoin(a, s1 @ Scan(_)), s2 @ Scan(_)) =>
        List(LeftDeepJoin(LeftDeepJoin(a, s2), s1))
      case _ => List.empty
    }
    override def shape(): Shape[TestNode] = Shapes.fixedHeight(3)
  }

  case class MultiJoin(override val children: Seq[TestNode]) extends TestNode {
    override def withNewChildren(newChildren: Seq[TestNode]): MultiJoin = copy(newChildren)
    override def selfCost(): Long = Long.MaxValue
  }

  case class LeftDeepJoin(left: TestNode, right: TestNode) extends LeftJoinBase(left, right) {
    override def withNewChildren(left: TestNode, right: TestNode): BinaryLike =
      copy(left = left, right = right)
  }
}
