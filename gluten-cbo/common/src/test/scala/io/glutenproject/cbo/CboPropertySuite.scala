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

import io.glutenproject.cbo.Best.BestNotFoundException
import io.glutenproject.cbo.CboConfig.PlannerType
import io.glutenproject.cbo.CboSuiteBase._
import io.glutenproject.cbo.property.PropertySet
import io.glutenproject.cbo.rule.{CboRule, Shape, Shapes}

import org.scalatest.funsuite.AnyFunSuite

class ExhaustivePlannerPropertySuite extends CboPropertySuite {
  override protected def conf: CboConfig = CboConfig(plannerType = PlannerType.Exhaustive)
}

class DpPlannerPropertySuite extends CboPropertySuite {
  override protected def conf: CboConfig = CboConfig(plannerType = PlannerType.Dp)
}

abstract class CboPropertySuite extends AnyFunSuite {
  import CboPropertySuite._

  protected def conf: CboConfig

  test(s"Get property") {
    val leaf = PLeaf(10, DummyProperty(0))
    val unary = PUnary(5, DummyProperty(0), leaf)
    val binary = PBinary(5, DummyProperty(0), leaf, unary)

    val model = DummyPropertyModel
    val propDefs = model.propertyDefs

    assert(propDefs.size === 1)
    assert(propDefs.head.getProperty(leaf) === DummyProperty(0))
    assert(propDefs.head.getProperty(unary) === DummyProperty(0))
    assert(propDefs.head.getProperty(binary) === DummyProperty(0))
    assert(propDefs.head.getChildrenConstraints(DummyProperty(0), leaf) === Seq.empty)
    assert(propDefs.head.getChildrenConstraints(DummyProperty(0), unary) === Seq(DummyProperty(0)))
    assert(propDefs.head
      .getChildrenConstraints(DummyProperty(0), binary) === Seq(DummyProperty(0), DummyProperty(0)))
  }

  test(s"Cannot enforce property") {
    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModelWithOutEnforcerRules,
        ExplainImpl,
        CboRule.Factory.none())
        .withNewConfig(_ => conf)
    val plan = TypedUnary(TypeA, 10, TypedLeaf(TypeA, 10))
    val planner = cbo.newPlanner(plan, PropertySet(Seq(TypeB)))
    assertThrows[BestNotFoundException] {
      planner.plan()
    }
  }

  test(s"Property enforcement - A to B") {
    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModel,
        ExplainImpl,
        CboRule.Factory.none())
        .withNewConfig(_ => conf)

    val plan =
      TypedBinary(TypeA, 5, TypedUnary(TypeA, 10, TypedLeaf(TypeA, 10)), TypedLeaf(TypeA, 10))
    val planner = cbo.newPlanner(plan, PropertySet(Seq(TypeB)))
    val out = planner.plan()
    assert(out == TypeEnforcer(TypeB, 1, plan))
  }

  test(s"Property convert - (A, B)") {
    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModel,
        ExplainImpl,
        CboRule.Factory.reuse(List(ReplaceByTypeARule, ReplaceByTypeBRule)))
        .withNewConfig(_ => conf)
    val plan =
      TypedBinary(TypeA, 5, TypedUnary(TypeA, 10, TypedLeaf(TypeA, 10)), TypedLeaf(TypeA, 10))
    val planner = cbo.newPlanner(plan, PropertySet(Seq(TypeB)))
    val out = planner.plan()
    assert(
      out == TypedBinary(
        TypeB,
        5,
        TypedUnary(TypeB, 10, TypedLeaf(TypeB, 10)),
        TypedLeaf(TypeB, 10)))
  }

  ignore(s"Memo cache hit - (A, B)") {
    object ReplaceLeafAByLeafBRule extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = {
        node match {
          case TypedLeaf(TypeA, cost) => List(TypedLeaf(TypeB, cost - 1))
          case other => List.empty
        }
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    object HitCacheOp extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = {
        node match {
          case PassNodeType(10, TypedLeaf(TypeA, 10)) =>
            List(TypedUnary(TypeB, 15, PassNodeType(10, TypedLeaf(TypeB, 9))))
          case other => List.empty
        }
      }
      override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
    }

    object FinalOp extends CboRule[TestNode] {

      override def shift(node: TestNode): Iterable[TestNode] = {
        node match {
          case TypedUnary(TypeB, 15, PassNodeType(10, TypedLeaf(TypeB, 9))) =>
            List(TypedLeaf(TypeA, 1))
          case other => List.empty
        }
      }
      override def shape(): Shape[TestNode] = Shapes.fixedHeight(3)
    }

    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModelWithOutEnforcerRules,
        ExplainImpl,
        CboRule.Factory.reuse(List(ReplaceLeafAByLeafBRule, HitCacheOp, FinalOp))
      )
        .withNewConfig(_ => conf)

    val plan = PassNodeType(10, TypedLeaf(TypeA, 10))
    val planner = cbo.newPlanner(plan, PropertySet(Seq(TypeA)))
    val out = planner.plan()
    assert(out == TypedLeaf(TypeA, 1))

    // FIXME: Cluster 2 and 1 are currently able to merge but it's better to
    //  have them identified as the same right after HitCacheOp is applied
    val clusterCount = planner.newState().memoState().allClusters().size
    assert(clusterCount == 2)
  }

  test(s"Property propagation - (A, B)") {
    // The propagation is expected to be done by built-in enforcer rule.
    object ReplaceLeafAByLeafBRule extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = {
        node match {
          case TypedLeaf(TypeA, cost) => List(TypedLeaf(TypeB, cost - 1))
          case other => List.empty
        }
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    object ReplaceUnaryBByUnaryARule extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = {
        node match {
          case TypedUnary(TypeB, cost, child) => List(TypedUnary(TypeA, cost - 2, child))
          case other => List.empty
        }
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModelWithOutEnforcerRules,
        ExplainImpl,
        CboRule.Factory.reuse(List(ReplaceLeafAByLeafBRule, ReplaceUnaryBByUnaryARule))
      )
        .withNewConfig(_ => conf)
    val sub =
      PassNodeType(5, TypedLeaf(TypeA, 10))
    val plan = TypedUnary(TypeB, 10, sub)
    val planner = cbo.newPlanner(plan, PropertySet(Seq(TypeAny)))
    val out = planner.plan()

    assert(out == TypedUnary(TypeA, 8, PassNodeType(5, TypedLeaf(TypeA, 10))))
  }

  test(s"Property convert - (A, B), alternative conventions") {
    object ConvertEnforcerAndTypeAToTypeB extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case TypeEnforcer(TypeB, _, TypedBinary(TypeA, 5, left, right)) =>
          List(TypedBinary(TypeB, 0, left, right))
        case _ => List.empty
      }
      override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
    }

    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModel,
        ExplainImpl,
        CboRule.Factory.reuse(List(ConvertEnforcerAndTypeAToTypeB)))
        .withNewConfig(_ => conf)
    val plan =
      TypedBinary(TypeA, 5, TypedUnary(TypeA, 10, TypedLeaf(TypeA, 10)), TypedLeaf(TypeA, 10))
    val planner = cbo.newPlanner(
      plan,
      PropertySet(Seq(TypeAny)),
      List(PropertySet(Seq(TypeB)), PropertySet(Seq(TypeC))))
    val out = planner.plan()
    assert(
      out == TypedBinary(
        TypeB,
        0,
        TypeEnforcer(TypeB, 1, TypedUnary(TypeA, 10, TypedLeaf(TypeA, 10))),
        TypeEnforcer(TypeB, 1, TypedLeaf(TypeA, 10))))
    assert(planner.newState().memoState().allGroups().size == 9)
  }

  test(s"Property convert - (A, B), Unary only has TypeA") {
    object ReplaceNonUnaryByTypeBRule extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = {
        node match {
          case TypedLeaf(_, cost) => List(TypedLeaf(TypeB, cost))
          case TypedBinary(_, cost, left, right) => List(TypedBinary(TypeB, cost, left, right))
          case other => List.empty
        }
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModel,
        ExplainImpl,
        CboRule.Factory.reuse(List(ReplaceByTypeARule, ReplaceNonUnaryByTypeBRule))
      )
        .withNewConfig(_ => conf)
    val plan =
      TypedBinary(TypeA, 5, TypedUnary(TypeA, 10, TypedLeaf(TypeA, 10)), TypedLeaf(TypeA, 10))
    val planner = cbo.newPlanner(plan, PropertySet(Seq(TypeB)))
    val out = planner.plan()
    assert(
      out == TypeEnforcer(
        TypeB,
        1,
        TypedBinary(
          TypeA,
          5,
          TypedUnary(TypeA, 10, TypedLeaf(TypeA, 10)),
          TypedLeaf(TypeA, 10))) || out == TypedBinary(
        TypeB,
        5,
        TypeEnforcer(TypeB, 1, TypedUnary(TypeA, 10, TypedLeaf(TypeA, 10))),
        TypedLeaf(TypeB, 10)))
  }

  test(s"Property convert - (A, B, C), TypeC has lowest cost") {
    object ReduceTypeBCost extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = {
        node match {
          case TypedLeaf(TypeB, _) => List(TypedLeaf(TypeB, 5))
          case TypedUnary(TypeB, _, child) => List(TypedUnary(TypeB, 5, child))
          case TypedBinary(TypeB, _, left, right) => List(TypedBinary(TypeB, 5, left, right))
          case other => List.empty
        }
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    object ConvertUnaryTypeBToTypeC extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case TypedUnary(TypeB, _, child) => List(TypedUnary(TypeC, 0, child))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModel,
        ExplainImpl,
        CboRule.Factory.reuse(List(ReduceTypeBCost, ConvertUnaryTypeBToTypeC)))
        .withNewConfig(_ => conf)

    val plan =
      TypedUnary(TypeB, 10, TypedLeaf(TypeA, 20))
    val planner = cbo.newPlanner(plan, PropertySet(Seq(TypeB)))
    val out = planner.plan()
    assert(
      out == TypeEnforcer(
        TypeB,
        1,
        TypedUnary(TypeC, 0, TypeEnforcer(TypeC, 1, TypedLeaf(TypeA, 20)))))
  }

  test(
    s"Property convert - (A, B, C), TypeC has lowest cost, binary root," +
      s" right enforcer added after left is explored, disordered group creation") {

    object RightOp extends CboRule[TestNode] {
      // Let the right child tree add group A to the leaf.
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case TypedUnary(TypeA, 15, TypedLeaf(TypeA, 20)) =>
          // This creates enforcer at leaf's cluster.
          List(TypedUnary(TypeB, 15, TypedLeaf(TypeA, 20)))
        case TypeEnforcer(TypeB, 1, TypedLeaf(TypeA, 20)) =>
          // Cost is high, so won't be chosen by right tree.
          List(TypedLeaf(TypeA, 100))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
    }

    object LeftOp extends CboRule[TestNode] {
      // The the left child tree should be aware of the enforcer and continue exploration.
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case TypedUnary(TypeA, 10, TypedLeaf(TypeA, 100)) =>
          // The leaf was created by right OP.
          List(TypedLeaf(TypeC, 0))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
    }

    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModel,
        ExplainImpl,
        CboRule.Factory.reuse(List(LeftOp, RightOp))
      )
        .withNewConfig(_ => conf)

    val left = TypedUnary(TypeA, 10, TypedLeaf(TypeA, 20))
    val right = TypeEnforcer(TypeA, 1, TypedUnary(TypeA, 15, TypedLeaf(TypeA, 20)))
    val plan = TypedBinary(TypeA, 10, left, right)
    val planner = cbo.newPlanner(plan, PropertySet(Seq(TypeA)))
    val out = planner.plan()
    assert(
      out == TypedBinary(
        TypeA,
        10,
        TypeEnforcer(TypeA, 1, TypedLeaf(TypeC, 0)),
        TypeEnforcer(TypeA, 1, TypedUnary(TypeA, 15, TypedLeaf(TypeA, 20)))))
  }

  test(
    s"Property convert - (A, B, C), TypeC has lowest cost, binary root," +
      s" right enforcer added after left is explored, merge") {
    object ConvertTypeBEnforcerAndLeafToTypeC extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case TypeEnforcer(TypeB, _, _ @TypedLeaf(_, _)) =>
          List(TypedLeaf(TypeC, 0))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
    }

    object ConvertTypeATypeCToTypeC extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case TypedUnary(TypeA, _, TypeEnforcer(TypeA, _, _ @TypedLeaf(TypeC, _))) =>
          List(TypedLeaf(TypeC, 0))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(3)
    }

    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModel,
        ExplainImpl,
        CboRule.Factory.reuse(List(ConvertTypeBEnforcerAndLeafToTypeC, ConvertTypeATypeCToTypeC))
      )
        .withNewConfig(_ => conf)

    val left =
      TypedUnary(TypeA, 10, TypedUnary(TypeA, 10, TypedLeaf(TypeA, 20)))
    val right =
      TypedUnary(TypeB, 15, TypedUnary(TypeB, 15, TypedLeaf(TypeA, 20)))
    val plan = TypedBinary(TypeB, 10, left, right)
    val planner = cbo.newPlanner(plan, PropertySet(Seq(TypeB)))
    val out = planner.plan()
    assert(
      out == TypedBinary(
        TypeB,
        10,
        TypeEnforcer(TypeB, 1, TypedLeaf(TypeC, 0)),
        TypedUnary(TypeB, 15, TypedUnary(TypeB, 15, TypeEnforcer(TypeB, 1, TypedLeaf(TypeC, 0))))))
  }

  test(s"Property convert - (A, B), Unary only has TypeA, TypeB has lowest cost") {
    // TODO: Apply enforce rules on low-cost nodes to propagate it to other groups.
    object ReduceTypeBCost extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = {
        node match {
          case TypedLeaf(TypeB, _) => List(TypedLeaf(TypeB, 0))
          case TypedUnary(TypeB, _, child) => List(TypedUnary(TypeB, 0, child))
          case TypedBinary(TypeB, _, left, right) => List(TypedBinary(TypeB, 0, left, right))
          case other => List.empty
        }
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    object ReplaceNonUnaryByTypeBRule extends CboRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = {
        node match {
          case TypedLeaf(_, cost) => List(TypedLeaf(TypeB, cost))
          case TypedBinary(_, cost, left, right) => List(TypedBinary(TypeB, cost, left, right))
          case other => List.empty
        }
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val cbo =
      Cbo[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        NodeTypePropertyModel,
        ExplainImpl,
        CboRule.Factory.reuse(List(ReplaceNonUnaryByTypeBRule, ReduceTypeBCost))
      )
        .withNewConfig(_ => conf)
    val plan =
      TypedBinary(TypeA, 5, TypedUnary(TypeA, 10, TypedLeaf(TypeA, 10)), TypedLeaf(TypeA, 10))
    val planner = cbo.newPlanner(plan, PropertySet(Seq(TypeB)))
    val out = planner.plan()
    assert(
      out == TypedBinary(
        TypeB,
        0,
        TypeEnforcer(TypeB, 1, TypedUnary(TypeA, 10, TypeEnforcer(TypeA, 1, TypedLeaf(TypeB, 0)))),
        TypedLeaf(TypeB, 0)))
  }
}

object CboPropertySuite {

  case class NoopEnforcerRule[T <: AnyRef]() extends CboRule[T] {
    override def shift(node: T): Iterable[T] = List.empty
    override def shape(): Shape[T] = Shapes.none()
  }

  // Dummy property model

  case class DummyProperty(id: Int) extends Property[TestNode] {
    override def satisfies(other: Property[TestNode]): Boolean = {
      other match {
        case DummyProperty(otherId) =>
          // Higher ID satisfies lower IDs.
          id >= otherId
        case _ => throw new IllegalStateException()
      }
    }

    override def definition(): PropertyDef[TestNode, DummyProperty] = {
      DummyPropertyDef
    }
  }

  case class PUnary(override val selfCost: Long, prop: DummyProperty, override val child: TestNode)
    extends UnaryLike {
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class PLeaf(override val selfCost: Long, prop: DummyProperty) extends LeafLike {
    override def makeCopy(): LeafLike = copy()
  }

  case class PBinary(
      override val selfCost: Long,
      prop: DummyProperty,
      override val left: TestNode,
      override val right: TestNode)
    extends BinaryLike {
    override def withNewChildren(left: TestNode, right: TestNode): BinaryLike =
      copy(left = left, right = right)
  }

  object DummyPropertyDef extends PropertyDef[TestNode, DummyProperty] {
    override def getProperty(plan: TestNode): DummyProperty = {
      plan match {
        case Group(_, _, _) => throw new IllegalStateException()
        case PUnary(_, prop, _) => prop
        case PLeaf(_, prop) => prop
        case PBinary(_, prop, _, _) => prop
        case _ => DummyProperty(-1)
      }
    }

    override def getChildrenConstraints(
        constraint: Property[TestNode],
        plan: TestNode): Seq[DummyProperty] = {
      plan match {
        case PUnary(_, _, _) => Seq(DummyProperty(0))
        case PLeaf(_, _) => Seq.empty
        case PBinary(_, _, _, _) => Seq(DummyProperty(0), DummyProperty(0))
        case _ => throw new IllegalStateException()
      }
    }
  }

  object DummyPropertyModel extends PropertyModel[TestNode] {
    override def propertyDefs: Seq[PropertyDef[TestNode, _ <: Property[TestNode]]] = Seq(
      DummyPropertyDef)

    override def newEnforcerRuleFactory(propertyDef: PropertyDef[TestNode, _ <: Property[TestNode]])
        : EnforcerRuleFactory[TestNode] = (constraint: Property[TestNode]) => List.empty
  }

  // Node type property model

  trait TypedNode extends TestNode {
    def nodeType: NodeType
  }

  case class TypedLeaf(override val nodeType: NodeType, override val selfCost: Long)
    extends LeafLike
    with TypedNode {
    override def makeCopy(): LeafLike = copy()
  }

  case class TypedUnary(
      override val nodeType: NodeType,
      override val selfCost: Long,
      override val child: TestNode)
    extends UnaryLike
    with TypedNode {
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class TypedBinary(
      override val nodeType: NodeType,
      override val selfCost: Long,
      override val left: TestNode,
      override val right: TestNode)
    extends BinaryLike
    with TypedNode {
    override def withNewChildren(left: TestNode, right: TestNode): BinaryLike =
      copy(left = left, right = right)
  }

  case class TypeEnforcer(
      override val nodeType: NodeType,
      override val selfCost: Long,
      override val child: TestNode)
    extends UnaryLike
    with TypedNode {
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class PassNodeType(override val selfCost: Long, child: TestNode) extends TypedNode {
    override def nodeType: NodeType = child match {
      case n: TypedNode => n.nodeType
      case g: Group => g.propSet.get(NodeTypeDef)
      case _ => throw new IllegalStateException()
    }

    override def children(): Seq[TestNode] = List(child)
    override def withNewChildren(children: Seq[TestNode]): TestNode = copy(selfCost, children.head)
  }

  case class NodeTypeEnforcerRule(reqType: NodeType) extends CboRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = {
      node match {
        case typed: TypedNode if typed.nodeType.satisfies(reqType) => List(typed)
        case typed: TypedNode => List(TypeEnforcer(reqType, 1, typed))
        case _ => throw new IllegalStateException()
      }
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  object ReplaceByTypeARule extends CboRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = {
      node match {
        case TypedLeaf(_, cost) => List(TypedLeaf(TypeA, cost))
        case TypedUnary(_, cost, child) => List(TypedUnary(TypeA, cost, child))
        case TypedBinary(_, cost, left, right) => List(TypedBinary(TypeA, cost, left, right))
        case other => List.empty
      }
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  object ReplaceByTypeBRule extends CboRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = {
      node match {
        case TypedLeaf(_, cost) => List(TypedLeaf(TypeB, cost))
        case TypedUnary(_, cost, child) => List(TypedUnary(TypeB, cost, child))
        case TypedBinary(_, cost, left, right) => List(TypedBinary(TypeB, cost, left, right))
        case other => List.empty
      }
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  object NodeTypeDef extends PropertyDef[TestNode, NodeType] {
    override def getProperty(plan: TestNode): NodeType = plan match {
      case typed: TypedNode => typed.nodeType
      case _ => throw new IllegalStateException()
    }

    override def getChildrenConstraints(
        constraint: Property[TestNode],
        plan: TestNode): Seq[NodeType] = plan match {
      case TypedLeaf(_, _) => Seq.empty
      case TypedUnary(t, _, _) => Seq(t)
      case TypedBinary(t, _, _, _) => Seq(t, t)
      case TypeEnforcer(_, _, _) => Seq(TypeAny)
      case p @ PassNodeType(_, _) => Seq(constraint.asInstanceOf[NodeType])
      case _ => throw new IllegalStateException()
    }

    override def toString: String = "NodeTypeDef"
  }

  trait NodeType extends Property[TestNode] {
    override def definition(): PropertyDef[TestNode, NodeType] = NodeTypeDef
    override def toString: String = getClass.getSimpleName
  }

  object TypeAny extends NodeType {
    override def satisfies(other: Property[TestNode]): Boolean = other match {
      case TypeAny => true
      case _: NodeType => false
      case _ => throw new IllegalStateException()
    }
  }

  object TypeA extends NodeType {
    override def satisfies(other: Property[TestNode]): Boolean = other match {
      case TypeA => true
      case TypeAny => true
      case _: NodeType => false
      case _ => throw new IllegalStateException()
    }
  }

  object TypeB extends NodeType {
    override def satisfies(other: Property[TestNode]): Boolean = other match {
      case TypeB => true
      case TypeAny => true
      case _: NodeType => false
      case _ => throw new IllegalStateException()
    }
  }

  object TypeC extends NodeType {
    override def satisfies(other: Property[TestNode]): Boolean = other match {
      case TypeC => true
      case TypeAny => true
      case _: NodeType => false
      case _ => throw new IllegalStateException()
    }
  }

  object NodeTypePropertyModel extends PropertyModel[TestNode] {
    override def propertyDefs: Seq[PropertyDef[TestNode, _ <: Property[TestNode]]] = Seq(
      NodeTypeDef)

    override def newEnforcerRuleFactory(propertyDef: PropertyDef[TestNode, _ <: Property[TestNode]])
        : EnforcerRuleFactory[TestNode] = {
      (constraint: Property[TestNode]) =>
        {
          List(NodeTypeEnforcerRule(constraint.asInstanceOf[NodeType]))
        }
    }
  }

  object NodeTypePropertyModelWithOutEnforcerRules extends PropertyModel[TestNode] {
    override def propertyDefs: Seq[PropertyDef[TestNode, _ <: Property[TestNode]]] = Seq(
      NodeTypeDef)

    override def newEnforcerRuleFactory(propertyDef: PropertyDef[TestNode, _ <: Property[TestNode]])
        : EnforcerRuleFactory[TestNode] = (_: Property[TestNode]) => List.empty
  }
}
