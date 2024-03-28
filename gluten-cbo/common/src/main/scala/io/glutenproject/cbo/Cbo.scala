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

import io.glutenproject.cbo.property.PropertySet
import io.glutenproject.cbo.rule.CboRule

import scala.collection.mutable

/**
 * Entrypoint of ACBO (Advanced CBO)'s search engine. See basic introduction of ACBO:
 * https://github.com/apache/incubator-gluten/issues/5057.
 */
trait Optimization[T <: AnyRef] {
  def newPlanner(
      plan: T,
      constraintSet: PropertySet[T],
      altConstraintSets: Seq[PropertySet[T]]): CboPlanner[T]

  def propSetsOf(plan: T): PropertySet[T]

  def withNewConfig(confFunc: CboConfig => CboConfig): Optimization[T]
}

object Optimization {
  def apply[T <: AnyRef](
      costModel: CostModel[T],
      planModel: PlanModel[T],
      propertyModel: PropertyModel[T],
      explain: CboExplain[T],
      ruleFactory: CboRule.Factory[T]): Optimization[T] = {
    Cbo(costModel, planModel, propertyModel, explain, ruleFactory)
  }

  implicit class OptimizationImplicits[T <: AnyRef](opt: Optimization[T]) {
    def newPlanner(plan: T): CboPlanner[T] = {
      opt.newPlanner(plan, opt.propSetsOf(plan), List.empty)
    }
    def newPlanner(plan: T, constraintSet: PropertySet[T]): CboPlanner[T] = {
      opt.newPlanner(plan, constraintSet, List.empty)
    }
  }
}

class Cbo[T <: AnyRef] private (
    val config: CboConfig,
    val costModel: CostModel[T],
    val planModel: PlanModel[T],
    val propertyModel: PropertyModel[T],
    val explain: CboExplain[T],
    val ruleFactory: CboRule.Factory[T])
  extends Optimization[T] {
  import Cbo._

  override def withNewConfig(confFunc: CboConfig => CboConfig): Cbo[T] = {
    new Cbo(confFunc(config), costModel, planModel, propertyModel, explain, ruleFactory)
  }

  // Normal groups start with ID 0, so it's safe to use -1 to do validation.
  private val dummyGroup: T =
    planModel.newGroupLeaf(-1, PropertySet(Seq.empty))
  private val infCost: Cost = costModel.makeInfCost()

  validateModels()

  private def assertThrows(message: String)(u: => Unit): Unit = {
    var notThrew: Boolean = false
    try {
      u
      notThrew = true
    } catch {
      case _: Exception =>
    }
    assert(!notThrew, message)
  }

  private def validateModels(): Unit = {
    // Node groups are leafs.
    assert(planModel.childrenOf(dummyGroup) == List.empty)
    assertThrows(
      "Group is not allowed to have cost. It's expected to throw an exception when " +
        "getting its cost but not") {
      // Node groups don't have user-defined cost, expect exception here.
      costModel.costOf(dummyGroup)
    }
    propertyModel.propertyDefs.foreach {
      propDef =>
        // Node groups don't have user-defined property, expect exception here.
        assertThrows(
          "Group is not allowed to return its property directly to optimizer (optimizer already" +
            " knew that). It's expected to throw an exception when getting its property but not") {
          propDef.getProperty(dummyGroup)
        }
    }
  }

  private val propSetFactory: PropertySetFactory[T] = PropertySetFactory(this)

  override def newPlanner(
      plan: T,
      constraintSet: PropertySet[T],
      altConstraintSets: Seq[PropertySet[T]]): CboPlanner[T] = {
    CboPlanner(this, altConstraintSets, constraintSet, plan)
  }

  override def propSetsOf(plan: T): PropertySet[T] = propertySetFactory().get(plan)

  private[cbo] def withNewChildren(node: T, newChildren: Seq[T]): T = {
    val oldChildren = planModel.childrenOf(node)
    assert(newChildren.size == oldChildren.size)
    val out = planModel.withNewChildren(node, newChildren)
    assert(planModel.childrenOf(out).size == newChildren.size)
    out
  }

  private[cbo] def isGroupLeaf(node: T): Boolean = {
    planModel.isGroupLeaf(node)
  }

  private[cbo] def isLeaf(node: T): Boolean = {
    planModel.childrenOf(node).isEmpty
  }

  private[cbo] def isCanonical(node: T): Boolean = {
    assert(!planModel.isGroupLeaf(node))
    planModel.childrenOf(node).forall(child => planModel.isGroupLeaf(child))
  }

  private[cbo] def getChildrenGroupIds(n: T): Seq[Int] = {
    assert(isCanonical(n))
    planModel
      .childrenOf(n)
      .map(child => planModel.getGroupId(child))
  }

  private[cbo] def propertySetFactory(): PropertySetFactory[T] = propSetFactory

  private[cbo] def dummyGroupLeaf(): T = {
    dummyGroup
  }

  private[cbo] def getInfCost(): Cost = infCost

  private[cbo] def isInfCost(cost: Cost) = costModel.costComparator().equiv(cost, infCost)
}

object Cbo {
  private[cbo] def apply[T <: AnyRef](
      costModel: CostModel[T],
      planModel: PlanModel[T],
      propertyModel: PropertyModel[T],
      explain: CboExplain[T],
      ruleFactory: CboRule.Factory[T]): Cbo[T] = {
    new Cbo[T](CboConfig(), costModel, planModel, propertyModel, explain, ruleFactory)
  }

  trait PropertySetFactory[T <: AnyRef] {
    def get(node: T): PropertySet[T]
    def childrenConstraintSets(constraintSet: PropertySet[T], node: T): Seq[PropertySet[T]]
  }

  private object PropertySetFactory {
    def apply[T <: AnyRef](cbo: Cbo[T]): PropertySetFactory[T] = new PropertySetFactoryImpl[T](cbo)

    private class PropertySetFactoryImpl[T <: AnyRef](val cbo: Cbo[T])
      extends PropertySetFactory[T] {
      private val propDefs: Seq[PropertyDef[T, _ <: Property[T]]] = cbo.propertyModel.propertyDefs

      override def get(node: T): PropertySet[T] = {
        val m: Map[PropertyDef[T, _ <: Property[T]], Property[T]] =
          propDefs.map(propDef => (propDef, propDef.getProperty(node))).toMap
        PropertySet[T](m)
      }

      override def childrenConstraintSets(
          constraintSet: PropertySet[T],
          node: T): Seq[PropertySet[T]] = {
        val builder: Seq[mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]]] =
          cbo.planModel
            .childrenOf(node)
            .map(_ => mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]]())

        propDefs
          .foldLeft(builder) {
            (
                builder: Seq[mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]]],
                propDef: PropertyDef[T, _ <: Property[T]]) =>
              val constraint = constraintSet.get(propDef)
              val childrenConstraints = propDef.getChildrenConstraints(constraint, node)
              builder.zip(childrenConstraints).map {
                case (childBuilder, childConstraint) =>
                  childBuilder += (propDef -> childConstraint)
              }
          }
          .map {
            builder: mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]] =>
              PropertySet[T](builder.toMap)
          }
      }
    }
  }
}
