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
package org.apache.gluten.cbo.rule

import org.apache.gluten.cbo._
import org.apache.gluten.cbo.memo.Closure
import org.apache.gluten.cbo.path.CboPath
import org.apache.gluten.cbo.util.CanonicalNodeMap

import scala.collection.mutable

trait RuleApplier[T <: AnyRef] {
  def apply(path: CboPath[T]): Unit
  def shape(): Shape[T]
}

object RuleApplier {
  def apply[T <: AnyRef](cbo: Cbo[T], closure: Closure[T], rule: CboRule[T]): RuleApplier[T] = {
    new ShapeAwareRuleApplier[T](cbo, new RegularRuleApplier(cbo, closure, rule))
  }

  def apply[T <: AnyRef](
      cbo: Cbo[T],
      closure: Closure[T],
      rule: EnforcerRule[T]): RuleApplier[T] = {
    new ShapeAwareRuleApplier[T](cbo, new EnforcerRuleApplier[T](cbo, closure, rule))
  }

  private class RegularRuleApplier[T <: AnyRef](cbo: Cbo[T], closure: Closure[T], rule: CboRule[T])
    extends RuleApplier[T] {
    private val cache = new CanonicalNodeMap[T, mutable.Set[T]](cbo)

    override def apply(path: CboPath[T]): Unit = {
      val can = path.node().self().asCanonical()
      val plan = path.plan()
      val appliedPlans = cache.getOrElseUpdate(can, mutable.Set())
      if (appliedPlans.contains(plan)) {
        return
      }
      apply0(can, plan)
      appliedPlans += plan
    }

    private def apply0(can: CanonicalNode[T], plan: T): Unit = {
      val equivalents = rule.shift(plan)
      equivalents.foreach {
        equiv =>
          closure
            .openFor(can)
            .memorize(equiv, cbo.propertySetFactory().get(equiv))
      }
    }

    override def shape(): Shape[T] = rule.shape()
  }

  private class EnforcerRuleApplier[T <: AnyRef](
      cbo: Cbo[T],
      closure: Closure[T],
      rule: EnforcerRule[T])
    extends RuleApplier[T] {
    private val cache = new CanonicalNodeMap[T, mutable.Set[T]](cbo)
    private val constraint = rule.constraint()
    private val constraintDef = constraint.definition()

    override def apply(path: CboPath[T]): Unit = {
      val can = path.node().self().asCanonical()
      if (can.propSet().get(constraintDef).satisfies(constraint)) {
        return
      }
      val plan = path.plan()
      val appliedPlans = cache.getOrElseUpdate(can, mutable.Set())
      if (appliedPlans.contains(plan)) {
        return
      }
      apply0(can, plan)
      appliedPlans += plan
    }

    private def apply0(can: CanonicalNode[T], plan: T): Unit = {
      val propSet = cbo.propertySetFactory().get(plan)
      val constraintSet = propSet.withProp(constraint)
      val equivalents = rule.shift(plan)
      equivalents.foreach {
        equiv =>
          closure
            .openFor(can)
            .memorize(equiv, constraintSet)
      }
    }

    override def shape(): Shape[T] = rule.shape()
  }

  private class ShapeAwareRuleApplier[T <: AnyRef](cbo: Cbo[T], rule: RuleApplier[T])
    extends RuleApplier[T] {
    private val ruleShape = rule.shape()

    override def apply(path: CboPath[T]): Unit = {
      if (!ruleShape.identify(path)) {
        return
      }
      rule.apply(path)
    }

    override def shape(): Shape[T] = ruleShape
  }
}
