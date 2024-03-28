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
package io.glutenproject.cbo.exaustive

import io.glutenproject.cbo._
import io.glutenproject.cbo.Best.KnownCostPath
import io.glutenproject.cbo.best.BestFinder
import io.glutenproject.cbo.exaustive.ExhaustivePlanner.ExhaustiveExplorer
import io.glutenproject.cbo.memo.{Memo, MemoState}
import io.glutenproject.cbo.path._
import io.glutenproject.cbo.property.PropertySet
import io.glutenproject.cbo.rule.{EnforcerRuleSet, RuleApplier, Shape}

private class ExhaustivePlanner[T <: AnyRef] private (
    cbo: Cbo[T],
    altConstraintSets: Seq[PropertySet[T]],
    constraintSet: PropertySet[T],
    plan: T)
  extends CboPlanner[T] {
  private val memo = Memo(cbo)
  private val rules = cbo.ruleFactory.create().map(rule => RuleApplier(cbo, memo, rule))
  private val enforcerRuleSet = EnforcerRuleSet[T](cbo, memo)

  private lazy val rootGroupId: Int = {
    memo.memorize(plan, constraintSet).id()
  }

  private lazy val best: (Best[T], KnownCostPath[T]) = {
    altConstraintSets.foreach(propSet => memo.memorize(plan, propSet))
    val groupId = rootGroupId
    explore()
    val memoState = memo.newState()
    val best = findBest(memoState, groupId)
    (best, best.path())
  }

  override def plan(): T = {
    best._2.cboPath.plan()
  }

  override def newState(): PlannerState[T] = {
    val foundBest = best._1
    PlannerState(cbo, memo.newState(), rootGroupId, foundBest)
  }

  private def explore(): Unit = {
    // TODO1: Prune paths within cost threshold
    // ~~ TODO2: Use partial-canonical paths to reduce search space ~~
    memo.doExhaustively {
      val explorer = new ExhaustiveExplorer(cbo, memo.newState(), rules, enforcerRuleSet)
      explorer.explore()
    }
  }

  private def findBest(memoState: MemoState[T], groupId: Int): Best[T] = {
    BestFinder(cbo, memoState).bestOf(groupId)
  }
}

object ExhaustivePlanner {
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      altConstraintSets: Seq[PropertySet[T]],
      constraintSet: PropertySet[T],
      plan: T): CboPlanner[T] = {
    new ExhaustivePlanner(cbo, altConstraintSets, constraintSet, plan)
  }

  private class ExhaustiveExplorer[T <: AnyRef](
      cbo: Cbo[T],
      memoState: MemoState[T],
      rules: Seq[RuleApplier[T]],
      enforcerRuleSet: EnforcerRuleSet[T]) {
    private val allClusters = memoState.allClusters()
    private val allGroups = memoState.allGroups()

    def explore(): Unit = {
      // TODO: ONLY APPLY RULES ON ALTERED GROUPS (and close parents)
      applyEnforcerRules()
      applyRules()
    }

    private def findPaths(canonical: CanonicalNode[T], shapes: Seq[Shape[T]])(
        onFound: CboPath[T] => Unit): Unit = {
      val finder = shapes
        .foldLeft(
          PathFinder
            .builder(cbo, memoState)) {
          case (builder, shape) =>
            builder.output(shape.wizard())
        }
        .build()
      finder.find(canonical).foreach(path => onFound(path))
    }

    private def applyRule(rule: RuleApplier[T], path: CboPath[T]): Unit = {
      rule.apply(path)
    }

    private def applyRules(): Unit = {
      if (rules.isEmpty) {
        return
      }
      val shapes = rules.map(_.shape())
      allClusters
        .flatMap(c => c.nodes())
        .foreach(
          node => findPaths(node, shapes)(path => rules.foreach(rule => applyRule(rule, path))))
    }

    private def applyEnforcerRules(): Unit = {
      allGroups.foreach {
        group =>
          val constraintSet = group.propSet()
          val enforcerRules = enforcerRuleSet.rulesOf(constraintSet)
          if (enforcerRules.nonEmpty) {
            val shapes = enforcerRules.map(_.shape())
            memoState.clusterLookup()(group.clusterKey()).nodes().foreach {
              node =>
                findPaths(node, shapes)(
                  path => enforcerRules.foreach(rule => applyRule(rule, path)))
            }
          }
      }
    }
  }
}
