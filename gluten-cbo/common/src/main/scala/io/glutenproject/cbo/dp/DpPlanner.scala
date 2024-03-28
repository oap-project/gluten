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
package io.glutenproject.cbo.dp

import io.glutenproject.cbo._
import io.glutenproject.cbo.Best.KnownCostPath
import io.glutenproject.cbo.best.BestFinder
import io.glutenproject.cbo.dp.DpZipperAlgo.Adjustment.Panel
import io.glutenproject.cbo.memo.{Memo, MemoTable}
import io.glutenproject.cbo.path.{CboPath, PathFinder}
import io.glutenproject.cbo.property.PropertySet
import io.glutenproject.cbo.rule.{EnforcerRuleSet, RuleApplier, Shape}

// TODO: Branch and bound pruning.
private class DpPlanner[T <: AnyRef] private (
    cbo: Cbo[T],
    altConstraintSets: Seq[PropertySet[T]],
    constraintSet: PropertySet[T],
    plan: T)
  extends CboPlanner[T] {
  import DpPlanner._

  private val memo = Memo.unsafe(cbo)
  private val rules = cbo.ruleFactory.create().map(rule => RuleApplier(cbo, memo, rule))
  private val enforcerRuleSet = EnforcerRuleSet[T](cbo, memo)

  private lazy val rootGroupId: Int = {
    memo.memorize(plan, constraintSet).id()
  }

  private lazy val best: (Best[T], KnownCostPath[T]) = {
    altConstraintSets.foreach(propSet => memo.memorize(plan, propSet))
    val groupId = rootGroupId
    val memoTable = memo.table()
    val best = findBest(memoTable, groupId)
    (best, best.path())
  }

  override def plan(): T = {
    best._2.cboPath.plan()
  }

  override def newState(): PlannerState[T] = {
    val foundBest = best._1
    PlannerState(cbo, memo.newState(), rootGroupId, foundBest)
  }

  private def findBest(memoTable: MemoTable[T], groupId: Int): Best[T] = {
    val cKey = memoTable.allGroups()(groupId).clusterKey()
    val algoDef = new DpExploreAlgoDef[T]
    val adjustment = new ExploreAdjustment(cbo, memoTable, rules, enforcerRuleSet)
    DpClusterAlgo.resolve(memoTable, algoDef, adjustment, cKey)
    val finder = BestFinder(cbo, memoTable.newState())
    finder.bestOf(groupId)
  }
}

object DpPlanner {
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      altConstraintSets: Seq[PropertySet[T]],
      constraintSet: PropertySet[T],
      plan: T): CboPlanner[T] = {
    new DpPlanner(cbo, altConstraintSets: Seq[PropertySet[T]], constraintSet, plan)
  }

  // Visited flag.
  sealed private trait SolvedFlag
  private case object Solved extends SolvedFlag

  private class DpExploreAlgoDef[T <: AnyRef] extends DpClusterAlgoDef[T, SolvedFlag, SolvedFlag] {
    override def solveNode(
        node: InClusterNode[T],
        childrenClustersOutput: CboClusterKey => SolvedFlag): SolvedFlag = Solved
    override def solveCluster(
        group: CboClusterKey,
        nodesOutput: InClusterNode[T] => SolvedFlag): SolvedFlag = Solved
    override def solveNodeOnCycle(node: InClusterNode[T]): SolvedFlag = Solved
    override def solveClusterOnCycle(cluster: CboClusterKey): SolvedFlag = Solved
  }

  private class ExploreAdjustment[T <: AnyRef](
      cbo: Cbo[T],
      memoTable: MemoTable[T],
      rules: Seq[RuleApplier[T]],
      enforcerRuleSet: EnforcerRuleSet[T])
    extends DpClusterAlgo.Adjustment[T] {
    private val allGroups = memoTable.allGroups()
    private val clusterLookup = cKey => memoTable.getCluster(cKey)

    override def exploreChildX(
        panel: Panel[InClusterNode[T], CboClusterKey],
        x: InClusterNode[T]): Unit = {}
    override def exploreChildY(
        panel: Panel[InClusterNode[T], CboClusterKey],
        y: CboClusterKey): Unit = {}
    override def exploreParentX(
        panel: Panel[InClusterNode[T], CboClusterKey],
        x: InClusterNode[T]): Unit = {}

    override def exploreParentY(
        panel: Panel[InClusterNode[T], CboClusterKey],
        cKey: CboClusterKey): Unit = {
      memoTable.doExhaustively {
        applyEnforcerRules(panel, cKey)
        applyRules(panel, cKey)
      }
    }

    private def applyRules(
        panel: Panel[InClusterNode[T], CboClusterKey],
        cKey: CboClusterKey): Unit = {
      if (rules.isEmpty) {
        return
      }
      val cluster = clusterLookup(cKey)
      cluster.nodes().foreach {
        node =>
          val shapes = rules.map(_.shape())
          findPaths(node, shapes)(path => rules.foreach(rule => applyRule(panel, cKey, rule, path)))
      }
    }

    private def applyEnforcerRules(
        panel: Panel[InClusterNode[T], CboClusterKey],
        cKey: CboClusterKey): Unit = {
      val cluster = clusterLookup(cKey)
      cKey.propSets(memoTable).foreach {
        constraintSet =>
          val enforcerRules = enforcerRuleSet.rulesOf(constraintSet)
          if (enforcerRules.nonEmpty) {
            val shapes = enforcerRules.map(_.shape())
            cluster.nodes().foreach {
              node =>
                findPaths(node, shapes)(
                  path => enforcerRules.foreach(rule => applyRule(panel, cKey, rule, path)))
            }
          }
      }
    }

    private def findPaths(canonical: CanonicalNode[T], shapes: Seq[Shape[T]])(
        onFound: CboPath[T] => Unit): Unit = {
      val finder = shapes
        .foldLeft(
          PathFinder
            .builder(cbo, memoTable)) {
          case (builder, shape) =>
            builder.output(shape.wizard())
        }
        .build()
      finder.find(canonical).foreach(path => onFound(path))
    }

    private def applyRule(
        panel: Panel[InClusterNode[T], CboClusterKey],
        thisClusterKey: CboClusterKey,
        rule: RuleApplier[T],
        path: CboPath[T]): Unit = {
      val probe = memoTable.probe()
      rule.apply(path)
      val diff = probe.toDiff()
      val changedClusters = diff.changedClusters()
      if (changedClusters.isEmpty) {
        return
      }

      // One or more cluster changed. If they're not the current cluster, we should
      // withdraw the DP results for them to trigger re-computation. Since
      // changed cluster (may created new groups, may added new nodes) could expand the
      // search spaces again.

      changedClusters.foreach {
        case cKey if cKey == thisClusterKey =>
        // This cluster has been changed. This cluster is being solved so we
        // don't have to invalidate.
        case cKey =>
          // Changes happened on another cluster. Invalidate solution for the cluster
          // To trigger re-computation.
          panel.invalidateYSolution(cKey)
      }
    }
  }

  private object ExploreAdjustment {}
}
