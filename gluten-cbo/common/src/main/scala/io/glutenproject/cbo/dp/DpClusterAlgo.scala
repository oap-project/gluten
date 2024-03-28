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

import io.glutenproject.cbo.{CboClusterKey, InClusterNode}
import io.glutenproject.cbo.dp.DpZipperAlgo.Solution
import io.glutenproject.cbo.memo.MemoTable

// Dynamic programming algorithm to solve problem against a single CBO cluster that can be
// broken down to sub problems for sub clusters.
//
// FIXME: Code is so similar with DpGroupAlgo.
trait DpClusterAlgoDef[T <: AnyRef, NodeOutput <: AnyRef, ClusterOutput <: AnyRef] {
  def solveNode(
      node: InClusterNode[T],
      childrenClustersOutput: CboClusterKey => ClusterOutput): NodeOutput
  def solveCluster(
      cluster: CboClusterKey,
      nodesOutput: InClusterNode[T] => NodeOutput): ClusterOutput

  def solveNodeOnCycle(node: InClusterNode[T]): NodeOutput
  def solveClusterOnCycle(cluster: CboClusterKey): ClusterOutput
}

object DpClusterAlgo {

  trait Adjustment[T <: AnyRef] extends DpZipperAlgo.Adjustment[InClusterNode[T], CboClusterKey]

  object Adjustment {
    private class None[T <: AnyRef] extends Adjustment[T] {
      override def exploreChildX(
          panel: DpZipperAlgo.Adjustment.Panel[InClusterNode[T], CboClusterKey],
          x: InClusterNode[T]): Unit = {}
      override def exploreParentY(
          panel: DpZipperAlgo.Adjustment.Panel[InClusterNode[T], CboClusterKey],
          y: CboClusterKey): Unit = {}
      override def exploreChildY(
          panel: DpZipperAlgo.Adjustment.Panel[InClusterNode[T], CboClusterKey],
          y: CboClusterKey): Unit = {}
      override def exploreParentX(
          panel: DpZipperAlgo.Adjustment.Panel[InClusterNode[T], CboClusterKey],
          x: InClusterNode[T]): Unit = {}
    }

    def none[T <: AnyRef](): Adjustment[T] = new None[T]()
  }

  def resolve[T <: AnyRef, NodeOutput <: AnyRef, ClusterOutput <: AnyRef](
      memoTable: MemoTable[T],
      groupAlgoDef: DpClusterAlgoDef[T, NodeOutput, ClusterOutput],
      adjustment: Adjustment[T],
      cluster: CboClusterKey)
      : Solution[InClusterNode[T], CboClusterKey, NodeOutput, ClusterOutput] = {
    DpZipperAlgo.resolve(new ZipperAlgoDefImpl(memoTable, groupAlgoDef), adjustment, cluster)
  }

  private class ZipperAlgoDefImpl[T <: AnyRef, NodeOutput <: AnyRef, ClusterOutput <: AnyRef](
      memoTable: MemoTable[T],
      clusterAlgoDef: DpClusterAlgoDef[T, NodeOutput, ClusterOutput])
    extends DpZipperAlgoDef[InClusterNode[T], CboClusterKey, NodeOutput, ClusterOutput] {
    override def idOfX(x: InClusterNode[T]): Any = {
      x
    }

    override def idOfY(y: CboClusterKey): Any = {
      y
    }

    override def browseX(x: InClusterNode[T]): Iterable[CboClusterKey] = {
      val allGroups = memoTable.allGroups()
      x.can
        .getChildrenGroups(allGroups)
        .map(gn => allGroups(gn.groupId()).clusterKey())
    }

    override def browseY(y: CboClusterKey): Iterable[InClusterNode[T]] = {
      memoTable.getCluster(y).nodes().map(n => InClusterNode(y, n))
    }

    override def solveX(x: InClusterNode[T], yOutput: CboClusterKey => ClusterOutput): NodeOutput =
      clusterAlgoDef.solveNode(x, yOutput)

    override def solveY(y: CboClusterKey, xOutput: InClusterNode[T] => NodeOutput): ClusterOutput =
      clusterAlgoDef.solveCluster(y, xOutput)

    override def solveXOnCycle(x: InClusterNode[T]): NodeOutput = clusterAlgoDef.solveNodeOnCycle(x)

    override def solveYOnCycle(y: CboClusterKey): ClusterOutput =
      clusterAlgoDef.solveClusterOnCycle(y)
  }
}
