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
package io.glutenproject.cbo.memo

import io.glutenproject.cbo._
import io.glutenproject.cbo.CboCluster.MutableCboCluster
import io.glutenproject.cbo.memo.MemoTable.Probe
import io.glutenproject.cbo.property.PropertySet
import io.glutenproject.cbo.util.IndexDisjointSet

import scala.collection.mutable

class ForwardMemoTable[T <: AnyRef] private (override val cbo: Cbo[T])
  extends MemoTable.Writable[T] {
  import ForwardMemoTable._

  private val groupBuffer: mutable.ArrayBuffer[CboGroup[T]] = mutable.ArrayBuffer()

  private val clusterKeyBuffer: mutable.ArrayBuffer[IntClusterKey] = mutable.ArrayBuffer()
  private val clusterBuffer: mutable.ArrayBuffer[MutableCboCluster[T]] = mutable.ArrayBuffer()
  private val clusterDisjointSet: IndexDisjointSet = IndexDisjointSet()
  private val groupLookup: mutable.ArrayBuffer[mutable.Map[PropertySet[T], CboGroup[T]]] =
    mutable.ArrayBuffer()

  private val clusterMergeLog: mutable.ArrayBuffer[(Int, Int)] = mutable.ArrayBuffer()
  private var memoWriteCount: Int = 0

  override def getCluster(key: CboClusterKey): MutableCboCluster[T] = {
    val ancestor = ancestorClusterIdOf(key)
    clusterBuffer(ancestor)
  }

  override def newCluster(metadata: Metadata): CboClusterKey = {
    checkBufferSizes()
    val key = IntClusterKey(clusterBuffer.size, metadata)
    clusterKeyBuffer += key
    clusterBuffer += MutableCboCluster(cbo, metadata)
    clusterDisjointSet.grow()
    groupLookup += mutable.Map()
    key
  }

  override def groupOf(key: CboClusterKey, propSet: PropertySet[T]): CboGroup[T] = {
    val ancestor = ancestorClusterIdOf(key)
    val lookup = groupLookup(ancestor)
    if (lookup.contains(propSet)) {
      return lookup(propSet)
    }
    val gid = groupBuffer.size
    val newGroup =
      CboGroup(cbo, IntClusterKey(ancestor, key.metadata), gid, propSet)
    lookup += propSet -> newGroup
    groupBuffer += newGroup
    memoWriteCount += 1
    newGroup
  }

  override def getClusterPropSets(key: CboClusterKey): Set[PropertySet[T]] = {
    val ancestor = ancestorClusterIdOf(key)
    groupLookup(ancestor).keySet.toSet
  }

  override def addToCluster(key: CboClusterKey, node: CanonicalNode[T]): Unit = {
    getCluster(key).add(node)
    memoWriteCount += 1
  }

  override def mergeClusters(one: CboClusterKey, other: CboClusterKey): Unit = {
    val oneAncestor = ancestorClusterIdOf(one)
    val otherAncestor = ancestorClusterIdOf(other)

    if (oneAncestor == otherAncestor) {
      // Already merged.
      return
    }

    case class Merge(from: CboClusterKey, to: CboClusterKey) {
      cbo.metadataModel.verify(from.metadata, to.metadata)
    }

    val merge = if (oneAncestor > otherAncestor) {
      Merge(clusterKeyBuffer(oneAncestor), clusterKeyBuffer(otherAncestor))
    } else {
      Merge(clusterKeyBuffer(otherAncestor), clusterKeyBuffer(oneAncestor))
    }

    val fromKey = merge.from
    val toKey = merge.to

    val fromCluster = clusterBuffer(fromKey.id())
    val toCluster = clusterBuffer(toKey.id())

    // Add absent nodes.
    fromCluster.nodes().foreach {
      fromNode =>
        if (!toCluster.contains(fromNode)) {
          toCluster.add(fromNode)
        }
    }

    // Add absent groups.
    val fromGroups = groupLookup(fromKey.id())
    val toGroups = groupLookup(toKey.id())
    fromGroups.foreach {
      case (fromPropSet, _) =>
        if (!toGroups.contains(fromPropSet)) {
          groupOf(toKey, fromPropSet)
        }
    }

    // Forward the element in disjoint set.
    clusterDisjointSet.forward(fromKey.id(), toKey.id())
    clusterMergeLog += (fromKey.id() -> toKey.id())
    memoWriteCount += 1
  }

  override def getGroup(id: Int): CboGroup[T] = groupBuffer(id)

  override def allClusters(): Seq[CboClusterKey] = clusterKeyBuffer

  override def allGroups(): Seq[CboGroup[T]] = groupBuffer

  private def ancestorClusterIdOf(key: CboClusterKey): Int = {
    clusterDisjointSet.find(key.id())
  }

  private def checkBufferSizes(): Unit = {
    assert(clusterKeyBuffer.size == clusterBuffer.size)
    assert(clusterKeyBuffer.size == clusterDisjointSet.size)
    assert(clusterKeyBuffer.size == groupLookup.size)
  }

  override def probe(): MemoTable.Probe[T] = new ForwardMemoTable.Probe[T](this)

  override def writeCount(): Int = memoWriteCount
}

object ForwardMemoTable {
  def apply[T <: AnyRef](cbo: Cbo[T]): MemoTable.Writable[T] = new ForwardMemoTable[T](cbo)

  private case class IntClusterKey(id: Int, metadata: Metadata) extends CboClusterKey

  private class Probe[T <: AnyRef](table: ForwardMemoTable[T]) extends MemoTable.Probe[T] {
    private val probedClusterCount: Int = table.clusterKeyBuffer.size
    private val probedGroupCount: Int = table.groupBuffer.size
    private val probedMergeLogSize: Int = table.clusterMergeLog.size

    override def toDiff(): Probe.Diff[T] = {
      val newClusterCount = table.clusterKeyBuffer.size
      val newGroupCount = table.groupBuffer.size
      val newMergeLogSize = table.clusterMergeLog.size

      assert(newClusterCount >= probedClusterCount)
      assert(newGroupCount >= probedGroupCount)
      assert(newMergeLogSize >= probedMergeLogSize)

      // Find new clusters.
      val newClusters = table.clusterKeyBuffer.slice(probedClusterCount, newClusterCount)

      // Find resident clusters of the new groups.
      val newGroups = table.groupBuffer.slice(probedGroupCount, newGroupCount)
      val clustersOfNewGroups = newGroups.map(g => g.clusterKey())

      // Find all the affected clusters, if cluster-merge happened.
      val newMergeLogs = table.clusterMergeLog.slice(probedMergeLogSize, newMergeLogSize)
      val affectedClustersDuringMerging = newMergeLogs
        .flatMap {
          case (from, to) =>
            table.clusterDisjointSet.setOf(to)
        }
        .map(index => table.clusterKeyBuffer(index))

      val changedClusters =
        (clustersOfNewGroups.toSet ++ affectedClustersDuringMerging) -- newClusters
      // We consider a existing cluster with new groups changed.
      Probe.Diff(changedClusters)
    }
  }

  implicit class CboClusterKeyImplicits(key: CboClusterKey) {
    def id(): Int = {
      asIntKey().id
    }

    private def asIntKey(): IntClusterKey = {
      key.asInstanceOf[IntClusterKey]
    }
  }
}
