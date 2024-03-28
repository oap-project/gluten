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
package io.glutenproject.cbo.mock

import io.glutenproject.cbo._
import io.glutenproject.cbo.memo.{MemoState, MemoStore}
import io.glutenproject.cbo.property.PropertySet
import io.glutenproject.cbo.vis.GraphvizVisualizer

import scala.collection.mutable

case class MockMemoState[T <: AnyRef] private (
    override val cbo: Cbo[T],
    override val clusterLookup: Map[CboClusterKey, CboCluster[T]],
    override val allGroups: Seq[CboGroup[T]])
  extends MemoState[T] {
  def printGraphviz(group: CboGroup[T]): Unit = {
    val graph = GraphvizVisualizer(cbo, this, group.id())
    // scalastyle:off println
    println(graph.format())
    // scalastyle:on println
  }

  def printGraphviz(best: Best[T]): Unit = {
    val graph = vis.GraphvizVisualizer(cbo, this, best)
    // scalastyle:off println
    println(graph.format())
    // scalastyle:on println
  }

  override def allClusters(): Iterable[CboCluster[T]] = clusterLookup.values

  override def getCluster(key: CboClusterKey): CboCluster[T] = clusterLookup(key)

  override def getGroup(id: Int): CboGroup[T] = allGroups(id)
}

object MockMemoState {
  class Builder[T <: AnyRef] private (cbo: Cbo[T]) {
    private var propSet: PropertySet[T] = PropertySet[T](List.empty)
    private val clusterBuffer = mutable.Map[CboClusterKey, MockMutableCluster[T]]()
    private val groupFactory: MockMutableGroup.Factory[T] =
      MockMutableGroup.Factory.create[T](cbo, propSet)

    def withPropertySet(propSet: PropertySet[T]): Builder[T] = {
      this.propSet = propSet
      this
    }

    def newCluster(): MockMutableCluster[T] = {
      val id = clusterBuffer.size
      val key = MockMutableCluster.DummyIntClusterKey(id)
      val cluster = MockMutableCluster[T](cbo, key, propSet, groupFactory)
      clusterBuffer += (key -> cluster)
      cluster
    }

    def build(): MockMemoState[T] = {
      MockMemoState[T](cbo, clusterBuffer.toMap, groupFactory.allGroups())
    }
  }

  object Builder {
    def apply[T <: AnyRef](cbo: Cbo[T]): Builder[T] = {
      new Builder[T](cbo)
    }
  }

  // TODO add groups with different property sets
  class MockMutableCluster[T <: AnyRef] private (
      cbo: Cbo[T],
      key: CboClusterKey,
      groupFactory: MockMutableGroup.Factory[T])
    extends CboCluster[T] {
    private val nodeBuffer = mutable.ArrayBuffer[CanonicalNode[T]]()

    def newGroup(): MockMutableGroup[T] = {
      groupFactory.newGroup(key)
    }

    def addNodes(nodes: Seq[CanonicalNode[T]]): Unit = {
      nodeBuffer ++= nodes
    }

    override def nodes(): Seq[CanonicalNode[T]] = nodeBuffer
  }

  object MockMutableCluster {
    def apply[T <: AnyRef](
        cbo: Cbo[T],
        key: CboClusterKey,
        propSet: PropertySet[T],
        groupFactory: MockMutableGroup.Factory[T]): MockMutableCluster[T] = {
      new MockMutableCluster[T](cbo, key, groupFactory)
    }

    case class DummyIntClusterKey(id: Int) extends CboClusterKey
  }

  class MockMutableGroup[T <: AnyRef] private (
      override val id: Int,
      override val clusterKey: CboClusterKey,
      override val propSet: PropertySet[T],
      override val self: T)
    extends CboGroup[T] {
    private val nodes: mutable.ArrayBuffer[CanonicalNode[T]] = mutable.ArrayBuffer()

    def add(node: CanonicalNode[T]): Unit = {
      nodes += node
    }

    def add(newNodes: Seq[CanonicalNode[T]]): Unit = {
      nodes ++= newNodes
    }

    override def nodes(store: MemoStore[T]): Iterable[CanonicalNode[T]] = nodes
  }

  object MockMutableGroup {
    class Factory[T <: AnyRef] private (cbo: Cbo[T], propSet: PropertySet[T]) {
      private val groupBuffer = mutable.ArrayBuffer[MockMutableGroup[T]]()

      def newGroup(clusterKey: CboClusterKey): MockMutableGroup[T] = {
        val id = groupBuffer.size
        val group =
          new MockMutableGroup[T](id, clusterKey, propSet, cbo.planModel.newGroupLeaf(id, propSet))
        groupBuffer += group
        group
      }

      def allGroups(): Seq[MockMutableGroup[T]] = groupBuffer
    }

    object Factory {
      def create[T <: AnyRef](cbo: Cbo[T], propSet: PropertySet[T]): Factory[T] = {
        new Factory[T](cbo, propSet)
      }
    }
  }

}
