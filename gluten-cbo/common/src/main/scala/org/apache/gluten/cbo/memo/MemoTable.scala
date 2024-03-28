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
package org.apache.gluten.cbo.memo

import org.apache.gluten.cbo._
import org.apache.gluten.cbo.CboCluster.ImmutableCboCluster
import org.apache.gluten.cbo.property.PropertySet

sealed trait MemoTable[T <: AnyRef] extends MemoStore[T] {
  import MemoTable._

  def cbo: Cbo[T]

  override def getCluster(key: CboClusterKey): CboCluster[T]
  override def getGroup(id: Int): CboGroup[T]

  def allClusters(): Seq[CboClusterKey]
  def allGroups(): Seq[CboGroup[T]]

  def getClusterPropSets(key: CboClusterKey): Set[PropertySet[T]]

  def probe(): Probe[T]

  def writeCount(): Int
}

object MemoTable {
  def create[T <: AnyRef](cbo: Cbo[T]): Writable[T] = ForwardMemoTable(cbo)

  trait Writable[T <: AnyRef] extends MemoTable[T] {
    def newCluster(): CboClusterKey
    def groupOf(key: CboClusterKey, propertySet: PropertySet[T]): CboGroup[T]

    def addToCluster(key: CboClusterKey, node: CanonicalNode[T]): Unit
    def mergeClusters(one: CboClusterKey, other: CboClusterKey): Unit
  }

  trait Probe[T <: AnyRef] {
    import Probe._
    def toDiff(): Diff[T]
  }

  object Probe {
    trait Diff[T <: AnyRef] {
      def changedClusters(): Set[CboClusterKey]
    }

    object Diff {
      def apply[T <: AnyRef](changedClusters: Set[CboClusterKey]): Diff[T] = DiffImpl(
        changedClusters)
      private case class DiffImpl[T <: AnyRef](override val changedClusters: Set[CboClusterKey])
        extends Diff[T]
    }
  }

  implicit class MemoTableImplicits[T <: AnyRef](table: MemoTable[T]) {
    def newState(): MemoState[T] = {
      val immutableClusters = table
        .allClusters()
        .map(key => key -> ImmutableCboCluster(table.cbo, table.getCluster(key)))
        .toMap
      MemoState(table.cbo, immutableClusters, table.allGroups())
    }

    def doExhaustively(func: => Unit): Unit = {
      while (true) {
        val prevWriteCount = table.writeCount()
        func
        val writeCount = table.writeCount()
        assert(writeCount >= prevWriteCount)
        if (writeCount == prevWriteCount) {
          return
        }
      }
    }
  }
}
