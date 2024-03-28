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

import io.glutenproject.cbo.memo.MemoTable
import io.glutenproject.cbo.property.PropertySet

import scala.collection.mutable

trait CboClusterKey {
  def metadata: Metadata
}

object CboClusterKey {
  implicit class CboClusterKeyImplicits[T <: AnyRef](key: CboClusterKey) {
    def propSets(memoTable: MemoTable[T]): Set[PropertySet[T]] = {
      memoTable.getClusterPropSets(key)
    }
  }
}

trait CboCluster[T <: AnyRef] {
  def nodes(): Iterable[CanonicalNode[T]]
}

object CboCluster {
  // Node cluster.
  trait MutableCboCluster[T <: AnyRef] extends CboCluster[T] {
    def cbo(): Cbo[T]
    def contains(t: CanonicalNode[T]): Boolean
    def add(t: CanonicalNode[T]): Unit
  }

  object MutableCboCluster {
    def apply[T <: AnyRef](cbo: Cbo[T], metadata: Metadata): MutableCboCluster[T] = {
      new RegularMutableCboCluster(cbo, metadata)
    }

    private class RegularMutableCboCluster[T <: AnyRef](val cbo: Cbo[T], metadata: Metadata)
      extends MutableCboCluster[T] {
      private val buffer: mutable.Set[CanonicalNode[T]] =
        mutable.Set()

      override def contains(t: CanonicalNode[T]): Boolean = {
        buffer.contains(t)
      }

      override def add(t: CanonicalNode[T]): Unit = {
        cbo.metadataModel.verify(metadata, cbo.metadataModel.metadataOf(t.self()))
        assert(!buffer.contains(t))
        buffer += t
      }

      override def nodes(): Iterable[CanonicalNode[T]] = {
        buffer
      }
    }
  }

  case class ImmutableCboCluster[T <: AnyRef] private (
      cbo: Cbo[T],
      override val nodes: Set[CanonicalNode[T]])
    extends CboCluster[T]

  object ImmutableCboCluster {
    def apply[T <: AnyRef](cbo: Cbo[T], cluster: CboCluster[T]): ImmutableCboCluster[T] = {
      ImmutableCboCluster(cbo, cluster.nodes().toSet)
    }
  }
}
