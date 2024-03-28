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

import io.glutenproject.cbo.memo.MemoStore
import io.glutenproject.cbo.property.PropertySet

trait CboGroup[T <: AnyRef] {
  def id(): Int
  def clusterKey(): CboClusterKey
  def propSet(): PropertySet[T]
  def self(): T
  def nodes(store: MemoStore[T]): Iterable[CanonicalNode[T]]
}

object CboGroup {
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      clusterKey: CboClusterKey,
      id: Int,
      propSet: PropertySet[T]): CboGroup[T] = {
    new CboGroupImpl[T](cbo, clusterKey, id, propSet)
  }

  private class CboGroupImpl[T <: AnyRef](
      cbo: Cbo[T],
      clusterKey: CboClusterKey,
      override val id: Int,
      override val propSet: PropertySet[T])
    extends CboGroup[T] {
    private val groupLeaf: T = cbo.planModel.newGroupLeaf(id, propSet)

    override def clusterKey(): CboClusterKey = clusterKey
    override def self(): T = groupLeaf
    override def nodes(store: MemoStore[T]): Iterable[CanonicalNode[T]] = {
      store.getCluster(clusterKey).nodes().filter(n => n.propSet().satisfies(propSet))
    }
    override def toString(): String = {
      s"CboGroup(id=$id, clusterKey=$clusterKey, propSet=$propSet))"
    }
  }
}
