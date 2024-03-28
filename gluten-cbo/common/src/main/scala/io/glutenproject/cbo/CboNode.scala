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

trait CboNode[T <: AnyRef] {
  def cbo(): Cbo[T]
  def self(): T
  def propSet(): PropertySet[T]
}

object CboNode {
  implicit class CboNodeImplicits[T <: AnyRef](node: CboNode[T]) {
    def isCanonical: Boolean = {
      node.isInstanceOf[CanonicalNode[T]]
    }

    def asCanonical(): CanonicalNode[T] = {
      node.asInstanceOf[CanonicalNode[T]]
    }

    def isGroup: Boolean = {
      node.isInstanceOf[GroupNode[T]]
    }

    def asGroup(): GroupNode[T] = {
      node.asInstanceOf[GroupNode[T]]
    }
  }
}

trait CanonicalNode[T <: AnyRef] extends CboNode[T] {
  def childrenCount: Int
}

object CanonicalNode {
  def apply[T <: AnyRef](cbo: Cbo[T], canonical: T): CanonicalNode[T] = {
    assert(cbo.isCanonical(canonical))
    val propSet = cbo.propSetsOf(canonical)
    val children = cbo.planModel.childrenOf(canonical)
    CanonicalNodeImpl[T](cbo, canonical, propSet, children.size)
  }

  // We put CboNode's API methods that accept mutable input in implicit definition.
  // Do not break this rule during further development.
  implicit class CanonicalNodeImplicits[T <: AnyRef](node: CanonicalNode[T]) {
    def isLeaf(): Boolean = {
      node.childrenCount == 0
    }

    def getChildrenGroups(allGroups: Int => CboGroup[T]): Seq[GroupNode[T]] = {
      val cbo = node.cbo()
      cbo.getChildrenGroupIds(node.self()).map(allGroups(_)).map(g => GroupNode(cbo, g))
    }

    def getChildrenGroupIds(): Seq[Int] = {
      val cbo = node.cbo()
      cbo.getChildrenGroupIds(node.self())
    }
  }

  private case class CanonicalNodeImpl[T <: AnyRef](
      cbo: Cbo[T],
      override val self: T,
      override val propSet: PropertySet[T],
      override val childrenCount: Int)
    extends CanonicalNode[T]
}

trait GroupNode[T <: AnyRef] extends CboNode[T] {
  def groupId(): Int
}

object GroupNode {
  def apply[T <: AnyRef](cbo: Cbo[T], group: CboGroup[T]): GroupNode[T] = {
    GroupNodeImpl[T](cbo, group.self(), group.propSet(), group.id())
  }

  private case class GroupNodeImpl[T <: AnyRef](
      cbo: Cbo[T],
      override val self: T,
      override val propSet: PropertySet[T],
      override val groupId: Int)
    extends GroupNode[T] {}

  // We put CboNode's API methods that accept mutable input in implicit definition.
  // Do not break this rule during further development.
  implicit class GroupNodeImplicits[T <: AnyRef](gn: GroupNode[T]) {
    def group(allGroups: Int => CboGroup[T]): CboGroup[T] = {
      allGroups(gn.groupId())
    }
  }
}

trait InGroupNode[T <: AnyRef] {
  def groupId: Int
  def can: CanonicalNode[T]
}

object InGroupNode {
  def apply[T <: AnyRef](groupId: Int, node: CanonicalNode[T]): InGroupNode[T] = {
    InGroupNodeImpl(groupId, node)
  }
  private case class InGroupNodeImpl[T <: AnyRef](groupId: Int, can: CanonicalNode[T])
    extends InGroupNode[T]
}

trait InClusterNode[T <: AnyRef] {
  def clusterKey: CboClusterKey
  def can: CanonicalNode[T]
}

object InClusterNode {
  def apply[T <: AnyRef](clusterId: CboClusterKey, node: CanonicalNode[T]): InClusterNode[T] = {
    InClusterNodeImpl(clusterId, node)
  }
  private case class InClusterNodeImpl[T <: AnyRef](
      clusterKey: CboClusterKey,
      can: CanonicalNode[T])
    extends InClusterNode[T]
}
