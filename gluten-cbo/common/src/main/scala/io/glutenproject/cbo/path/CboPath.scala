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
package io.glutenproject.cbo.path

import io.glutenproject.cbo._
import io.glutenproject.cbo.memo.MemoStore

trait CboPath[T <: AnyRef] {
  def cbo(): Cbo[T]
  def keys(): PathKeySet
  def height(): Int
  def node(): CboPath.PathNode[T]
  def plan(): T
}

object CboPath {
  val INF_DEPTH: Int = Int.MaxValue

  trait PathNode[T <: AnyRef] {
    def self(): CboNode[T]
    def children(): Seq[PathNode[T]]
  }

  object PathNode {
    def apply[T <: AnyRef](node: CboNode[T], children: Seq[PathNode[T]]): PathNode[T] = {
      PathNodeImpl(node, children)
    }
  }

  implicit class PathNodeImplicits[T <: AnyRef](pNode: CboPath.PathNode[T]) {
    def zipChildrenWithGroupIds(): Seq[(CboPath.PathNode[T], Int)] = {
      pNode
        .children()
        .zip(pNode.self().asCanonical().getChildrenGroupIds())
    }

    def zipChildrenWithGroups(
        allGroups: Int => CboGroup[T]): Seq[(CboPath.PathNode[T], CboGroup[T])] = {
      pNode
        .children()
        .zip(pNode.self().asCanonical().getChildrenGroups(allGroups).map(_.group(allGroups)))
    }
  }

  private def apply[T <: AnyRef](
      cbo: Cbo[T],
      keys: PathKeySet,
      height: Int,
      node: CboPath.PathNode[T]): CboPath[T] = {
    CboPathImpl(cbo, keys, height, node)
  }

  // Returns none if children doesn't share at least one path key.
  def apply[T <: AnyRef](
      cbo: Cbo[T],
      node: CboNode[T],
      children: Seq[CboPath[T]]): Option[CboPath[T]] = {
    assert(children.forall(_.cbo() eq cbo))

    val newKeysUnsafe = children.map(_.keys().keys()).reduce[Set[PathKey]] {
      case (one, other) =>
        one.intersect(other)
    }
    if (newKeysUnsafe.isEmpty) {
      return None
    }
    val newKeys = PathKeySet(newKeysUnsafe)
    Some(
      CboPath(
        cbo,
        newKeys,
        1 + children.map(_.height()).reduceOption(_ max _).getOrElse(0),
        PathNode(node, children.map(_.node()))))
  }

  def zero[T <: AnyRef](cbo: Cbo[T], keys: PathKeySet, group: GroupNode[T]): CboPath[T] = {
    CboPath(cbo, keys, 0, PathNode(group, List.empty))
  }

  def one[T <: AnyRef](
      cbo: Cbo[T],
      keys: PathKeySet,
      allGroups: Int => CboGroup[T],
      canonical: CanonicalNode[T]): CboPath[T] = {
    CboPath(
      cbo,
      keys,
      1,
      PathNode(canonical, canonical.getChildrenGroups(allGroups).map(g => PathNode(g, List.empty))))
  }

  // Aggregates paths that have same shape but different keys together.
  // Currently not in use because of bad performance.
  def aggregate[T <: AnyRef](cbo: Cbo[T], paths: Iterable[CboPath[T]]): Iterable[CboPath[T]] = {
    // Scala has specialized optimization against small set of input of group-by.
    // So it's better only to pass small inputs to this method if possible.
    val grouped = paths.groupBy(_.node())
    grouped.map {
      case (node, paths) =>
        val heights = paths.map(_.height()).toSeq.distinct
        assert(heights.size == 1)
        val height = heights.head
        val keys = paths.map(_.keys().keys()).reduce[Set[PathKey]] {
          case (one, other) =>
            one.union(other)
        }
        CboPath(cbo, PathKeySet(keys), height, node)
    }
  }

  def cartesianProduct[T <: AnyRef](
      cbo: Cbo[T],
      canonical: CanonicalNode[T],
      children: Seq[Iterable[CboPath[T]]]): Iterable[CboPath[T]] = {
    // Apply cartesian product across all children to get an enumeration
    // of all possible choices of parent and children.
    //
    // Example:
    //
    // Root:
    //   n0(group1, group2)
    // Children Input:
    //   (group1, group2)
    // = ([n1 || n2], [n3 || n4 || n5])
    // = ([p1 || p2.1 || p2.2], [p3 || p4 || p5]) (expanded)
    // Children Output:
    //   [(p1, p3), (p1, p4), (p1, p5), (p2.1, p3), (p2.1, p4), (p2.1, p5),
    //    (p2.2, p3), (p2.2, p4), (p2.2, p5))] (choices)
    // Path enumerated:
    //   [p0.1(p1, p3), p0.2(p1, p4), p0.3(p1, p5), p0.4(p2.1, p3), p0.5(p2.1, p4),
    //    p0.6(p2.1, p5), p0.7(p2.2, p3), p0.8(p2.2, p4), p0.9(p2.2, p5)] (output)
    //
    // TODO: Make inner builder list mutable to reduce memory usage
    val choicesBuilder: Iterable[Seq[CboPath[T]]] = List(List.empty)
    val choices: Iterable[Seq[CboPath[T]]] = children
      .foldLeft(choicesBuilder) {
        (choicesBuilder: Iterable[Seq[CboPath[T]]], child: Iterable[CboPath[T]]) =>
          for (left <- choicesBuilder; right <- child) yield left :+ right
      }

    choices.flatMap { children: Seq[CboPath[T]] => CboPath(cbo, canonical, children) }
  }

  implicit class CboPathImplicits[T <: AnyRef](path: CboPath[T]) {
    def dive(memoStore: MemoStore[T], extraDepth: Int): Iterable[CboPath[T]] = {
      val accumulatedDepth = extraDepth match {
        case CboPath.INF_DEPTH => CboPath.INF_DEPTH
        case _ =>
          Math.addExact(path.height(), extraDepth)
      }

      val finder = PathFinder
        .builder(path.cbo(), memoStore)
        .depth(accumulatedDepth)
        .build()
      finder.find(path)
    }
  }

  private case class PathNodeImpl[T <: AnyRef](
      override val self: CboNode[T],
      override val children: Seq[PathNode[T]])
    extends PathNode[T]

  private case class CboPathImpl[T <: AnyRef](
      override val cbo: Cbo[T],
      override val keys: PathKeySet,
      override val height: Int,
      override val node: CboPath.PathNode[T])
    extends CboPath[T] {
    assert(height >= 0)
    private lazy val built: T = {
      def dfs(node: CboPath.PathNode[T]): T = {
        cbo.withNewChildren(node.self().self(), node.children().map(c => dfs(c)))
      }
      dfs(node)
    }

    override def plan(): T = built
  }
}
