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

import io.glutenproject.cbo.CanonicalNode
import io.glutenproject.cbo.path.CboPath.PathNode

trait Pattern[T <: AnyRef] {
  def matches(path: CboPath[T], depth: Int): Boolean
  def root(): Pattern.Node[T]
}

object Pattern {
  trait Matcher[T <: AnyRef] extends (T => Boolean)

  trait Node[T <: AnyRef] {
    // If abort returns true, caller should make sure not to call further methods.
    // It provides a way to fast fail the matching before actually jumping
    // in to #matches call.
    def skip(): Boolean
    def abort(node: CanonicalNode[T]): Boolean
    def matches(node: CanonicalNode[T]): Boolean
    def children(count: Int): Seq[Node[T]]
  }

  private case class Any[T <: AnyRef]() extends Node[Null] {
    override def skip(): Boolean = false
    override def abort(node: CanonicalNode[Null]): Boolean = false
    override def matches(node: CanonicalNode[Null]): Boolean = true
    override def children(count: Int): Seq[Node[Null]] = (0 until count).map(_ => ignore[Null])
  }

  private object Any {
    val INSTANCE: Any[Null] = Any[Null]()
    // Enclose default constructor.
    private def apply[T <: AnyRef](): Any[T] = new Any()
  }

  private case class Ignore[T <: AnyRef]() extends Node[Null] {
    override def skip(): Boolean = true
    override def abort(node: CanonicalNode[Null]): Boolean = false
    override def matches(node: CanonicalNode[Null]): Boolean =
      throw new UnsupportedOperationException()
    override def children(count: Int): Seq[Node[Null]] = throw new UnsupportedOperationException()
  }

  private object Ignore {
    val INSTANCE: Ignore[Null] = Ignore[Null]()

    // Enclose default constructor.
    private def apply[T <: AnyRef](): Ignore[T] = new Ignore()
  }

  private case class Branch[T <: AnyRef](matcher: Matcher[T], children: Seq[Node[T]])
    extends Node[T] {
    override def skip(): Boolean = false
    override def abort(node: CanonicalNode[T]): Boolean = node.childrenCount != children.size
    override def matches(node: CanonicalNode[T]): Boolean = matcher(node.self())
    override def children(count: Int): Seq[Node[T]] = {
      assert(count == children.size)
      children
    }
  }

  def any[T <: AnyRef]: Node[T] = Any.INSTANCE.asInstanceOf[Node[T]]
  def ignore[T <: AnyRef]: Node[T] = Ignore.INSTANCE.asInstanceOf[Node[T]]
  def node[T <: AnyRef](matcher: Matcher[T], children: Node[T]*): Node[T] =
    Branch(matcher, children.toSeq)
  def leaf[T <: AnyRef](matcher: Matcher[T]): Node[T] = Branch(matcher, List.empty)

  implicit class NodeImplicits[T <: AnyRef](node: Node[T]) {
    def build(): Pattern[T] = {
      PatternImpl(node)
    }
  }

  private case class PatternImpl[T <: AnyRef](root: Node[T]) extends Pattern[T] {
    override def matches(path: CboPath[T], depth: Int): Boolean = {
      assert(depth >= 1)
      assert(depth <= path.height())
      def dfs(remainingDepth: Int, patternN: Node[T], n: PathNode[T]): Boolean = {
        assert(remainingDepth >= 0)
        assert(n.self().isCanonical)
        if (remainingDepth == 0) {
          return true
        }
        val can = n.self().asCanonical()
        if (patternN.abort(can)) {
          return false
        }
        if (patternN.skip()) {
          return true
        }
        if (!patternN.matches(can)) {
          return false
        }
        // Pattern matches the current node.
        val nc = n.children()
        val patternNc = patternN.children(nc.size)
        assert(
          patternNc.size == nc.size,
          "A node in pattern doesn't match the node in input path's children size. " +
            "This might because the input path is not inferred by this pattern. " +
            "It's currently not a valid use case by design."
        )
        if (
          patternNc.zip(nc).exists {
            case (cPatternN, cN) =>
              !dfs(remainingDepth - 1, cPatternN, cN)
          }
        ) {
          return false
        }
        true
      }
      dfs(depth, root, path.node())
    }
  }
}
