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

import scala.collection.mutable

// Mask is an integer array (pre-order DFS).
//
// FIXME: This is not currently in use. Use pattern instead.
//  We may consider open up some API based on this once pattern-match's
//  performance doesn't meet expectations.
trait PathMask {
  import PathMask._
  def get(index: Int): Digit
  def all(): Seq[Digit]
}

object PathMask {
  type Digit = Int
  val ANY: Int = -1

  def apply(mask: Seq[Digit]): PathMask = {
    // Validate the mask.
    validate(mask)
    PathMaskImpl(mask)
  }

  private def validate(mask: Seq[Digit]): Unit = {
    // FIXME: This is a rough validation.
    val m = mask
    assert(m.forall(digit => digit == ANY || digit >= 0))
    assert(m.size == m.map(_.max(0)).sum + 1)
  }

  def union(masks: Seq[PathMask]): PathMask = {
    unionUnsafe(masks).get
  }

  // Union two masks. Any mask satisfies one of the
  // input masks would satisfy the output mask too.
  //
  // Return None if not union-able.
  private def unionUnsafe(masks: Seq[PathMask]): Option[PathMask] = {
    assert(masks.nonEmpty)
    val out = masks.reduce[PathMask] {
      case (left: PathMask, right: PathMask) =>
        val buffer = mutable.ArrayBuffer[Digit]()

        def dfs(depth: Int, lCursor: Int, rCursor: Int): Boolean = {
          // lcc: left children count
          // rcc: right children count
          val lcc = left.get(lCursor)
          val rcc = right.get(rCursor)

          if (lcc == ANY || rcc == ANY) {
            buffer += ANY
            return true
          }

          if (lcc != rcc) {
            return false
          }

          // cc: children count
          val cc = lcc
          buffer += cc

          var lChildCursor = lCursor + 1
          var rChildCursor = rCursor + 1
          (0 until cc).foreach {
            _ =>
              if (!dfs(depth + 1, lChildCursor, rChildCursor)) {
                return false
              }
              lChildCursor = left.skip(lChildCursor)
              rChildCursor = right.skip(rChildCursor)
          }
          true
        }

        if (!dfs(0, 0, 0)) {
          return None
        }

        PathMask(buffer)
    }

    Some(out)
  }

  private case class PathMaskImpl(mask: Seq[Digit]) extends PathMask {
    override def get(index: Int): Digit = mask(index)

    override def all(): Seq[Digit] = mask
  }

  implicit class PathMaskImplicits(mask: PathMask) {
    def isAny(index: Int): Boolean = {
      mask.get(index) == ANY
    }

    // Input is the index of node element of skip, then returns the
    // index of it's next brother node element. If no remaining brothers,
    // returns parent's next brother's index recursively in pre-order.
    def skip(ele: Int): Int = {
      val childrenCount = mask.get(ele)
      if (childrenCount == ANY) {
        return ele + 1
      }
      var accumulatedSkips = childrenCount + 1
      var skipCursor = ele

      def loop(): Unit = {
        while (true) {
          skipCursor += 1
          accumulatedSkips -= 1
          if (accumulatedSkips == 0) {
            return
          }
          accumulatedSkips += (if (mask.isAny(skipCursor)) 0 else mask.get(skipCursor))
        }
      }

      loop()
      assert(accumulatedSkips == 0)
      skipCursor
    }

    // Truncate the path within a fixed maximum depth.
    // The nodes deeper than the depth will be normalized
    // into its ancestor at the depth with children count value '0'.
    def fold(maxDepth: Int): PathMask = {
      val buffer = mutable.ArrayBuffer[Digit]()

      def dfs(depth: Int, cursor: Int): Unit = {
        if (depth == maxDepth) {
          buffer += ANY
          return
        }
        assert(depth < maxDepth)
        if (mask.isAny(cursor)) {
          buffer += ANY
          return
        }
        val childrenCount = mask.get(cursor)
        buffer += childrenCount
        var childCursor = cursor + 1
        (0 until childrenCount).foreach {
          _ =>
            dfs(depth + 1, childCursor)
            childCursor = mask.skip(childCursor)
        }
      }

      dfs(0, 0)

      PathMask(buffer)
    }

    // Return the sub-mask whose root node is the node at the input index
    // of this mask.
    def subMaskAt(index: Int): PathMask = {
      PathMask(mask.all().slice(index, mask.skip(index)))
    }

    // Tests if this mask satisfies another.
    //
    // Term 'satisfy' here means if a path is not omitted (though it can be pruned)
    // by this mask, the the output of pruning procedure must not be omitted by the
    // 'other' mask.
    def satisfies(other: PathMask): Boolean = {
      unionUnsafe(List(mask, other)) match {
        case Some(union) if union == other => true
        case _ => false
      }
    }
  }
}
