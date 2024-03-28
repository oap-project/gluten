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

import io.glutenproject.cbo.{CanonicalNode, Cbo, GroupNode}
import io.glutenproject.cbo.memo.MemoStore

import scala.collection.mutable

trait PathFinder[T <: AnyRef] {
  def find(base: CanonicalNode[T]): Iterable[CboPath[T]]
  def find(base: CboPath[T]): Iterable[CboPath[T]]
}

object PathFinder {
  def apply[T <: AnyRef](cbo: Cbo[T], memoStore: MemoStore[T]): PathFinder[T] = {
    builder(cbo, memoStore).build()
  }

  def builder[T <: AnyRef](cbo: Cbo[T], memoStore: MemoStore[T]): Builder[T] = {
    Builder[T](cbo, memoStore)
  }

  class Builder[T <: AnyRef] private (cbo: Cbo[T], memoStore: MemoStore[T]) {
    private val filterWizards = mutable.ListBuffer[FilterWizard[T]](FilterWizards.omitCycles())
    private val outputWizards = mutable.ListBuffer[OutputWizard[T]]()

    def depth(depth: Int): Builder[T] = {
      outputWizards += OutputWizards.withMaxDepth(depth)
      this
    }

    def filter(wizard: FilterWizard[T]): Builder[T] = {
      filterWizards += wizard
      this
    }

    def output(wizard: OutputWizard[T]): Builder[T] = {
      outputWizards += wizard
      this
    }

    def build(): PathFinder[T] = {
      if (outputWizards.isEmpty) {
        outputWizards += OutputWizards.emit()
      }
      val allOutputs = OutputWizards.union(outputWizards)
      val wizard = filterWizards.foldLeft(allOutputs) {
        (outputWizard, filterWizard) => outputWizard.filterBy(filterWizard)
      }
      PathEnumerator(cbo, memoStore, wizard)
    }
  }

  private object Builder {
    def apply[T <: AnyRef](cbo: Cbo[T], memoStore: MemoStore[T]): Builder[T] = {
      new Builder(cbo, memoStore)
    }
  }

  // Using children's enumerated paths recursively to enumerate the paths of the current node.
  // This works like from bottom up to assemble all possible paths.
  private class PathEnumerator[T <: AnyRef] private (
      cbo: Cbo[T],
      memoStore: MemoStore[T],
      wizard: OutputWizard[T])
    extends PathFinder[T] {

    override def find(canonical: CanonicalNode[T]): Iterable[CboPath[T]] = {
      val all =
        wizard.prepareForNode(cbo, memoStore.asGroupSupplier(), canonical).visit().onContinue {
          newWizard => enumerateFromNode(canonical, newWizard)
        }
      all
    }

    override def find(base: CboPath[T]): Iterable[CboPath[T]] = {
      val can = base.node().self().asCanonical()
      val all = wizard.prepareForNode(cbo, memoStore.asGroupSupplier(), can).visit().onContinue {
        newWizard => diveFromNode(base.height(), base.node(), newWizard)
      }
      all
    }

    private def enumerateFromGroup(
        group: GroupNode[T],
        wizard: OutputWizard[T]): Iterable[CboPath[T]] = {
      group
        .group(memoStore.asGroupSupplier())
        .nodes(memoStore)
        .flatMap(
          can => {
            wizard.prepareForNode(cbo, memoStore.asGroupSupplier(), can).visit().onContinue {
              newWizard => enumerateFromNode(can, newWizard)
            }
          })
    }

    private def enumerateFromNode(
        canonical: CanonicalNode[T],
        wizard: OutputWizard[T]): Iterable[CboPath[T]] = {
      val childrenGroups = canonical.getChildrenGroups(memoStore.asGroupSupplier())
      if (childrenGroups.isEmpty) {
        // It's a canonical leaf node.
        return List.empty
      }
      // It's a canonical branch node.
      val expandedChildren: Seq[Iterable[CboPath[T]]] =
        childrenGroups.zipWithIndex.map {
          case (childGroup, index) =>
            wizard
              .prepareForGroup(cbo, childGroup, index, childrenGroups.size)
              .advance()
              .onContinue(newWizard => enumerateFromGroup(childGroup, newWizard))
        }
      CboPath.cartesianProduct(cbo, canonical, expandedChildren)
    }

    private def diveFromGroup(
        depth: Int,
        gpn: GroupedPathNode[T],
        wizard: OutputWizard[T]): Iterable[CboPath[T]] = {
      assert(depth >= 0)
      if (depth == 0) {
        assert(gpn.node.self().isGroup)
        return enumerateFromGroup(gpn.group, wizard)
      }

      assert(gpn.node.self().isCanonical)
      val canonical = gpn.node.self().asCanonical()

      wizard.prepareForNode(cbo, memoStore.asGroupSupplier(), canonical).visit().onContinue {
        newWizard => diveFromNode(depth, gpn.node, newWizard)
      }
    }

    private def diveFromNode(
        depth: Int,
        node: CboPath.PathNode[T],
        wizard: OutputWizard[T]): Iterable[CboPath[T]] = {
      assert(depth >= 1)
      assert(node.self().isCanonical)
      val canonical = node.self().asCanonical()
      val children = node.children()
      if (children.isEmpty) {
        // It's a canonical leaf node.
        return List.empty
      }

      val childrenGroups = canonical.getChildrenGroups(memoStore.asGroupSupplier())
      CboPath.cartesianProduct(
        cbo,
        canonical,
        children.zip(childrenGroups).zipWithIndex.map {
          case ((child, childGroup), index) =>
            wizard
              .prepareForGroup(cbo, childGroup, index, childrenGroups.size)
              .advance()
              .onContinue {
                newWizard => diveFromGroup(depth - 1, GroupedPathNode(childGroup, child), newWizard)
              }
        }
      )
    }
  }

  private object PathEnumerator {
    def apply[T <: AnyRef](
        cbo: Cbo[T],
        memoStore: MemoStore[T],
        wizard: OutputWizard[T]): PathEnumerator[T] = {
      new PathEnumerator(cbo, memoStore, wizard)
    }
  }

  private case class GroupedPathNode[T <: AnyRef](group: GroupNode[T], node: CboPath.PathNode[T])
}
