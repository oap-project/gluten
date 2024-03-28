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

import io.glutenproject.cbo.{CanonicalNode, Cbo, CboGroup, GroupNode}
import io.glutenproject.cbo.path.OutputWizard.{OutputAction, PathDrain}

import scala.collection.{mutable, Seq}

trait OutputWizard[T <: AnyRef] {
  import OutputWizard._
  // Visit a new node.
  def visit(can: CanonicalNode[T]): OutputAction[T]
  // The returned object is a wizard for one of the node's children at the
  // known offset among all children.
  def advance(group: GroupNode[T], offset: Int, count: Int): OutputAction[T]
  // The returned wizard would be same with this wizard
  // except it drains paths with the input path key.
  def withPathKey(newKey: PathKey): OutputWizard[T]
}

object OutputWizard {
  sealed trait OutputAction[T <: AnyRef] {
    def drain(): PathDrain
  }
  object OutputAction {
    case class Stop[T <: AnyRef] private (override val drain: PathDrain) extends OutputAction[T]
    object Stop {
      val INSTANCE: Stop[Null] = Stop[Null]()
      // Enclose default constructor.
      private def apply[T <: AnyRef](): Stop[T] = new Stop(PathDrain.none)
    }
    def stop[T <: AnyRef]: Stop[T] = Stop.INSTANCE.asInstanceOf[Stop[T]]

    case class Continue[T <: AnyRef](override val drain: PathDrain, newWizard: OutputWizard[T])
      extends OutputAction[T]
  }

  // Path drain provides possibility to lazily materialize the yielded paths using path key.
  // Otherwise if each wizard emits its own paths during visiting, the de-dup operation
  // will be required and could cause serious performance issues.
  sealed trait PathDrain {
    def isEmpty(): Boolean
    def keysUnsafe(): Seq[PathKey]
  }

  object PathDrain {
    private case class None[T <: AnyRef] private () extends PathDrain {
      override def isEmpty(): Boolean = true
      override def keysUnsafe(): Seq[PathKey] = List.empty
    }
    private object None {
      val INSTANCE: None[Null] = None[Null]()
      private def apply[T <: AnyRef](): None[T] = new None[T]()
    }
    def none[T <: AnyRef]: PathDrain = None.INSTANCE.asInstanceOf[None[T]]
    case class Specific[T <: AnyRef](override val keysUnsafe: Seq[PathKey]) extends PathDrain {
      override def isEmpty(): Boolean = keysUnsafe.isEmpty
    }
    private case class Trivial[T <: AnyRef] private () extends PathDrain {
      private val k: Seq[PathKey] = List(PathKey.Trivial)
      override def isEmpty(): Boolean = k.isEmpty
      override def keysUnsafe(): Seq[PathKey] = k
    }
    private object Trivial {
      val INSTANCE: Trivial[Null] = Trivial[Null]()
      private def apply[T <: AnyRef](): Trivial[T] = new Trivial[T]()
    }
    def trivial[T <: AnyRef]: PathDrain = Trivial.INSTANCE.asInstanceOf[Trivial[T]]
  }

  implicit class OutputWizardImplicits[T <: AnyRef](wizard: OutputWizard[T]) {
    import OutputWizardImplicits._

    def filterBy(filterWizard: FilterWizard[T]): OutputWizard[T] = {
      OutputFilter(wizard, filterWizard)
    }

    def prepareForNode(
        cbo: Cbo[T],
        allGroups: Int => CboGroup[T],
        can: CanonicalNode[T]): NodePrepare[T] = {
      new NodePrepareImpl[T](cbo, wizard, allGroups, can)
    }

    def prepareForGroup(
        cbo: Cbo[T],
        group: GroupNode[T],
        offset: Int,
        count: Int): GroupPrepare[T] = {
      new GroupPrepareImpl[T](cbo, wizard, group, offset, count)
    }
  }

  object OutputWizardImplicits {
    sealed trait NodePrepare[T <: AnyRef] {
      def visit(): Terminate[T]
    }

    sealed trait GroupPrepare[T <: AnyRef] {
      def advance(): Terminate[T]
    }

    sealed trait Terminate[T <: AnyRef] {
      def onContinue(extra: OutputWizard[T] => Iterable[CboPath[T]]): Iterable[CboPath[T]]
    }

    private class DrainedTerminate[T <: AnyRef](
        action: OutputAction[T],
        drained: Iterable[CboPath[T]])
      extends Terminate[T] {
      override def onContinue(
          extra: OutputWizard[T] => Iterable[CboPath[T]]): Iterable[CboPath[T]] = {
        action match {
          case OutputAction.Stop(_) =>
            drained.view
          case OutputAction.Continue(_, newWizard) =>
            drained.view ++ extra(newWizard)
        }
      }
    }

    private class NodePrepareImpl[T <: AnyRef](
        cbo: Cbo[T],
        wizard: OutputWizard[T],
        allGroups: Int => CboGroup[T],
        can: CanonicalNode[T])
      extends NodePrepare[T] {
      override def visit(): Terminate[T] = {
        val action = wizard.visit(can)
        val drained = if (action.drain().isEmpty()) {
          List.empty
        } else {
          List(CboPath.one(cbo, PathKeySet(action.drain().keysUnsafe().toSet), allGroups, can))
        }
        new DrainedTerminate[T](action, drained)
      }
    }

    private class GroupPrepareImpl[T <: AnyRef](
        cbo: Cbo[T],
        wizard: OutputWizard[T],
        group: GroupNode[T],
        offset: Int,
        count: Int)
      extends GroupPrepare[T] {
      override def advance(): Terminate[T] = {
        val action = wizard.advance(group, offset, count)
        val drained = if (action.drain().isEmpty()) {
          List.empty
        } else {
          List(CboPath.zero(cbo, PathKeySet(action.drain().keysUnsafe().toSet), group))
        }
        new DrainedTerminate[T](action, drained)
      }
    }
  }
}

object OutputWizards {
  def none[T <: AnyRef](): OutputWizard[T] = {
    None()
  }

  def emit[T <: AnyRef](): OutputWizard[T] = {
    Emit()
  }

  def union[T <: AnyRef](wizards: Seq[OutputWizard[T]]): OutputWizard[T] = {
    Union[T](wizards)
  }

  def withMask[T <: AnyRef](mask: PathMask): OutputWizard[T] = {
    WithMask[T](mask, 0)
  }

  def withPattern[T <: AnyRef](pattern: Pattern[T]): OutputWizard[T] = {
    WithPattern[T](pattern)
  }

  def withMaxDepth[T <: AnyRef](depth: Int): OutputWizard[T] = {
    WithMaxDepth[T](depth)
  }

  private class None[T <: AnyRef]() extends OutputWizard[T] {
    override def visit(can: CanonicalNode[T]): OutputAction[T] = {
      OutputAction.Stop(PathDrain.none)
    }

    override def advance(group: GroupNode[T], offset: Int, count: Int): OutputAction[T] =
      OutputAction.Stop(PathDrain.none)

    override def withPathKey(newKey: PathKey): OutputWizard[T] = this
  }

  private object None {
    def apply[T <: AnyRef](): None[T] = new None[T]()
  }

  private class Emit[T <: AnyRef](drain: PathDrain) extends OutputWizard[T] {
    override def visit(can: CanonicalNode[T]): OutputAction[T] = {
      if (can.isLeaf()) {
        return OutputAction.Stop(drain)
      }
      OutputAction.Continue(PathDrain.none, this)
    }

    override def advance(group: GroupNode[T], offset: Int, count: Int): OutputAction[T] =
      OutputAction.Continue(PathDrain.none, this)

    override def withPathKey(newKey: PathKey): OutputWizard[T] =
      new Emit[T](PathDrain.Specific(List(newKey)))
  }

  private object Emit {
    def apply[T <: AnyRef](): Emit[T] = new Emit[T](PathDrain.trivial)
  }

  // Composite wizard works within "or" basis, which means,
  // when one of the sub-wizards yield "continue",
  // then itself yields continue.
  private class Union[T <: AnyRef] private (wizards: Seq[OutputWizard[T]]) extends OutputWizard[T] {
    import Union._
    assert(wizards.nonEmpty)

    private def act(actions: Seq[OutputAction[T]]): OutputAction[T] = {
      val drainBuffer = mutable.ListBuffer[PathDrain]()
      val newWizardBuffer = mutable.ListBuffer[OutputWizard[T]]()

      val state: State = actions.foldLeft[State](ContinueNotFound) {
        case (_, OutputAction.Continue(drain, newWizard)) =>
          drainBuffer += drain
          newWizardBuffer += newWizard
          ContinueFound
        case (s, OutputAction.Stop(drain)) =>
          drainBuffer += drain
          s
      }

      val newWizards = newWizardBuffer
      val newDrain = PathDrain.Specific(drainBuffer.flatMap(_.keysUnsafe()))
      state match {
        // All omits.
        case ContinueNotFound => OutputAction.Stop(newDrain)
        // At least one continue.
        case ContinueFound => OutputAction.Continue(newDrain, new Union(newWizards))
      }
    }

    override def visit(can: CanonicalNode[T]): OutputAction[T] = {
      val actions = wizards
        .map(_.visit(can))
      act(actions)
    }

    override def advance(group: GroupNode[T], offset: Int, count: Int): OutputAction[T] = {
      val actions = wizards
        .map(_.advance(group, offset, count))
      act(actions)
    }

    override def withPathKey(newKey: PathKey): OutputWizard[T] =
      new Union[T](wizards.map(w => w.withPathKey(newKey)))
  }

  private object Union {
    def apply[T <: AnyRef](wizards: Seq[OutputWizard[T]]): Union[T] = {
      new Union(wizards)
    }

    sealed private trait State
    private case object ContinueNotFound extends State
    private case object ContinueFound extends State
  }

  // Prune paths within the path mask.
  //
  // Example:
  //
  // The Tree:
  //
  // A
  // |- B
  // |- C
  //    |- D
  //    \- E
  // \- F
  //
  // Mask 1:
  //  [3, 0, 0, 0]
  //
  // Mask 1 output:
  //
  // A
  // |- B
  // |- C
  // \- F
  //
  // Mask 2:
  //  [3, 0, 2, 0, 0, 0]
  //
  // Mask 2 output:
  //
  // A
  // |- B
  // |- C
  //    |- D
  //    \- E
  // \- F
  private class WithMask[T <: AnyRef] private (drain: PathDrain, mask: PathMask, ele: Int)
    extends OutputWizard[T] {

    override def visit(can: CanonicalNode[T]): OutputAction[T] = {
      if (can.isLeaf()) {
        return OutputAction.Stop(drain)
      }
      OutputAction.Continue(PathDrain.none, this)
    }

    override def advance(group: GroupNode[T], offset: Int, count: Int): OutputAction[T] = {
      var skipCursor = ele + 1
      (0 until offset).foreach(_ => skipCursor = mask.skip(skipCursor))
      if (mask.isAny(skipCursor)) {
        return OutputAction.Stop(drain)
      }
      OutputAction.Continue(PathDrain.none, new WithMask[T](drain, mask, skipCursor))
    }

    override def withPathKey(newKey: PathKey): OutputWizard[T] =
      new WithMask[T](PathDrain.Specific(List(newKey)), mask, ele)
  }

  private object WithMask {
    def apply[T <: AnyRef](mask: PathMask, cursor: Int): WithMask[T] = {
      new WithMask(PathDrain.Specific(List(PathKey.random())), mask, cursor)
    }
  }

  // TODO: Document
  private class WithPattern[T <: AnyRef] private (
      drain: PathDrain,
      pattern: Pattern[T],
      pNode: Pattern.Node[T])
    extends OutputWizard[T] {

    override def visit(can: CanonicalNode[T]): OutputAction[T] = {
      // Prune should be done in #advance.
      assert(!pNode.skip())
      if (pNode.abort(can)) {
        return OutputAction.stop
      }
      if (!pNode.matches(can)) {
        return OutputAction.stop
      }
      if (can.isLeaf()) {
        return OutputAction.Stop(drain)
      }
      OutputAction.Continue(PathDrain.none, this)
    }

    override def advance(group: GroupNode[T], offset: Int, count: Int): OutputAction[T] = {
      // Omit should be done in #advance.
      val child = pNode.children(count)(offset)
      if (child.skip()) {
        return OutputAction.Stop(drain)
      }
      OutputAction.Continue(PathDrain.none, new WithPattern(drain, pattern, child))
    }

    override def withPathKey(newKey: PathKey): OutputWizard[T] =
      new WithPattern[T](PathDrain.Specific(List(newKey)), pattern, pNode)
  }

  private object WithPattern {
    def apply[T <: AnyRef](pattern: Pattern[T]): WithPattern[T] = {
      new WithPattern(PathDrain.Specific(List(PathKey.random())), pattern, pattern.root())
    }
  }

  // "Depth" is similar to path's "height" but it mainly describes about the
  // distance between pathfinder from the root node.
  private class WithMaxDepth[T <: AnyRef] private (drain: PathDrain, depth: Int, currentDepth: Int)
    extends OutputWizard[T] {

    override def visit(can: CanonicalNode[T]): OutputAction[T] = {
      assert(
        currentDepth <= depth,
        "Current depth already larger than the maximum depth to prune. " +
          "It probably because a zero depth was specified for path finding."
      )
      if (can.isLeaf()) {
        return OutputAction.Stop(drain)
      }
      OutputAction.Continue(PathDrain.none, this)
    }

    override def advance(group: GroupNode[T], offset: Int, count: Int): OutputAction[T] = {
      assert(currentDepth <= depth)
      val nextDepth = currentDepth + 1
      if (nextDepth > depth) {
        return OutputAction.Stop(drain)
      }
      OutputAction.Continue(PathDrain.none, new WithMaxDepth(drain, depth, nextDepth))
    }

    override def withPathKey(newKey: PathKey): OutputWizard[T] =
      new WithMaxDepth[T](PathDrain.Specific(List(newKey)), depth, currentDepth)
  }

  private object WithMaxDepth {
    def apply[T <: AnyRef](depth: Int): WithMaxDepth[T] = {
      new WithMaxDepth(PathDrain.Specific(List(PathKey.random())), depth, 1)
    }
  }
}
