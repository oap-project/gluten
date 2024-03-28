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

import io.glutenproject.cbo.{CanonicalNode, GroupNode}
import io.glutenproject.cbo.path.FilterWizard.FilterAction
import io.glutenproject.cbo.path.OutputWizard.OutputAction
import io.glutenproject.cbo.util.CycleDetector

trait FilterWizard[T <: AnyRef] {
  import FilterWizard._
  def omit(can: CanonicalNode[T]): FilterAction[T]
  def omit(group: GroupNode[T], offset: Int, count: Int): FilterAction[T]
}

object FilterWizard {
  sealed trait FilterAction[T <: AnyRef]
  object FilterAction {
    case class Omit[T <: AnyRef] private () extends FilterAction[T]
    object Omit {
      val INSTANCE: Omit[Null] = Omit[Null]()
      // Enclose default constructor.
      private def apply[T <: AnyRef](): Omit[T] = new Omit()
    }
    def omit[T <: AnyRef]: Omit[T] = Omit.INSTANCE.asInstanceOf[Omit[T]]

    case class Continue[T <: AnyRef](newWizard: FilterWizard[T]) extends FilterAction[T]
  }
}

object FilterWizards {
  def omitCycles[T <: AnyRef](): FilterWizard[T] = {
    // Compares against group ID to identify cycles.
    OmitCycles[T](CycleDetector[GroupNode[T]]((one, other) => one.groupId() == other.groupId()))
  }

  // Cycle detection starts from the first visited group in the input path.
  private class OmitCycles[T <: AnyRef] private (detector: CycleDetector[GroupNode[T]])
    extends FilterWizard[T] {
    override def omit(can: CanonicalNode[T]): FilterAction[T] = {
      FilterAction.Continue(this)
    }

    override def omit(group: GroupNode[T], offset: Int, count: Int): FilterAction[T] = {
      if (detector.contains(group)) {
        return FilterAction.omit
      }
      FilterAction.Continue(new OmitCycles(detector.append(group)))
    }
  }

  private object OmitCycles {
    def apply[T <: AnyRef](detector: CycleDetector[GroupNode[T]]): OmitCycles[T] = {
      new OmitCycles(detector)
    }
  }
}

object OutputFilter {
  def apply[T <: AnyRef](
      outputWizard: OutputWizard[T],
      filterWizard: FilterWizard[T]): OutputWizard[T] = {
    new OutputFilterImpl[T](outputWizard, filterWizard)
  }

  // Composite wizard works within "and" basis, to filter out
  // the unwanted emitted paths from a certain specified output wizard
  // by another filter wizard.
  private class OutputFilterImpl[T <: AnyRef](
      outputWizard: OutputWizard[T],
      filterWizard: FilterWizard[T])
    extends OutputWizard[T] {

    override def visit(can: CanonicalNode[T]): OutputAction[T] = {
      filterWizard.omit(can) match {
        case FilterAction.Omit() => OutputAction.stop
        case FilterAction.Continue(newFilterWizard) =>
          outputWizard.visit(can) match {
            case stop @ OutputAction.Stop(_) =>
              stop
            case OutputAction.Continue(drain, newOutputWizard) =>
              OutputAction.Continue(drain, new OutputFilterImpl(newOutputWizard, newFilterWizard))
          }
      }
    }

    override def advance(group: GroupNode[T], offset: Int, count: Int): OutputAction[T] = {
      filterWizard.omit(group: GroupNode[T], offset: Int, count: Int) match {
        case FilterAction.Omit() => OutputAction.stop
        case FilterAction.Continue(newFilterWizard) =>
          outputWizard.advance(group, offset, count) match {
            case stop @ OutputAction.Stop(_) => stop
            case OutputAction.Continue(drain, newOutputWizard) =>
              OutputAction.Continue(drain, new OutputFilterImpl(newOutputWizard, newFilterWizard))
          }
      }
    }

    override def withPathKey(newKey: PathKey): OutputWizard[T] =
      new OutputFilterImpl[T](outputWizard.withPathKey(newKey), filterWizard)
  }
}
