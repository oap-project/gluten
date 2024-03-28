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
package org.apache.gluten.utils

import org.apache.spark.{InterruptibleIterator, TaskContext}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.TaskResources

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

private class PayloadCloser[A](in: Iterator[A])(closeCallback: A => Unit) extends Iterator[A] {
  private var closer: Option[() => Unit] = None

  TaskResources.addRecycler("Iterators#PayloadCloser", 100) {
    tryClose()
  }

  override def hasNext: Boolean = {
    tryClose()
    in.hasNext
  }

  override def next(): A = {
    val a: A = in.next()
    closer.synchronized {
      closer = Some(
        () => {
          closeCallback.apply(a)
        })
    }
    a
  }

  private def tryClose(): Unit = {
    closer.synchronized {
      closer match {
        case Some(c) => c.apply()
        case None =>
      }
      closer = None // make sure the payload is closed once
    }
  }
}

private class IteratorCompleter[A](in: Iterator[A])(completionCallback: => Unit)
  extends Iterator[A] {
  private val completed = new AtomicBoolean(false)

  TaskResources.addRecycler("Iterators#IteratorRecycler", 100) {
    tryComplete()
  }

  override def hasNext: Boolean = {
    val out = in.hasNext
    if (!out) {
      tryComplete()
    }
    out
  }

  override def next(): A = {
    in.next()
  }

  private def tryComplete(): Unit = {
    if (!completed.compareAndSet(false, true)) {
      return // make sure the iterator is completed once
    }
    completionCallback
  }
}

private class PipelineTimeAccumulator[A](in: Iterator[A], pipelineTime: SQLMetric)
  extends Iterator[A] {
  private val closed = new AtomicBoolean(false)
  private val startTime = System.nanoTime()

  TaskResources.addRecycler("Iterators#PipelineTimeAccumulator", 100) {
    tryFinish()
  }

  override def hasNext: Boolean = {
    val out = in.hasNext
    if (!out) {
      tryFinish()
    }
    out
  }

  override def next(): A = {
    in.next()
  }

  private def tryFinish(): Unit = {
    // pipeline metric should only be calculate once.
    if (!closed.compareAndSet(false, true)) {
      return
    }
    pipelineTime += TimeUnit.NANOSECONDS.toMillis(
      System.nanoTime() - startTime
    )
  }
}

/**
 * To protect the wrapped iterator to avoid undesired order of calls to its `hasNext` and `next`
 * methods.
 */
private class InvocationFlowProtection[A](in: Iterator[A]) extends Iterator[A] {
  sealed private trait State
  private case object Init extends State
  private case class HasNextCalled(hasNext: Boolean) extends State
  private case object NextCalled extends State

  private var state: State = Init

  override def hasNext: Boolean = {
    val out = state match {
      case Init | NextCalled =>
        in.hasNext
      case HasNextCalled(lastHasNext) =>
        lastHasNext
    }
    state = HasNextCalled(out)
    out
  }

  override def next(): A = {
    val out = state match {
      case Init | NextCalled =>
        if (!in.hasNext) {
          throw new IllegalStateException("End of stream")
        }
        in.next()
      case HasNextCalled(lastHasNext) =>
        if (!lastHasNext) {
          throw new IllegalStateException("End of stream")
        }
        in.next()
    }
    state = NextCalled
    out
  }
}

class WrapperBuilder[A](in: Iterator[A]) { // FIXME how to make the ctor companion-private?
  private var wrapped: Iterator[A] = in

  def recyclePayload(closeCallback: (A) => Unit): WrapperBuilder[A] = {
    wrapped = new PayloadCloser(wrapped)(closeCallback)
    this
  }

  def recycleIterator(completionCallback: => Unit): WrapperBuilder[A] = {
    wrapped = new IteratorCompleter(wrapped)(completionCallback)
    this
  }

  def addToPipelineTime(pipelineTime: SQLMetric): WrapperBuilder[A] = {
    wrapped = new PipelineTimeAccumulator[A](wrapped, pipelineTime)
    this
  }

  def asInterruptible(context: TaskContext): WrapperBuilder[A] = {
    wrapped = new InterruptibleIterator[A](context, wrapped)
    this
  }

  def protectInvocationFlow(): WrapperBuilder[A] = {
    wrapped = new InvocationFlowProtection[A](wrapped)
    this
  }

  def create(): Iterator[A] = {
    wrapped
  }
}

/**
 * Utility class to provide iterator wrappers for non-trivial use cases. E.g. iterators that manage
 * payload's lifecycle.
 */
object Iterators {
  def wrap[A](in: Iterator[A]): WrapperBuilder[A] = {
    new WrapperBuilder[A](in)
  }
}
