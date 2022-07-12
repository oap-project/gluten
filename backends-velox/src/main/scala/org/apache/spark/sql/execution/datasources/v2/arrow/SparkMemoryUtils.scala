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

package org.apache.spark.sql.execution.datasources.v2.arrow

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import io.glutenproject.memory._
import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.TaskCompletionListener

object SparkMemoryUtils extends Logging {

  private val DEBUG: Boolean = {
    SQLConf.get
      .getConfString("spark.gluten.sql.memory.debug", "false").toBoolean
  }
  private val ACCUMULATED_LEAK_BYTES = new AtomicLong(0L)

  class TaskMemoryResources {
    if (!inSparkTask()) {
      throw new IllegalStateException("Creating TaskMemoryResources instance out of Spark task")
    }

    val sharedMetrics = new NativeSQLMemoryMetrics()

    val arrowAllocListener = new SparkManagedAllocationListener(
      new NativeSQLMemoryConsumer(getTaskMemoryManager(), Spiller.NO_OP),
      sharedMetrics)

    val arrowAllocListenerUnmanaged: AllocationListener = AllocationListener.NOOP

    private val arrowAllocators = new java.util.ArrayList[BufferAllocator]()

    private val nativeAllocators = new java.util.ArrayList[NativeMemoryAllocator]()

    val taskDefaultArrowAllocator: BufferAllocator = {
      val alloc = new RootAllocator(arrowAllocListener, Long.MaxValue)
      arrowAllocators.add(alloc)
      alloc
    }

    val taskDefaultArrowAllocatorUnmanaged: BufferAllocator = taskDefaultArrowAllocator
      .newChildAllocator("CHILD-ARROW-ALLOC-UNMANAGED", arrowAllocListenerUnmanaged, 0L,
        Long.MaxValue)

    val taskDefaultNativeAllocator: NativeMemoryAllocator = {
      val rl = new SparkManagedReservationListener(
        new NativeSQLMemoryConsumer(getTaskMemoryManager(), Spiller.NO_OP),
        sharedMetrics)
      val alloc = NativeMemoryAllocator.createListenable(rl)
      nativeAllocators.add(alloc)
      alloc
    }

    def createSpillableNativeAllocator(spiller: Spiller): NativeMemoryAllocator = {
      val rl = new SparkManagedReservationListener(
        new NativeSQLMemoryConsumer(getTaskMemoryManager(), spiller),
        sharedMetrics)
      val alloc = NativeMemoryAllocator.createListenable(rl)
      nativeAllocators.add(alloc)
      alloc
    }

    def createSpillableArrowAllocator(spiller: Spiller): BufferAllocator = {
      val al = new SparkManagedAllocationListener(
        new NativeSQLMemoryConsumer(getTaskMemoryManager(), spiller),
        sharedMetrics)
      val parent = taskDefaultArrowAllocator
      val alloc = parent.newChildAllocator("Spark Managed Allocator - " +
        UUID.randomUUID().toString, al, 0, parent.getLimit).asInstanceOf[BufferAllocator]
      arrowAllocators.add(alloc)
      alloc
    }

    private def close(allocator: BufferAllocator): Unit = {
      allocator.getChildAllocators.forEach(close(_))
      allocator.close()
    }

    /**
     * Close the allocator quietly without having any OOM errors thrown. We rely on Spark's memory
     * management system to detect possible memory leaks after the task get successfully down. Any
     * leak shown right here is possibly not actual because buffers may be cleaned up after
     * this check code is executed. Having said that developers should manage to make sure
     * the specific clean up logic of operators is registered at last of the program which means
     * it will be executed earlier.
     *
     * @see org.apache.spark.executor.Executor.TaskRunner#run()
     */
    private def softClose(allocator: BufferAllocator): Unit = {
      // move to leaked list
      val leakBytes = allocator.getAllocatedMemory
      val accumulated = ACCUMULATED_LEAK_BYTES.addAndGet(leakBytes)
      logWarning(s"Detected leaked Arrow allocator, size: $leakBytes, " +
        s"process accumulated leaked size: $accumulated...")
      if (DEBUG) {
        logDebug(s"Leaked allocator stack ${allocator.toVerboseString}")
        leakedArrowAllocators.add(allocator)
      }
    }

    private def close(allocator: NativeMemoryAllocator): Unit = {
      allocator.close()
    }

    private def softClose(allocator: NativeMemoryAllocator): Unit = {
      // move to leaked list
      val leakBytes = allocator.getBytesAllocated
      val accumulated = ACCUMULATED_LEAK_BYTES.addAndGet(leakBytes)
      logWarning(s"Detected leaked native allocator, size: $leakBytes, " +
        s"process accumulated leaked size: $accumulated...")
      allocator.listener().inactivate()
      if (DEBUG) {
        leakedNativeAllocators.add(allocator)
      }
    }

    private def closeIfCloseable(some: Any) = {
      some match {
        case closeable: AutoCloseable =>
          closeable.close()
        case _ =>
      }
    }

    def release(): Unit = {
      for (allocator <- arrowAllocators.asScala.reverse) {
        val allocated = allocator.getAllocatedMemory
        if (allocated == 0L) {
          close(allocator)
        } else {
            softClose(allocator)
        }
      }
      for (alloc <- nativeAllocators.asScala) {
        val allocated = alloc.getBytesAllocated
        if (allocated == 0L) {
          close(alloc)
        } else {
          softClose(alloc)
        }
      }
      closeIfCloseable(arrowAllocListener)
    }
  }

  private val taskToResourcesMap = new java.util.IdentityHashMap[TaskContext, TaskMemoryResources]()

  private val leakedArrowAllocators = new java.util.Vector[BufferAllocator]()
  private val leakedNativeAllocators = new java.util.Vector[NativeMemoryAllocator]()

  private def getLocalTaskContext(): TaskContext = {
    TaskContext.get()
  }

  private def inSparkTask(): Boolean = {
    TaskContext.get() != null
  }

  private def getTaskMemoryManager(): TaskMemoryManager = {
    getLocalTaskContext().taskMemoryManager()
  }

  def addLeakSafeTaskCompletionListener[U](f: TaskContext => U): TaskContext = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    getTaskMemoryResources() // initialize cleaners
    getLocalTaskContext().addTaskCompletionListener(f)
  }

  def getTaskMemoryResources(): TaskMemoryResources = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    val tc = getLocalTaskContext()
    taskToResourcesMap.synchronized {

      if (!taskToResourcesMap.containsKey(tc)) {
        taskToResourcesMap.put(tc, new TaskMemoryResources)
        tc.addTaskCompletionListener(
          new TaskCompletionListener {
            override def onTaskCompletion(context: TaskContext): Unit = {
              taskToResourcesMap.synchronized {
                val resources = taskToResourcesMap.remove(context)
                resources.release()
                context.taskMetrics().incPeakExecutionMemory(resources.sharedMetrics.peak())
              }
            }
          })
      }

      return taskToResourcesMap.get(tc)
    }
  }

  private val maxAllocationSize = {
    SparkEnv.get.conf.get(MEMORY_OFFHEAP_SIZE)
  }

  private val globalArrowAlloc = new RootAllocator(maxAllocationSize)

  def globalArrowAllocator(): BufferAllocator = {
    globalArrowAlloc
  }

  def createSpillableArrowAllocator(spiller: Spiller): BufferAllocator = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Spiller must be used in a Spark task")
    }
    getTaskMemoryResources().createSpillableArrowAllocator(spiller)
  }

  def contextArrowAllocator(): BufferAllocator = {
    if (!inSparkTask()) {
      return globalArrowAllocator()
    }
    getTaskMemoryResources().taskDefaultArrowAllocator
  }

  def contextArrowAllocatorUnmanaged(): BufferAllocator = {
    if (!inSparkTask()) {
      return globalArrowAllocator()
    }
    getTaskMemoryResources().taskDefaultArrowAllocatorUnmanaged
  }

  def globalNativeAllocator(): NativeMemoryAllocator = {
    NativeMemoryAllocator.getDefault
  }

  def createSpillableNativeAllocator(spiller: Spiller): NativeMemoryAllocator = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Spiller must be used in a Spark task")
    }
    getTaskMemoryResources().createSpillableNativeAllocator(spiller)
  }

  def contextNativeAllocator(): NativeMemoryAllocator = {
    if (!inSparkTask()) {
      return globalNativeAllocator()
    }
    getTaskMemoryResources().taskDefaultNativeAllocator
  }

  def getLeakedArrowAllocators(): List[BufferAllocator] = {
    val list = new java.util.ArrayList[BufferAllocator](leakedArrowAllocators)
    list.asScala.toList
  }

  def getLeakedNativeAllocators(): List[NativeMemoryAllocator] = {
    val list = new java.util.ArrayList[NativeMemoryAllocator](leakedNativeAllocators)
    list.asScala.toList
  }

  class UnsafeItr[T <: AutoCloseable](delegate: Iterator[T])
    extends Iterator[T] {
    val holder = new GenericRetainer[T]()

    SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]((_: TaskContext) => {
      holder.release()
    })

    override def hasNext: Boolean = {
      holder.release()
      val hasNext = delegate.hasNext
      hasNext
    }

    override def next(): T = {
      val b = delegate.next()
      holder.retain(b)
      b
    }
  }

  class GenericRetainer[T <: AutoCloseable] {
    private var retained: Option[T] = None

    def retain(batch: T): Unit = {
      if (retained.isDefined) {
        throw new IllegalStateException
      }
      retained = Some(batch)
    }

    def release(): Unit = {
      retained.foreach(b => b.close())
      retained = None
    }
  }
}
