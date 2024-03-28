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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.exception.GlutenException

import org.apache.spark.sql.execution.datasources.VeloxWriteQueue.EOS_BATCH
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

class VeloxColumnarBatchIterator(schema: Schema, allocator: BufferAllocator)
  extends Iterator[ColumnarBatch]
  with AutoCloseable {
  private val writeQueue = new ArrayBlockingQueue[ColumnarBatch](64)
  private var currentBatch: Option[ColumnarBatch] = None

  def enqueue(batch: ColumnarBatch): Unit = {
    // Throw exception if the queue is full.
    if (!writeQueue.offer(batch, 30L, TimeUnit.MINUTES)) {
      throw new GlutenException("VeloxParquetWriter: Timeout waiting for adding data")
    }
  }

  override def hasNext: Boolean = {
    val batch =
      try {
        writeQueue.poll(30L, TimeUnit.MINUTES)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          EOS_BATCH
      }
    if (batch == null) {
      throw new GlutenException("VeloxParquetWriter: Timeout waiting for data")
    }
    if (batch == EOS_BATCH) {
      return false
    }
    currentBatch = Some(batch)
    true
  }

  override def next(): ColumnarBatch = {
    try {
      currentBatch match {
        case Some(b) => b
        case _ =>
          throw new IllegalStateException("VeloxParquetWriter: Fatal: Call hasNext() first!")
      }
    } finally {
      currentBatch = None
    }
  }

  override def close(): Unit = {
    allocator.close()
  }
}
