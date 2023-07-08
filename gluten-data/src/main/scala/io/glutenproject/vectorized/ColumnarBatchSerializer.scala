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

package io.glutenproject.vectorized

import java.io._
import java.nio.ByteBuffer
import scala.reflect.ClassTag
import io.glutenproject.columnarbatch.ColumnarBatches
import io.glutenproject.memory.alloc.NativeMemoryAllocators
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.utils.ArrowAbiUtil

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkSchemaUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.util.TaskResources

class ColumnarBatchSerializer(
    schema: StructType,
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    decompressTime: SQLMetric)
  extends Serializer
  with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    new ColumnarBatchSerializerInstance(schema, readBatchNumRows, numOutputRows, decompressTime)
  }
}

private class ColumnarBatchSerializerInstance(
    schema: StructType,
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    decompressTime: SQLMetric)
  extends SerializerInstance
    with Logging {

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {

      private val allocator: BufferAllocator = ArrowBufferAllocators
        .contextInstance()
        .newChildAllocator("GlutenColumnarBatch deserialize", 0, Long.MaxValue)

      private val readerMetrics = new ShuffleReaderMetrics()

      private lazy val jniByteInputStream = JniByteInputStreams.create(in)
      private lazy val cSchema = ArrowSchema.allocateNew(ArrowBufferAllocators.contextInstance())

      private lazy val shuffleReaderHandle = {
        val arrowSchema =
          SparkSchemaUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
        ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
        val handle = ShuffleReaderJniWrapper.INSTANCE.make(
          jniByteInputStream, cSchema.memoryAddress(),
          NativeMemoryAllocators.getDefault().contextInstance.getNativeInstanceId)
        // Close shuffle reader instance as lately as the end of task processing,
        // since the native reader could hold a reference to memory pool that
        // was used to create all buffers read from shuffle reader. The pool
        // should keep alive before all buffers finish consuming.
        TaskResources.addRecycler(50) {
          close()
          ShuffleReaderJniWrapper.INSTANCE.close(handle)
        }
        handle
      }

      private var cb: ColumnarBatch = _

      private var numBatchesTotal: Long = _
      private var numRowsTotal: Long = _

      private var isClosed: Boolean = false

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      @throws(classOf[EOFException])
      override def readValue[T: ClassTag](): T = {
        if (cb != null) {
          cb.close()
          cb = null
        }
        val batch = {
          val batchHandle = try {
            ShuffleReaderJniWrapper.INSTANCE.next(shuffleReaderHandle)
          } catch {
            case ioe: IOException =>
              this.close()
              logError("Failed to load next RecordBatch", ioe)
              throw ioe
          }
          if (batchHandle == -1L) {
            // EOF reached
            this.close()
            throw new EOFException
          }
          ColumnarBatches.create(batchHandle)
        }
        val numRows = batch.numRows()
        logDebug(s"Read ColumnarBatch of ${numRows} rows")
        numBatchesTotal += 1
        numRowsTotal += numRows
        cb = batch
        cb.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        if (!isClosed) {
          // Collect Metrics
          ShuffleReaderJniWrapper.INSTANCE.populateMetrics(shuffleReaderHandle, readerMetrics)
          decompressTime += readerMetrics.getDecompressTime
          if (numBatchesTotal > 0) {
            readBatchNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
          }
          numOutputRows += numRowsTotal

          cSchema.close()
          jniByteInputStream.close()
          if (cb != null) cb.close()
          allocator.close()
          isClosed = true
        }
      }
    }
  }

  // Columnar shuffle write process don't need this.
  override def serializeStream(s: OutputStream): SerializationStream =
    throw new UnsupportedOperationException

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
