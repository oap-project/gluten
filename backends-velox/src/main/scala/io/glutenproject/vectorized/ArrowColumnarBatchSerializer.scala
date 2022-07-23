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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.arrow.compression.CommonsCompressionFactory
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.compression.NoCompressionCodec
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.datasources.v2.arrow.{SparkMemoryUtils}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class ArrowColumnarBatchSerializer(schema: StructType, readBatchNumRows: SQLMetric,
                                   numOutputRows: SQLMetric)
  extends Serializer with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
    new ArrowColumnarBatchSerializerInstance(arrowSchema, readBatchNumRows, numOutputRows)
  }
}

private class ArrowColumnarBatchSerializerInstance(
                                                    schema: Schema,
                                                    readBatchNumRows: SQLMetric,
                                                    numOutputRows: SQLMetric)
  extends SerializerInstance
    with Logging {

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private val compressionEnabled =
        SparkEnv.get.conf.getBoolean("spark.shuffle.compress", true)
      private val allocator: BufferAllocator = SparkMemoryUtils
        .contextArrowAllocatorUnmanaged()
        .newChildAllocator("ArrowColumnarBatch deserialize", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var vectors: Array[ColumnVector] = _
      private var cb: ColumnarBatch = _
      private var batchLoaded = true

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
        if (reader != null && batchLoaded) {
          root.clear()
          if (cb != null) {
            cb.close()
            cb = null
          }

          try {
            batchLoaded = reader.loadNextBatch()
          } catch {
            case ioe: IOException =>
              this.close()
              logError("Failed to load next RecordBatch", ioe)
              throw ioe
          }
          if (batchLoaded) {
            val numRows = root.getRowCount
            logDebug(s"Read ColumnarBatch of ${numRows} rows")

            numBatchesTotal += 1
            numRowsTotal += numRows

            val newFieldVectors = root.getFieldVectors.asScala.map { vector =>
              val newVector = vector.getField.createVector(allocator)
              vector.makeTransferPair(newVector).transfer()
              newVector
            }.asJava

            vectors = ArrowWritableColumnVector
              .loadColumns(numRows, newFieldVectors)
              .toArray[ColumnVector]

            cb = new ColumnarBatch(vectors, numRows)
            cb.asInstanceOf[T]
          } else {
            this.close()
            throw new EOFException
          }
        } else {
          reader = if (compressionEnabled) {
            new ArrowStreamReader(in, allocator, CommonsCompressionFactory.INSTANCE)
          } else {
            new ArrowStreamReader(in, allocator, NoCompressionCodec.Factory.INSTANCE)
          }

          try {
            root = reader.getVectorSchemaRoot
          } catch {
            case _: IOException =>
              this.close()
              throw new EOFException
          }
          readValue()
        }
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        if (!isClosed) {
          if (numBatchesTotal > 0) {
            readBatchNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
          }
          numOutputRows += numRowsTotal
          if (cb != null) cb.close()
          if (reader != null) reader.close(true)
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
