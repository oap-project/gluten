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
package org.apache.spark.sql.execution

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.exec.Runtimes
import org.apache.gluten.memory.arrowalloc.ArrowBufferAllocators
import org.apache.gluten.memory.nmm.NativeMemoryManagers
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.{ArrowAbiUtil, Iterators}
import org.apache.gluten.vectorized.{ColumnarBatchSerializerJniWrapper, NativeColumnarToRowJniWrapper}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.TaskResources

import org.apache.arrow.c.ArrowSchema

import scala.collection.JavaConverters.asScalaIteratorConverter

case class ColumnarBuildSideRelation(output: Seq[Attribute], batches: Array[Array[Byte]])
  extends BuildSideRelation {

  override def deserialized: Iterator[ColumnarBatch] = {
    val jniWrapper = ColumnarBatchSerializerJniWrapper.create()
    val serializeHandle: Long = {
      val allocator = ArrowBufferAllocators.contextInstance()
      val cSchema = ArrowSchema.allocateNew(allocator)
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
      val handle = jniWrapper
        .init(
          cSchema.memoryAddress(),
          NativeMemoryManagers
            .contextInstance("BuildSideRelation#BatchSerializer")
            .getNativeInstanceHandle)
      cSchema.close()
      handle
    }

    Iterators
      .wrap(new Iterator[ColumnarBatch] {
        var batchId = 0

        override def hasNext: Boolean = {
          batchId < batches.length
        }

        override def next: ColumnarBatch = {
          val handle =
            jniWrapper
              .deserialize(serializeHandle, batches(batchId))
          batchId += 1
          ColumnarBatches.create(Runtimes.contextInstance(), handle)
        }
      })
      .recycleIterator {
        jniWrapper.close(serializeHandle)
      }
      .recyclePayload(ColumnarBatches.forceClose) // FIXME why force close?
      .create()
  }

  override def asReadOnlyCopy(): ColumnarBuildSideRelation = this

  /**
   * Transform columnar broadcast value to Array[InternalRow] by key and distinct. NOTE: This method
   * was called in Spark Driver, should manage resources carefully.
   */
  override def transform(key: Expression): Array[InternalRow] = TaskResources.runUnsafe {
    // This transformation happens in Spark driver, thus resources can not be managed automatically.
    val runtime = Runtimes.contextInstance()
    val nativeMemoryManager = NativeMemoryManagers.contextInstance("BuildSideRelation#transform")
    val serializerJniWrapper = ColumnarBatchSerializerJniWrapper.create()
    val serializeHandle = {
      val allocator = ArrowBufferAllocators.contextInstance()
      val cSchema = ArrowSchema.allocateNew(allocator)
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
      val handle = serializerJniWrapper
        .init(cSchema.memoryAddress(), nativeMemoryManager.getNativeInstanceHandle)
      cSchema.close()
      handle
    }

    var closed = false

    // Convert columnar to Row.
    val jniWrapper = NativeColumnarToRowJniWrapper.create()
    val c2rId = jniWrapper.nativeColumnarToRowInit(nativeMemoryManager.getNativeInstanceHandle)
    var batchId = 0
    val iterator = if (batches.length > 0) {
      val res: Iterator[Iterator[InternalRow]] = new Iterator[Iterator[InternalRow]] {
        override def hasNext: Boolean = {
          val itHasNext = batchId < batches.length
          if (!itHasNext && !closed) {
            jniWrapper.nativeClose(c2rId)
            serializerJniWrapper.close(serializeHandle)
            closed = true
          }
          itHasNext
        }

        override def next(): Iterator[InternalRow] = {
          val batchBytes = batches(batchId)
          batchId += 1
          val batchHandle =
            serializerJniWrapper.deserialize(serializeHandle, batchBytes)
          val batch = ColumnarBatches.create(runtime, batchHandle)
          if (batch.numRows == 0) {
            batch.close()
            Iterator.empty
          } else if (output.isEmpty) {
            val rows = ColumnarBatches.emptyRowIterator(batch.numRows()).asScala
            batch.close()
            rows
          } else {
            val cols = batch.numCols()
            val rows = batch.numRows()
            val info =
              jniWrapper.nativeColumnarToRowConvert(batchHandle, c2rId)
            batch.close()
            val columnNames = key.flatMap {
              case expression: AttributeReference =>
                Some(expression)
              case _ =>
                None
            }
            if (columnNames.isEmpty) {
              throw new IllegalArgumentException(s"Key column not found in expression: $key")
            }
            if (columnNames.size != 1) {
              throw new IllegalArgumentException(s"Multiple key columns found in expression: $key")
            }
            val columnExpr = columnNames.head
            val oneColumnWithSameName = output.count(_.name == columnExpr.name) == 1
            val columnInOutput = output.zipWithIndex.filter {
              p: (Attribute, Int) =>
                if (oneColumnWithSameName) {
                  // The comparison of exprId can be ignored when
                  // only one attribute name match is found.
                  p._1.name == columnExpr.name
                } else {
                  // A case where output has multiple columns with same name
                  p._1.name == columnExpr.name && p._1.exprId == columnExpr.exprId
                }
            }
            if (columnInOutput.isEmpty) {
              throw new IllegalStateException(
                s"Key $key not found from build side relation output: $output")
            }
            if (columnInOutput.size != 1) {
              throw new IllegalStateException(
                s"More than one key $key found from build side relation output: $output")
            }
            val replacement =
              BoundReference(columnInOutput.head._2, columnExpr.dataType, columnExpr.nullable)

            val projExpr = key.transformDown {
              case _: AttributeReference =>
                replacement
            }

            val proj = UnsafeProjection.create(projExpr)

            new Iterator[InternalRow] {
              var rowId = 0
              val row = new UnsafeRow(cols)

              override def hasNext: Boolean = {
                rowId < rows
              }

              override def next: UnsafeRow = {
                if (rowId >= rows) throw new NoSuchElementException

                val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
                row.pointTo(null, info.memoryAddress + offset, length.toInt)
                rowId += 1
                row
              }
            }.map(proj).map(_.copy())
          }
        }
      }
      res.flatten
    } else {
      Iterator.empty
    }
    iterator.toArray
  }
}
