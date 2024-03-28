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
package org.apache.gluten.execution

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.memory.nmm.NativeMemoryManagers
import org.apache.gluten.utils.Iterators
import org.apache.gluten.vectorized.NativeColumnarToRowJniWrapper

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.{BroadcastUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

case class VeloxColumnarToRowExec(child: SparkPlan) extends ColumnarToRowExecBase(child = child) {

  override def nodeName: String = "VeloxColumnarToRowExec"

  override protected def doValidateInternal(): ValidationResult = {
    val schema = child.schema
    // Depending on the input type, VeloxColumnarToRowConverter.
    for (field <- schema.fields) {
      field.dataType match {
        case _: BooleanType =>
        case _: ByteType =>
        case _: ShortType =>
        case _: IntegerType =>
        case _: LongType =>
        case _: FloatType =>
        case _: DoubleType =>
        case _: StringType =>
        case _: TimestampType =>
        case _: DateType =>
        case _: BinaryType =>
        case _: DecimalType =>
        case _: ArrayType =>
        case _: MapType =>
        case _: StructType =>
        case YearMonthIntervalType.DEFAULT =>
        case _: NullType =>
        case _ =>
          throw new GlutenNotSupportException(
            s"${field.dataType} is unsupported in " +
              s"VeloxColumnarToRowExec.")
      }
    }
    ValidationResult.ok
  }

  override def doExecuteInternal(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val convertTime = longMetric("convertTime")
    child.executeColumnar().mapPartitions {
      it =>
        VeloxColumnarToRowExec.toRowIterator(
          it,
          output,
          numOutputRows,
          numInputBatches,
          convertTime
        )
    }
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val convertTime = longMetric("convertTime")

    val mode = BroadcastUtils.getBroadcastMode(outputPartitioning)
    val relation = child.executeBroadcast()
    BroadcastUtils.veloxToSparkUnsafe(
      sparkContext,
      mode,
      relation,
      VeloxColumnarToRowExec.toRowIterator(
        _,
        output,
        numOutputRows,
        numInputBatches,
        convertTime
      ))
  }

  protected def withNewChildInternal(newChild: SparkPlan): VeloxColumnarToRowExec =
    copy(child = newChild)
}

object VeloxColumnarToRowExec {
  def toRowIterator(
      batches: Iterator[ColumnarBatch],
      output: Seq[Attribute],
      numOutputRows: SQLMetric,
      numInputBatches: SQLMetric,
      convertTime: SQLMetric): Iterator[InternalRow] = {
    if (batches.isEmpty) {
      return Iterator.empty
    }

    // TODO:: pass the jni jniWrapper and arrowSchema  and serializeSchema method by broadcast
    val jniWrapper = NativeColumnarToRowJniWrapper.create()
    val c2rId = jniWrapper.nativeColumnarToRowInit(
      NativeMemoryManagers.contextInstance("ColumnarToRow").getNativeInstanceHandle)

    val res: Iterator[Iterator[InternalRow]] = new Iterator[Iterator[InternalRow]] {

      override def hasNext: Boolean = {
        batches.hasNext
      }

      override def next(): Iterator[InternalRow] = {
        val batch = batches.next()
        numInputBatches += 1
        numOutputRows += batch.numRows()

        if (batch.numRows == 0) {
          batch.close()
          Iterator.empty
        } else if (
          batch.numCols() > 0 &&
          !ColumnarBatches.isLightBatch(batch)
        ) {
          // Fallback to ColumnarToRow of vanilla Spark.
          val localOutput = output
          val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
          batch.rowIterator().asScala.map(toUnsafe)
        } else if (output.isEmpty) {
          numInputBatches += 1
          numOutputRows += batch.numRows()
          val rows = ColumnarBatches.emptyRowIterator(batch.numRows()).asScala
          batch.close()
          rows
        } else {
          val cols = batch.numCols()
          val rows = batch.numRows()
          val beforeConvert = System.currentTimeMillis()
          val batchHandle = ColumnarBatches.getNativeHandle(batch)
          val info =
            jniWrapper.nativeColumnarToRowConvert(batchHandle, c2rId)

          convertTime += (System.currentTimeMillis() - beforeConvert)

          new Iterator[InternalRow] {
            var rowId = 0
            val row = new UnsafeRow(cols)

            override def hasNext: Boolean = {
              rowId < rows
            }

            override def next: UnsafeRow = {
              val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
              row.pointTo(null, info.memoryAddress + offset, length)
              rowId += 1
              row
            }
          }
        }
      }
    }
    Iterators
      .wrap(res.flatten)
      .protectInvocationFlow() // Spark may call `hasNext()` again after a false output which
      // is not allowed by Gluten iterators. E.g. GroupedIterator#fetchNextGroupIterator
      .recycleIterator {
        jniWrapper.nativeClose(c2rId)
      }
      .create()
  }
}
