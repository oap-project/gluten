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

import org.apache.gluten.execution.ColumnarToRowExecBase
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.GlutenTimeMetric
import org.apache.gluten.vectorized.CHNativeBlock

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.CHExecUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CHColumnarToRowExec(child: SparkPlan) extends ColumnarToRowExecBase(child = child) {
  override def nodeName: String = "CHNativeColumnarToRow"

  override protected def doValidateInternal(): ValidationResult = {
    val schema = child.schema
    for (field <- schema.fields) {
      field.dataType match {
        case d: BooleanType =>
        case d: ByteType =>
        case d: ShortType =>
        case d: IntegerType =>
        case d: LongType =>
        case d: FloatType =>
        case d: DoubleType =>
        case d: DecimalType =>
        case d: StringType =>
        case d: BinaryType =>
        case d: DateType =>
        case d: TimestampType =>
        case d: ArrayType =>
        case d: StructType =>
        case d: MapType =>
        case d: NullType =>
        case _ =>
          throw new UnsupportedOperationException(
            s"${field.dataType} is not supported in ColumnarToRowExecBase.")
      }
    }
    ValidationResult.ok
  }

  override def doExecuteInternal(): RDD[InternalRow] = {
    new CHColumnarToRowRDD(
      sparkContext,
      child.executeColumnar(),
      longMetric("numOutputRows"),
      longMetric("numInputBatches"),
      longMetric("convertTime"))
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    child.executeBroadcast[T]()
  }

  protected def withNewChildInternal(newChild: SparkPlan): CHColumnarToRowExec =
    copy(child = newChild)
}

class CHColumnarToRowRDD(
    @transient sc: SparkContext,
    rdd: RDD[ColumnarBatch],
    numOutputRows: SQLMetric,
    numInputBatches: SQLMetric,
    convertTime: SQLMetric)
  extends RDD[InternalRow](sc, Seq(new OneToOneDependency(rdd))) {

  private val cleanedF = sc.clean(f)

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    cleanedF(firstParent[ColumnarBatch].iterator(split, context))
  }

  private def f: Iterator[ColumnarBatch] => Iterator[InternalRow] = {
    batches =>
      batches.flatMap {
        batch =>
          numInputBatches += 1
          numOutputRows += batch.numRows()

          if (batch.numRows == 0) {
            logInfo(s"Skip ColumnarBatch of ${batch.numRows} rows, ${batch.numCols} cols")
            Iterator.empty
          } else {
            val blockAddress = GlutenTimeMetric.millis(convertTime) {
              _ => CHNativeBlock.fromColumnarBatch(batch).blockAddress()
            }
            CHExecUtil.getRowIterFromSparkRowInfo(blockAddress, batch.numCols(), batch.numRows())
          }
      }
  }

  override def getPartitions: Array[Partition] = firstParent[ColumnarBatch].partitions
}
