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

import org.apache.gluten.metrics.GlutenTimeMetric
import org.apache.gluten.vectorized.{CHNativeExpressionEvaluator, CloseableCHColumnBatchIterator, GeneralInIterator, GeneralOutIterator}

import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util
import java.util.concurrent.TimeUnit.NANOSECONDS

class NativeFileScanColumnarRDD(
    @transient sc: SparkContext,
    @transient private val inputPartitions: Seq[InputPartition],
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    scanTime: SQLMetric)
  extends RDD[ColumnarBatch](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val inputPartition = castNativePartition(split)

    assert(
      inputPartition.isInstanceOf[GlutenPartition],
      "NativeFileScanColumnarRDD only accepts GlutenPartition.")

    val splitInfoByteArray = inputPartition
      .asInstanceOf[GlutenPartition]
      .splitInfosByteArray

    val resIter: GeneralOutIterator = GlutenTimeMetric.millis(scanTime) {
      _ =>
        val transKernel = new CHNativeExpressionEvaluator()
        val inBatchIters = new util.ArrayList[GeneralInIterator]()
        transKernel.createKernelWithBatchIterator(
          inputPartition.plan,
          splitInfoByteArray,
          inBatchIters,
          false
        )
    }
    TaskContext.get().addTaskCompletionListener[Unit](_ => resIter.close())
    val iter: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
      var scanTotalTime = 0L
      var scanTimeAdded = false

      override def hasNext: Boolean = {
        val res = GlutenTimeMetric.withNanoTime(resIter.hasNext)(t => scanTotalTime += t)
        if (!res && !scanTimeAdded) {
          scanTime += NANOSECONDS.toMillis(scanTotalTime)
          scanTimeAdded = true
        }
        res
      }

      override def next(): ColumnarBatch = {
        GlutenTimeMetric.withNanoTime {
          val cb = resIter.next()
          numOutputRows += cb.numRows()
          numOutputBatches += 1
          cb
        }(t => scanTotalTime += t)
      }
    }
    new CloseableCHColumnBatchIterator(iter)
  }

  private def castNativePartition(split: Partition): BaseGlutenPartition = split match {
    case FirstZippedPartitionsPartition(_, p: BaseGlutenPartition, _) => p
    case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.preferredLocations()
  }

  private def castPartition(split: Partition): FirstZippedPartitionsPartition = split match {
    case p: FirstZippedPartitionsPartition => p
    case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
  }

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartition, index) => FirstZippedPartitionsPartition(index, inputPartition)
    }.toArray
  }
}
