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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.metrics.{GlutenTimeMetric, IMetrics}

import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.InputFileBlockHolderProxy
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ExecutorManager

import scala.collection.mutable

trait BaseGlutenPartition extends Partition with InputPartition {
  def plan: Array[Byte]
}

case class GlutenPartition(
    index: Int,
    plan: Array[Byte],
    splitInfosByteArray: Array[Array[Byte]] = Array.empty[Array[Byte]],
    locations: Array[String] = Array.empty[String],
    files: Array[String] =
      Array.empty[String] // touched files, for implementing UDF input_file_names
) extends BaseGlutenPartition {

  override def preferredLocations(): Array[String] = locations
}

case class GlutenFilePartition(index: Int, files: Array[PartitionedFile], plan: Array[Byte])
  extends BaseGlutenPartition {
  override def preferredLocations(): Array[String] = {
    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach {
      file =>
        file.locations.filter(_ != "localhost").foreach {
          host => hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
        }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq
      .sortBy { case (host, numBytes) => numBytes }
      .reverse
      .take(3)
      .map { case (host, numBytes) => host }
      .toArray
  }
}

case class FirstZippedPartitionsPartition(
    index: Int,
    inputPartition: InputPartition,
    inputColumnarRDDPartitions: Seq[Partition] = Seq.empty)
  extends Partition

class GlutenWholeStageColumnarRDD(
    @transient sc: SparkContext,
    @transient private val inputPartitions: Seq[InputPartition],
    var rdds: ColumnarInputRDDsWrapper,
    pipelineTime: SQLMetric,
    updateInputMetrics: InputMetricsWrapper => Unit,
    updateNativeMetrics: IMetrics => Unit)
  extends RDD[ColumnarBatch](sc, rdds.getDependencies) {
  private val numaBindingInfo = GlutenConfig.getConf.numaBindingInfo

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {

    // To support input_file_name(). According to semantic we should return
    // the exact file name a row belongs to. However in columnar engine it's
    // not easy to accomplish this. so we return a list of file(part) names
    split match {
      case FirstZippedPartitionsPartition(_, g: GlutenPartition, _) =>
        InputFileBlockHolderProxy.set(g.files.mkString(","))
      case _ =>
        InputFileBlockHolderProxy.unset()
    }

    GlutenTimeMetric.millis(pipelineTime) {
      _ =>
        ExecutorManager.tryTaskSet(numaBindingInfo)
        val (inputPartition, inputColumnarRDDPartitions) = castNativePartition(split)
        val inputIterators = rdds.getIterators(inputColumnarRDDPartitions, context)
        BackendsApiManager.getIteratorApiInstance.genFirstStageIterator(
          inputPartition,
          context,
          pipelineTime,
          updateInputMetrics,
          updateNativeMetrics,
          split.index,
          inputIterators
        )
    }
  }

  private def castNativePartition(split: Partition): (BaseGlutenPartition, Seq[Partition]) = {
    split match {
      case FirstZippedPartitionsPartition(_, g: BaseGlutenPartition, p) => (g, p)
      case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castNativePartition(split)._1.preferredLocations()
  }

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex
      .map {
        case (partition, i) => FirstZippedPartitionsPartition(i, partition, rdds.getPartitions(i))
      }
      .toArray[Partition]
  }

  override protected def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
