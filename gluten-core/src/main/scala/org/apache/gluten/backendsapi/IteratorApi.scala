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
package org.apache.gluten.backendsapi

import org.apache.gluten.GlutenNumaBindingInfo
import org.apache.gluten.execution.{BaseGlutenPartition, BasicScanExecTransformer, WholeStageTransformContext}
import org.apache.gluten.metrics.IMetrics
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.SplitInfo

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

trait IteratorApi {

  def genSplitInfo(
      partition: InputPartition,
      partitionSchema: StructType,
      fileFormat: ReadFileFormat,
      metadataColumnNames: Seq[String]): SplitInfo

  /** Generate native row partition. */
  def genPartitions(
      wsCtx: WholeStageTransformContext,
      splitInfos: Seq[Seq[SplitInfo]],
      scans: Seq[BasicScanExecTransformer]): Seq[BaseGlutenPartition]

  /**
   * Inject the task attempt temporary path for native write files, this method should be called
   * before `genFirstStageIterator` or `genFinalStageIterator`
   */
  def injectWriteFilesTempPath(path: String): Unit = throw new UnsupportedOperationException()

  /**
   * Generate Iterator[ColumnarBatch] for first stage. ("first" means it does not depend on other
   * SCAN inputs)
   */
  def genFirstStageIterator(
      inputPartition: BaseGlutenPartition,
      context: TaskContext,
      pipelineTime: SQLMetric,
      updateInputMetrics: (InputMetricsWrapper) => Unit,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      inputIterators: Seq[Iterator[ColumnarBatch]] = Seq()
  ): Iterator[ColumnarBatch]

  /**
   * Generate Iterator[ColumnarBatch] for final stage. ("Final" means it depends on other SCAN
   * inputs, maybe it was a mistake to use the word "final")
   */
  // scalastyle:off argcount
  def genFinalStageIterator(
      context: TaskContext,
      inputIterators: Seq[Iterator[ColumnarBatch]],
      numaBindingInfo: GlutenNumaBindingInfo,
      sparkConf: SparkConf,
      rootNode: PlanNode,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      materializeInput: Boolean = false): Iterator[ColumnarBatch]
  // scalastyle:on argcount

  /** Generate Native FileScanRDD, currently only for ClickHouse Backend. */
  def genNativeFileScanRDD(
      sparkContext: SparkContext,
      wsCxt: WholeStageTransformContext,
      splitInfos: Seq[SplitInfo],
      scan: BasicScanExecTransformer,
      numOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      scanTime: SQLMetric): RDD[ColumnarBatch]
}
