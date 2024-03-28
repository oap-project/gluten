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
package io.glutenproject.sql.shims.spark32

import io.glutenproject.execution.datasource.GlutenParquetWriterInjects
import io.glutenproject.expression.{ExpressionNames, Sig}
import io.glutenproject.sql.shims.{ShimDescriptor, SparkShims}

import org.apache.spark.{ShuffleUtils, SparkContext, TaskContext, TaskContextUtils}
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil, SparkPlan}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.FileFormatWriter.Empty2Null
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.datasources.v2.utils.CatalogUtil
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.{BlockId, BlockManagerId}

import org.apache.hadoop.fs.{FileStatus, Path}

import java.util.{HashMap => JHashMap, Map => JMap}

class Spark32Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def getDistribution(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Distribution] = {
    HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
  }

  override def scalarExpressionMappings: Seq[Sig] = Seq(Sig[Empty2Null](ExpressionNames.EMPTY2NULL))

  override def aggregateExpressionMappings: Seq[Sig] = Seq.empty

  override def convertPartitionTransforms(
      partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    CatalogUtil.convertPartitionTransforms(partitions)
  }

  override def generateFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      fileSourceScanExec: FileSourceScanExec): FileScanRDD = {
    new FileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def getTextScan(
      sparkSession: SparkSession,
      fileIndex: PartitioningAwareFileIndex,
      dataSchema: StructType,
      readDataSchema: StructType,
      readPartitionSchema: StructType,
      options: CaseInsensitiveStringMap,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): TextScan = {
    new TextScan(
      sparkSession,
      fileIndex,
      readDataSchema,
      readPartitionSchema,
      options,
      partitionFilters,
      dataFilters)
  }

  override def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]] = {
    selectedPartitions
      .flatMap {
        p => p.files.map(f => PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values))
      }
      .groupBy {
        f =>
          BucketingUtils
            .getBucketId(f.filePath)
            .getOrElse(throw new IllegalStateException(s"Invalid bucket file ${f.filePath}"))
      }
  }

  override def getBatchScanExecTable(batchScan: BatchScanExec): Table = null

  override def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile =
    PartitionedFile(partitionValues, filePath, start, length, locations)

  override def hasBloomFilterAggregate(
      agg: org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec): Boolean = false

  override def extractSubPlanFromMightContain(expr: Expression): Option[SparkPlan] = None

  override def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] = {
    List(session => GlutenParquetWriterInjects.getInstance().getExtendedColumnarPostRule(session))
  }

  override def createTestTaskContext(): TaskContext = {
    TaskContextUtils.createTestTaskContext()
  }

  def setJobDescriptionOrTagForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    // Setup a job group here so later it may get cancelled by groupId if necessary.
    sc.setJobGroup(
      broadcastExchange.runId.toString,
      s"broadcast exchange (runId ${broadcastExchange.runId})",
      interruptOnCancel = true)
  }

  def cancelJobGroupForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    sc.cancelJobGroup(broadcastExchange.runId.toString)
  }

  override def getShuffleReaderParam[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int)
      : Tuple2[Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])], Boolean] = {
    ShuffleUtils.getReaderParam(handle, startMapIndex, endMapIndex, startPartition, endPartition)
  }

  override def getPartitionId(taskInfo: TaskInfo): Int = {
    throw new IllegalStateException("This is not supported.")
  }

  override def supportDuplicateReadingTracking: Boolean = false

  def getFileStatus(partition: PartitionDirectory): Seq[FileStatus] = partition.files

  def splitFiles(
      sparkSession: SparkSession,
      file: FileStatus,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile] = {
    PartitionedFileUtil.splitFiles(
      sparkSession,
      file,
      filePath,
      isSplitable,
      maxSplitBytes,
      partitionValues)
  }

  def structFromAttributes(attrs: Seq[Attribute]): StructType = {
    StructType(attrs.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  }

  def attributesFromStruct(structType: StructType): Seq[Attribute] = {
    structType.fields.map {
      field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    }
  }

  override def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String]): JMap[String, String] =
    new JHashMap[String, String]()

  def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan] = {
    ae.plan
  }

  override def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]] = null

  override def getCommonPartitionValues(batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]] =
    null
}
