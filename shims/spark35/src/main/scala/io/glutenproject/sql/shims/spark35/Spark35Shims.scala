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
package io.glutenproject.sql.shims.spark35

import io.glutenproject.GlutenConfig
import io.glutenproject.expression.{ExpressionNames, Sig}
import io.glutenproject.sql.shims.{ShimDescriptor, SparkShims}

import org.apache.spark.{ShuffleUtils, SparkContext, SparkContextUtils, SparkException, TaskContext, TaskContextUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.paths.SparkPath
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{ExtendedAnalysisException, InternalRow}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{FileSourceScanExec, GlobalLimitExec, GlutenFileFormatWriter, PartitionedFileUtil, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.datasources.{BucketingUtils, FileFormat, FilePartition, FileScanRDD, FileStatusWithMetadata, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex, WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.datasources.v2.utils.CatalogUtil
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.{BlockId, BlockManagerId}

import org.apache.hadoop.fs.{FileStatus, Path}

import java.time.ZoneOffset
import java.util.{HashMap => JHashMap, Map => JMap}

import scala.reflect.ClassTag

class Spark35Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def getDistribution(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Distribution] = {
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
  }

  override def scalarExpressionMappings: Seq[Sig] = {
    val list = if (GlutenConfig.getConf.enableNativeBloomFilter) {
      Seq(
        Sig[BloomFilterMightContain](ExpressionNames.MIGHT_CONTAIN),
        Sig[BloomFilterAggregate](ExpressionNames.BLOOM_FILTER_AGG))
    } else Seq.empty
    list ++ Seq(
      Sig[SplitPart](ExpressionNames.SPLIT_PART),
      Sig[Sec](ExpressionNames.SEC),
      Sig[Csc](ExpressionNames.CSC),
      Sig[Empty2Null](ExpressionNames.EMPTY2NULL))
  }

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
    new FileScanRDD(
      sparkSession,
      readFunction,
      filePartitions,
      new StructType(
        fileSourceScanExec.requiredSchema.fields ++
          fileSourceScanExec.relation.partitionSchema.fields),
      fileSourceScanExec.fileConstantMetadataColumns
    )
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
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      options,
      partitionFilters,
      dataFilters)
  }

  override def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]] = {
    selectedPartitions
      .flatMap(p => p.files.map(f => PartitionedFileUtil.getPartitionedFile(f, p.values)))
      .groupBy {
        f =>
          BucketingUtils
            .getBucketId(f.toPath.getName)
            .getOrElse(throw invalidBucketFile(f.urlEncodedPath))
      }
  }

  override def getBatchScanExecTable(batchScan: BatchScanExec): Table = batchScan.table

  override def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile =
    PartitionedFile(partitionValues, SparkPath.fromPathString(filePath), start, length, locations)

  override def hasBloomFilterAggregate(
      agg: org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec): Boolean = {
    agg.aggregateExpressions.exists(
      expr => expr.aggregateFunction.isInstanceOf[BloomFilterAggregate])
  }

  override def extractSubPlanFromMightContain(expr: Expression): Option[SparkPlan] = {
    expr match {
      case mc @ BloomFilterMightContain(sub: org.apache.spark.sql.execution.ScalarSubquery, _) =>
        Some(sub.plan)
      case mc @ BloomFilterMightContain(
            g @ GetStructField(sub: org.apache.spark.sql.execution.ScalarSubquery, _, _),
            _) =>
        Some(sub.plan)
      case _ => None
    }
  }

  override def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String]): JMap[String, String] = {
    val metadataColumn = new JHashMap[String, String]()
    val path = new Path(file.filePath.toString)
    for (columnName <- metadataColumnNames) {
      columnName match {
        case FileFormat.FILE_PATH => metadataColumn.put(FileFormat.FILE_PATH, path.toString)
        case FileFormat.FILE_NAME => metadataColumn.put(FileFormat.FILE_NAME, path.getName)
        case FileFormat.FILE_SIZE =>
          metadataColumn.put(FileFormat.FILE_SIZE, file.fileSize.toString)
        case FileFormat.FILE_MODIFICATION_TIME =>
          val fileModifyTime = TimestampFormatter
            .getFractionFormatter(ZoneOffset.UTC)
            .format(file.modificationTime * 1000L)
          metadataColumn.put(FileFormat.FILE_MODIFICATION_TIME, fileModifyTime)
        case FileFormat.FILE_BLOCK_START =>
          metadataColumn.put(FileFormat.FILE_BLOCK_START, file.start.toString)
        case FileFormat.FILE_BLOCK_LENGTH =>
          metadataColumn.put(FileFormat.FILE_BLOCK_LENGTH, file.length.toString)
        case _ =>
      }
    }

    // TODO row_index metadata support
    metadataColumn
  }

  // https://issues.apache.org/jira/browse/SPARK-40400
  private def invalidBucketFile(path: String): Throwable = {
    new SparkException(
      errorClass = "INVALID_BUCKET_FILE",
      messageParameters = Map("path" -> path),
      cause = null)
  }

  private def getLimit(limit: Int, offset: Int): Int = {
    if (limit == -1) {
      // Only offset specified, so fetch the maximum number rows
      Int.MaxValue
    } else {
      assert(limit > offset)
      limit - offset
    }
  }

  override def getLimitAndOffsetFromGlobalLimit(plan: GlobalLimitExec): (Int, Int) = {
    (getLimit(plan.limit, plan.offset), plan.offset)
  }

  override def getLimitAndOffsetFromTopK(plan: TakeOrderedAndProjectExec): (Int, Int) = {
    (getLimit(plan.limit, plan.offset), plan.offset)
  }

  override def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] = List()

  override def writeFilesExecuteTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]): WriteTaskResult = {
    GlutenFileFormatWriter.writeFilesExecuteTask(
      description,
      jobTrackerID,
      sparkStageId,
      sparkPartitionId,
      sparkAttemptNumber,
      committer,
      iterator
    )
  }

  override def enableNativeWriteFilesByDefault(): Boolean = true

  override def createTestTaskContext(): TaskContext = {
    TaskContextUtils.createTestTaskContext()
  }

  override def broadcastInternal[T: ClassTag](sc: SparkContext, value: T): Broadcast[T] = {
    SparkContextUtils.broadcastInternal(sc, value)
  }

  override def setJobDescriptionOrTagForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    // Setup a job tag here so later it may get cancelled by tag if necessary.
    sc.addJobTag(broadcastExchange.jobTag)
    sc.setInterruptOnCancel(true)
  }

  override def cancelJobGroupForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    sc.cancelJobsWithTag(broadcastExchange.jobTag)
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

  override def getShuffleAdvisoryPartitionSize(shuffle: ShuffleExchangeLike): Option[Long] =
    shuffle.advisoryPartitionSize

  override def getPartitionId(taskInfo: TaskInfo): Int = {
    taskInfo.partitionId
  }

  override def supportDuplicateReadingTracking: Boolean = true

  def getFileStatus(partition: PartitionDirectory): Seq[FileStatus] =
    partition.files.map(_.fileStatus)

  def splitFiles(
      sparkSession: SparkSession,
      file: FileStatus,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile] = {
    PartitionedFileUtil.splitFiles(
      sparkSession,
      FileStatusWithMetadata(file),
      isSplitable,
      maxSplitBytes,
      partitionValues)
  }

  def structFromAttributes(attrs: Seq[Attribute]): StructType = {
    DataTypeUtils.fromAttributes(attrs)
  }

  def attributesFromStruct(structType: StructType): Seq[Attribute] = {
    DataTypeUtils.toAttributes(structType)
  }

  def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan] = {
    ae match {
      case eae: ExtendedAnalysisException =>
        eae.plan
      case _ =>
        None
    }
  }

  override def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]] = null

  override def getCommonPartitionValues(batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]] =
    null

  override def supportsRowBased(plan: SparkPlan): Boolean = plan.supportsRowBased
}
