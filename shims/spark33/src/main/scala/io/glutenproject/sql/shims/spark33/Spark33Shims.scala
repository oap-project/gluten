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
package io.glutenproject.sql.shims.spark33

import io.glutenproject.GlutenConfig
import io.glutenproject.execution.datasource.GlutenParquetWriterInjects
import io.glutenproject.expression.{ExpressionNames, Sig}
import io.glutenproject.expression.ExpressionNames.TIMESTAMP_ADD
import io.glutenproject.sql.shims.{ShimDescriptor, SparkShims}

import org.apache.spark._
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.TimestampFormatter
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

import java.time.ZoneOffset
import java.util.{HashMap => JHashMap, Map => JMap}

class Spark33Shims extends SparkShims {
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
      Sig[Empty2Null](ExpressionNames.EMPTY2NULL),
      Sig[TimestampAdd](TIMESTAMP_ADD)
    )
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
      fileSourceScanExec.metadataColumns
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
      .flatMap {
        p => p.files.map(f => PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values))
      }
      .groupBy {
        f =>
          BucketingUtils
            .getBucketId(new Path(f.filePath).getName)
            .getOrElse(throw invalidBucketFile(f.filePath))
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
        case _ =>
      }
    }
    metadataColumn
  }

  private def invalidBucketFile(path: String): Throwable = {
    new SparkException(
      errorClass = "INVALID_BUCKET_FILE",
      messageParameters = Array(path),
      cause = null)
  }

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
    taskInfo.partitionId
  }

  override def supportDuplicateReadingTracking: Boolean = true

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

  def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan] = {
    ae.plan
  }

  override def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]] = {
    batchScan.keyGroupedPartitioning
  }
  override def getCommonPartitionValues(batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]] =
    null

  override def extractExpressionTimestampAddUnit(exp: Expression): Option[Seq[String]] = {
    exp match {
      case timestampAdd: TimestampAdd =>
        Option.apply(Seq(timestampAdd.unit, timestampAdd.timeZoneId.getOrElse("")))
      case _ => Option.empty
    }
  }

  override def supportsRowBased(plan: SparkPlan): Boolean = plan.supportsRowBased
}
