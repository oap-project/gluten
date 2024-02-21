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
import io.glutenproject.sql.shims.{ShimDescriptor, SparkShimLoader, SparkShims}

import org.apache.spark.{SparkConf, TaskContext, TaskContextImpl, TaskContextUtils}
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{BucketingUtils, FilePartition, FileScanRDD, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.FileFormatWriter.Empty2Null
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.datasources.v2.utils.CatalogUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.fs.Path

import java.util.Properties

class Spark32Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def getDistribution(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Distribution] = {
    HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
  }

  override def structFromAttributes(
      attrs: Seq[Attribute]): StructType = {
    StructType.fromAttributes(attrs)
  }

  override def expressionMappings: Seq[Sig] = Seq(Sig[Empty2Null](ExpressionNames.EMPTY2NULL))

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

  override def splitFiles(
       sparkSession: SparkSession,
       file: FileStatusWithMetadata,
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

  override def createTestTaskContext(): TaskContext = {
    TaskContextUtils.createTestTaskContext()
  }
}
