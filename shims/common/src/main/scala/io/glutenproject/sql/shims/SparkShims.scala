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
package io.glutenproject.sql.shims

import io.glutenproject.expression.Sig

import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{FileSourceScanExec, GlobalLimitExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

sealed abstract class ShimDescriptor

case class SparkShimDescriptor(major: Int, minor: Int, patch: Int) extends ShimDescriptor {
  override def toString(): String = s"$major.$minor.$patch"

  def toMajorMinorVersion: String = s"$major.$minor"
}

trait SparkShims {
  def getShimDescriptor: ShimDescriptor

  // for this purpose, change HashClusteredDistribution to ClusteredDistribution
  // https://github.com/apache/spark/pull/32875
  def getDistribution(leftKeys: Seq[Expression], rightKeys: Seq[Expression]): Seq[Distribution]

  def structFromAttributes(attrs: Seq[Attribute]): StructType

  def expressionMappings: Seq[Sig]

  def convertPartitionTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec])

  def generateFileScanRDD(
      sparkSession: SparkSession,
      readFunction: (PartitionedFile) => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      fileSourceScanExec: FileSourceScanExec): FileScanRDD

  def splitFiles(
      sparkSession: SparkSession,
      // Hack -- the type changes in a non-backwards compatible way
      file: Object,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile]

  def getTextScan(
      sparkSession: SparkSession,
      fileIndex: PartitioningAwareFileIndex,
      dataSchema: StructType,
      readDataSchema: StructType,
      readPartitionSchema: StructType,
      options: CaseInsensitiveStringMap,
      partitionFilters: Seq[Expression] = Seq.empty,
      dataFilters: Seq[Expression] = Seq.empty): TextScan

  def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]]

  // Spark3.4 new add table parameter in BatchScanExec.
  def getBatchScanExecTable(batchScan: BatchScanExec): Table

  // The PartitionedFile API changed in spark 3.4
  def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile

  def hasBloomFilterAggregate(
      agg: org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec): Boolean

  def extractSubPlanFromMightContain(expr: Expression): Option[SparkPlan]

  def getLimitAndOffsetFromGlobalLimit(plan: GlobalLimitExec): (Int, Int) = (plan.limit, 0)

  def getLimitAndOffsetFromTopK(plan: TakeOrderedAndProjectExec): (Int, Int) = (plan.limit, 0)

  def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]]

  def writeFilesExecuteTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]): WriteTaskResult = {
    throw new UnsupportedOperationException()
  }

  def enableNativeWriteFilesByDefault(): Boolean = false

  def createTestTaskContext(): TaskContext
}
