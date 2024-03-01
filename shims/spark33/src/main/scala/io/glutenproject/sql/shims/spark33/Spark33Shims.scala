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
import io.glutenproject.sql.shims.{ShimDescriptor, SparkShims}

import org.apache.spark.{ShuffleDependency, ShuffleUtils, SparkEnv, SparkException, TaskContext, TaskContextUtils}
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.{ColumnPruning, ConstantFolding}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil, SparkPlan}
import org.apache.spark.sql.execution.datasources.{BucketingUtils, FilePartition, FileScanRDD, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.FileFormatWriter.Empty2Null
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.datasources.v2.utils.CatalogUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.{BlockId, BlockManagerId}

import org.apache.hadoop.fs.Path

class Spark33Shims extends SparkShims with PredicateHelper {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def getDistribution(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Distribution] = {
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
  }

  override def expressionMappings: Seq[Sig] = {
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

  override def needsPreProjectForBloomFilterAgg(filter: Filter)(
      needsPreProject: LogicalPlan => Boolean): Boolean = {
    splitConjunctivePredicates(filter.condition).exists {
      case _ @BloomFilterMightContain(sub: ScalarSubquery, _) =>
        sub.plan.exists {
          case agg: Aggregate => needsPreProject(agg)
          case _ => false
        }
      case _ => false
    }
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

  override def addPreProjectForBloomFilter(filter: Filter)(
      transformAgg: Aggregate => LogicalPlan): LogicalPlan = {
    val newConditions = splitConjunctivePredicates(filter.condition).map {
      case bloom @ BloomFilterMightContain(sub: ScalarSubquery, _) =>
        val newSubqueryPlan = sub.plan.transform {
          case agg: Aggregate => ConstantFolding(ColumnPruning(transformAgg(agg)))
        }
        bloom.copy(bloomFilterExpression = sub.copy(plan = newSubqueryPlan))
      case other => other
    }
    filter.copy(condition = newConditions.reduceLeft(And))
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
}
