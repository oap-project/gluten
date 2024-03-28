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

import org.apache.gluten.metrics.GlutenTimeMetric

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Expression, FileSourceConstantMetadataAttribute, FileSourceGeneratedMetadataAttribute, FileSourceMetadataAttribute, PlanExpression, Predicate}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

abstract class FileSourceScanExecShim(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends AbstractFileSourceScanExec(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] = Map()

  lazy val metadataColumns = output.collect {
    case FileSourceConstantMetadataAttribute(attr) => attr
    case FileSourceGeneratedMetadataAttribute(attr) => attr
  }

  def dataFiltersInScan: Seq[Expression] = dataFilters

  def hasUnsupportedColumns: Boolean = {
    val metadataColumnsNames = metadataColumns.map(_.name)
    // row_index metadata is not support yet
    metadataColumnsNames.contains(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME) ||
    output
      .filterNot(metadataColumns.toSet)
      .exists(v => metadataColumnsNames.contains(v.name)) ||
    // Below name has special meaning in Velox.
    output.exists(a => a.name == "$path" || a.name == "$bucket")
  }

  def isMetadataColumn(attr: Attribute): Boolean = metadataColumns.contains(attr)

  protected lazy val driverMetricsAlias = driverMetrics

  def hasFieldIds: Boolean = ParquetUtils.hasFieldIds(requiredSchema)

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  protected def setFilesNumAndSizeMetric(
      partitions: Seq[PartitionDirectory],
      static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles").set(filesNum)
      driverMetrics("filesSize").set(filesSize)
    } else {
      driverMetrics("staticFilesNum").set(filesNum)
      driverMetrics("staticFilesSize").set(filesSize)
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions").set(partitions.length)
    }
  }

  @transient override lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters =
      partitionFilters.filter(isDynamicPruningFilter)
    val selected = if (dynamicPartitionFilters.nonEmpty) {
      GlutenTimeMetric.withMillisTime {
        // call the file index for the files matching all filters except dynamic partition filters
        val predicate = dynamicPartitionFilters.reduce(And)
        val partitionColumns = relation.partitionSchema
        val boundPredicate = Predicate.create(
          predicate.transform {
            case a: AttributeReference =>
              val index = partitionColumns.indexWhere(a.name == _.name)
              BoundReference(index, partitionColumns(index).dataType, nullable = true)
          },
          Nil
        )
        val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
        setFilesNumAndSizeMetric(ret, static = false)
        ret
      }(t => driverMetrics("pruningTime").set(t))
    } else {
      selectedPartitions
    }
    sendDriverMetrics()
    selected
  }
}
