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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, DynamicPruningExpression, Expression, PlanExpression, Predicate}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable

abstract class FileSourceScanExecShim(
    @transient val relation: HadoopFsRelation,
    val output: Seq[Attribute],
    val requiredSchema: StructType,
    val partitionFilters: Seq[Expression],
    val optionalBucketSet: Option[BitSet],
    val optionalNumCoalescedBuckets: Option[Int],
    val dataFilters: Seq[Expression],
    val tableIdentifier: Option[TableIdentifier],
    val disableBucketedScan: Boolean = false)
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

  def dataFiltersInScan: Seq[Expression] = dataFilters

  def metadataColumns: Seq[AttributeReference] = Seq.empty

  def hasUnsupportedColumns: Boolean = {
    // Below name has special meaning in Velox.
    output.exists(a => a.name == "$path" || a.name == "$bucket")
  }

  def isMetadataColumn(attr: Attribute): Boolean = false

  def hasFieldIds: Boolean = false

  // The codes below are copied from FileSourceScanExec in Spark,
  // all of them are private.
  protected lazy val driverMetrics: mutable.HashMap[String, Long] = mutable.HashMap.empty

  protected lazy val driverMetricsAlias = {
    if (partitionFilters.exists(isDynamicPruningFilter)) {
      Map(
        "staticFilesNum" -> SQLMetrics.createMetric(sparkContext, "static number of files read"),
        "staticFilesSize" -> SQLMetrics.createSizeMetric(sparkContext, "static size of files read")
      )
    } else {
      Map.empty[String, SQLMetric]
    }
  }

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has been
   * initialized. See SPARK-26327 for more details.
   */
  protected def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext,
      executionId,
      metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
  }

  protected def setFilesNumAndSizeMetric(
      partitions: Seq[PartitionDirectory],
      static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles") = filesNum
      driverMetrics("filesSize") = filesSize
    } else {
      driverMetrics("staticFilesNum") = filesNum
      driverMetrics("staticFilesSize") = filesSize
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions") = partitions.length
    }
  }

  @transient override lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    GlutenTimeMetric.withNanoTime {
      val ret =
        relation.location.listFiles(partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)
      setFilesNumAndSizeMetric(ret, static = true)
      ret
    }(t => driverMetrics("metadataTime") = NANOSECONDS.toMillis(t + optimizerMetadataTimeNs))
  }.toArray

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  @transient lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
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
      }(t => driverMetrics("pruningTime") = t)
    } else {
      selectedPartitions
    }
    sendDriverMetrics()
    selected
  }
}
