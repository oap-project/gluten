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
package org.apache.spark.sql.execution.datasources.v2

import io.glutenproject.GlutenConfig

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.base.Objects

class BatchScanExecShim(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression],
    @transient table: Table)
  extends BatchScanExec(output, scan, runtimeFilters, table = table) {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] = Map()

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException("Need to implement this method")
  }

  override def equals(other: Any): Boolean = other match {
    case that: BatchScanExecShim =>
      (that.canEqual(this)) && super.equals(that)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def canEqual(other: Any): Boolean = other.isInstanceOf[BatchScanExecShim]

  @transient protected lazy val filteredPartitions: Seq[Seq[InputPartition]] = {
    val dataSourceFilters = runtimeFilters.flatMap {
      case DynamicPruningExpression(e) => DataSourceV2Strategy.translateRuntimeFilterV2(e)
      case _ => None
    }

    if (dataSourceFilters.nonEmpty) {
      val originalPartitioning = outputPartitioning

      // the cast is safe as runtime filters are only assigned if the scan can be filtered
      val filterableScan = scan.asInstanceOf[SupportsRuntimeV2Filtering]
      filterableScan.filter(dataSourceFilters.toArray)

      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case p: KeyGroupedPartitioning =>
          if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
            throw new SparkException(
              "Data source must have preserved the original partitioning " +
                "during runtime filtering: not all partitions implement HasPartitionKey after " +
                "filtering")
          }
          val newPartitionValues = newPartitions
            .map(
              partition =>
                InternalRowComparableWrapper(
                  partition.asInstanceOf[HasPartitionKey],
                  p.expressions))
            .toSet
          val oldPartitionValues = p.partitionValues
            .map(partition => InternalRowComparableWrapper(partition, p.expressions))
            .toSet
          // We require the new number of partition values to be equal or less than the old number
          // of partition values here. In the case of less than, empty partitions will be added for
          // those missing values that are not present in the new input partitions.
          if (oldPartitionValues.size < newPartitionValues.size) {
            throw new SparkException(
              "During runtime filtering, data source must either report " +
                "the same number of partition values, or a subset of partition values from the " +
                s"original. Before: ${oldPartitionValues.size} partition values. " +
                s"After: ${newPartitionValues.size} partition values")
          }

          if (!newPartitionValues.forall(oldPartitionValues.contains)) {
            throw new SparkException(
              "During runtime filtering, data source must not report new " +
                "partition values that are not present in the original partitioning.")
          }

          groupPartitions(newPartitions).get.map(_._2)

        case _ =>
          // no validation is needed as the data source did not report any specific partitioning
          newPartitions.map(Seq(_))
      }

    } else {
      partitions
    }
  }

  @transient lazy val pushedAggregate: Option[Aggregation] = {
    scan match {
      case s: ParquetScan => s.pushedAggregate
      case o: OrcScan => o.pushedAggregate
      case _ => None
    }
  }
}
