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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.backendsapi.MetricsApi
import org.apache.gluten.metrics._
import org.apache.gluten.substrait.{AggregationParams, JoinParams}
import org.apache.gluten.utils.LogLevelUtil

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import java.lang.{Long => JLong}
import java.util.{List => JList, Map => JMap}

class CHMetricsApi extends MetricsApi with Logging with LogLevelUtil {
  override def metricsUpdatingFunction(
      child: SparkPlan,
      relMap: JMap[JLong, JList[JLong]],
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): IMetrics => Unit = {
    MetricsUtil.updateNativeMetrics(child, relMap, joinParamsMap, aggParamsMap)
  }

  override def genInputIteratorTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] = {
    Map(
      "iterReadTime" -> SQLMetrics.createTimingMetric(
        sparkContext,
        "time of reading from iterator"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "fillingRightJoinSideTime" -> SQLMetrics.createTimingMetric(
        sparkContext,
        "filling right join side time")
    )
  }

  override def genInputIteratorTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = {
    InputIteratorMetricsUpdater(metrics)
  }

  override def genBatchScanTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time")
    )

  override def genBatchScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new BatchScanMetricsUpdater(metrics)

  override def genHiveTableScanTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
      "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
      "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read"),
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
      "pruningTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "readBytes" -> SQLMetrics.createMetric(sparkContext, "number of read bytes")
    )

  override def genHiveTableScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new HiveTableScanMetricsUpdater(metrics)

  override def genFileSourceScanTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
      "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
      "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read"),
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
      "pruningTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "selectedMarksPk" -> SQLMetrics.createMetric(sparkContext, "selected marks primary"),
      "selectedMarks" -> SQLMetrics.createMetric(sparkContext, "selected marks"),
      "totalMarksPk" -> SQLMetrics.createMetric(sparkContext, "total marks primary")
    )

  override def genFileSourceScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new FileSourceScanMetricsUpdater(metrics)

  override def genFilterTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genFilterTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new FilterMetricsUpdater(metrics)

  override def genProjectTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genProjectTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new ProjectMetricsUpdater(metrics)

  override def genHashAggregateTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "resizeInputRows" -> SQLMetrics.createMetric(sparkContext, "number of resize input rows"),
      "resizeOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of resize output rows"),
      "aggregatingTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "time of aggregating"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genHashAggregateTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater =
    new HashAggregateMetricsUpdater(metrics)

  override def genExpandTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genExpandTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new ExpandMetricsUpdater(metrics)

  override def genCustomExpandMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def genColumnarShuffleExchangeMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
      "bytesSpilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
      "computePidTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "totaltime to compute pid"),
      "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime to split"),
      "IOTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime to disk io"),
      "serializeTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "totaltime to block serialization"),
      "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime to spill"),
      "compressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime to compress"),
      "prepareTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime to prepare"),
      "avgReadBatchNumRows" -> SQLMetrics
        .createAverageMetric(sparkContext, "avg read batch num rows"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numOutputRows" -> SQLMetrics
        .createMetric(sparkContext, "number of output rows"),
      "inputBatches" -> SQLMetrics
        .createMetric(sparkContext, "number of input batches"),
      "outputBatches" -> SQLMetrics
        .createMetric(sparkContext, "number of output batches")
    )

  override def genWindowTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genWindowTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new WindowMetricsUpdater(metrics)

  override def genColumnarToRowMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
      "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert")
    )

  override def genRowToColumnarMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
      "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time to convert")
    )

  override def genLimitTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genLimitTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new LimitMetricsUpdater(metrics)

  override def genSortTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time")
    )

  override def genSortTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new SortMetricsUpdater(metrics)

  override def genSortMergeJoinTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map.empty

  override def genSortMergeJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new SortMergeJoinMetricsUpdater(metrics)

  override def genColumnarBroadcastExchangeMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
      "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast")
    )

  override def genColumnarSubqueryBroadcastMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)")
    )

  override def genHashJoinTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "extraTime" -> SQLMetrics.createTimingMetric(sparkContext, "extra operators time"),
      "inputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for data"),
      "outputWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of waiting for output"),
      "streamPreProjectionTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "time of stream side preProjection"),
      "buildPreProjectionTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "time of build side preProjection"),
      "postProjectTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "time of postProjection"),
      "probeTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "time of probe"),
      "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "total time"),
      "fillingRightJoinSideTime" -> SQLMetrics.createTimingMetric(
        sparkContext,
        "filling right join side time"),
      "conditionTime" -> SQLMetrics.createTimingMetric(sparkContext, "join condition time")
    )

  override def genHashJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new HashJoinMetricsUpdater(metrics)

  override def genNestedLoopJoinTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] = {
    throw new UnsupportedOperationException(
      s"NestedLoopJoinTransformer metrics update is not supported in CH backend")
  }

  override def genNestedLoopJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = {
    throw new UnsupportedOperationException(
      s"NestedLoopJoinTransformer metrics update is not supported in CH backend")
  }

  def genWriteFilesTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = {
    throw new UnsupportedOperationException(
      s"WriteFilesTransformer metrics update is not supported in CH backend")
  }

  def genWriteFilesTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater = {
    throw new UnsupportedOperationException(
      s"WriteFilesTransformer metrics update is not supported in CH backend")
  }
}
