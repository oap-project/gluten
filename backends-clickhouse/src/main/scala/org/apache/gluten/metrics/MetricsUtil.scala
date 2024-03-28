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
package org.apache.gluten.metrics

import org.apache.gluten.execution._
import org.apache.gluten.substrait.{AggregationParams, JoinParams}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, Collections => JCollections, List => JList, Map => JMap}

import scala.collection.JavaConverters._

object MetricsUtil extends Logging {

  /** Generate metrics updaters tree by SparkPlan */
  def treeifyMetricsUpdaters(plan: SparkPlan): MetricsUpdaterTree = {
    plan match {
      case j: HashJoinLikeExecTransformer =>
        MetricsUpdaterTree(
          j.metricsUpdater(),
          // must put the buildPlan first
          Seq(treeifyMetricsUpdaters(j.buildPlan), treeifyMetricsUpdaters(j.streamedPlan)))
      case t: TransformSupport =>
        MetricsUpdaterTree(t.metricsUpdater(), t.children.map(treeifyMetricsUpdaters))
      case _ =>
        MetricsUpdaterTree(NoopMetricsUpdater, Seq())
    }
  }

  /**
   * Update metrics fetched from certain iterator to transformers.
   *
   * @param child
   *   the child spark plan
   * @param relMap
   *   the map between operator index and its rels
   * @param joinParamsMap
   *   the map between operator index and join parameters
   * @param aggParamsMap
   *   the map between operator index and aggregation parameters
   */
  def updateNativeMetrics(
      child: SparkPlan,
      relMap: JMap[JLong, JList[JLong]],
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): IMetrics => Unit = {

    val mut: MetricsUpdaterTree = treeifyMetricsUpdaters(child)

    updateTransformerMetrics(
      mut,
      relMap,
      java.lang.Long.valueOf(relMap.size() - 1),
      joinParamsMap,
      aggParamsMap)
  }

  /**
   * A recursive function updating the metrics of one transformer and its child.
   *
   * @param mut
   *   the metrics updater tree built from the original plan
   * @param relMap
   *   the map between operator index and its rels
   * @param operatorIdx
   *   the index of operator
   * @param metrics
   *   the metrics fetched from native
   * @param metricsIdx
   *   the index of metrics
   * @param joinParamsMap
   *   the map between operator index and join parameters
   * @param aggParamsMap
   *   the map between operator index and aggregation parameters
   */
  def updateTransformerMetrics(
      mutNode: MetricsUpdaterTree,
      relMap: JMap[JLong, JList[JLong]],
      operatorIdx: JLong,
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): IMetrics => Unit = {
    imetrics =>
      try {
        val metrics = imetrics.asInstanceOf[NativeMetrics]
        val numNativeMetrics = metrics.metricsDataList.size()
        val relSize = relMap.values().asScala.flatMap(l => l.asScala).size
        if (numNativeMetrics == 0 || numNativeMetrics != relSize) {
          logWarning(
            s"Updating native metrics failed due to the wrong size of metrics data: " +
              s"$numNativeMetrics")
          ()
        } else if (mutNode.updater == NoopMetricsUpdater) {
          ()
        } else {
          updateTransformerMetricsInternal(
            mutNode,
            relMap,
            operatorIdx,
            metrics,
            numNativeMetrics - 1,
            joinParamsMap,
            aggParamsMap)
        }
      } catch {
        case e: Throwable =>
          logWarning(s"Updating native metrics failed due to ${e.getCause}.")
          ()
      }
  }

  /**
   * @return
   *   operator index and metrics index
   */
  def updateTransformerMetricsInternal(
      mutNode: MetricsUpdaterTree,
      relMap: JMap[JLong, JList[JLong]],
      operatorIdx: JLong,
      metrics: NativeMetrics,
      metricsIdx: Int,
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): (JLong, Int) = {
    val nodeMetricsList = new JArrayList[MetricsData]()
    var curMetricsIdx = metricsIdx
    relMap
      .get(operatorIdx)
      .forEach(
        idx => {
          nodeMetricsList.add(metrics.metricsDataList.get(idx.toInt))
          curMetricsIdx -= 1
        })

    JCollections.reverse(nodeMetricsList)
    val operatorMetrics = new OperatorMetrics(
      nodeMetricsList,
      joinParamsMap.getOrDefault(operatorIdx, null),
      aggParamsMap.getOrDefault(operatorIdx, null))
    mutNode.updater.updateNativeMetrics(operatorMetrics)

    var newOperatorIdx: JLong = operatorIdx - 1

    mutNode.children.foreach {
      child =>
        if (child.updater != NoopMetricsUpdater) {
          val result = updateTransformerMetricsInternal(
            child,
            relMap,
            newOperatorIdx,
            metrics,
            curMetricsIdx,
            joinParamsMap,
            aggParamsMap)
          newOperatorIdx = result._1
          curMetricsIdx = result._2
        }
    }
    (newOperatorIdx, curMetricsIdx)
  }

  /** Get all processors */
  def getAllProcessorList(metricData: MetricsData): Seq[MetricsProcessor] = {
    metricData.steps.asScala.flatMap(
      step => {
        step.processors.asScala
      })
  }

  /** Update extra time metric by the processors */
  def updateExtraTimeMetric(
      metricData: MetricsData,
      extraTime: SQLMetric,
      outputRows: SQLMetric,
      outputBytes: SQLMetric,
      inputRows: SQLMetric,
      inputBytes: SQLMetric,
      includingMetrics: Array[String],
      planNodeNames: Array[String]): Unit = {
    val processors = MetricsUtil.getAllProcessorList(metricData)
    processors.foreach(
      processor => {
        if (!includingMetrics.exists(processor.name.startsWith(_))) {
          extraTime += (processor.time / 1000L).toLong
        }
        if (planNodeNames.exists(processor.name.startsWith(_))) {
          outputRows += processor.outputRows
          outputBytes += processor.outputBytes
          inputRows += processor.inputRows
          inputBytes += processor.inputBytes
        }
      })
  }
}
