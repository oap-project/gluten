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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric

import scala.collection.JavaConverters._

class HashAggregateMetricsUpdater(val metrics: Map[String, SQLMetric])
  extends MetricsUpdater
  with Logging {

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    try {
      if (opMetrics != null) {
        val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
        if (!operatorMetrics.metricsList.isEmpty && operatorMetrics.aggParams != null) {
          var currentIdx = operatorMetrics.metricsList.size() - 1
          var totalTime = 0L

          // aggregating
          val aggMetricsData = operatorMetrics.metricsList.get(currentIdx)
          metrics("aggregatingTime") += (aggMetricsData.time / 1000L).toLong
          metrics("outputVectors") += aggMetricsData.outputVectors
          metrics("inputWaitTime") += (aggMetricsData.inputWaitTime / 1000L).toLong
          metrics("outputWaitTime") += (aggMetricsData.outputWaitTime / 1000L).toLong
          totalTime += aggMetricsData.time

          MetricsUtil.updateExtraTimeMetric(
            aggMetricsData,
            metrics("extraTime"),
            metrics("numOutputRows"),
            metrics("outputBytes"),
            metrics("numInputRows"),
            metrics("inputBytes"),
            HashAggregateMetricsUpdater.INCLUDING_PROCESSORS,
            HashAggregateMetricsUpdater.CH_PLAN_NODE_NAME
          )

          val resizeStep = aggMetricsData.steps.asScala
            .flatMap(_.processors.asScala)
            .find(s => s.getName.equalsIgnoreCase("Resize"))
          if (!resizeStep.isEmpty) {
            metrics("resizeInputRows") += resizeStep.get.inputRows
            metrics("resizeOutputRows") += aggMetricsData.getOutputRows
          }

          currentIdx -= 1
          metrics("totalTime") += (totalTime / 1000L).toLong
        }
      }
    } catch {
      case e: Throwable =>
        logError(s"Updating native metrics failed due to ${e.getCause}.")
        throw e
    }
  }
}

object HashAggregateMetricsUpdater {
  val INCLUDING_PROCESSORS = Array(
    "AggregatingTransform",
    "StreamingAggregatingTransform",
    "MergingAggregatedTransform",
    "GraceMergingAggregatedTransform")
  val CH_PLAN_NODE_NAME = Array(
    "AggregatingTransform",
    "StreamingAggregatingTransform",
    "MergingAggregatedTransform",
    "GraceMergingAggregatedTransform")
}
