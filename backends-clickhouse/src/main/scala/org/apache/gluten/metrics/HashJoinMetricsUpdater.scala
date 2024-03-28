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

class HashJoinMetricsUpdater(val metrics: Map[String, SQLMetric])
  extends MetricsUpdater
  with Logging {
  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    try {
      if (opMetrics != null) {
        val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
        if (!operatorMetrics.metricsList.isEmpty && operatorMetrics.joinParams != null) {
          val joinParams = operatorMetrics.joinParams
          var currentIdx = operatorMetrics.metricsList.size() - 1
          var totalTime = 0L

          // build side pre projection
          if (joinParams.buildPreProjectionNeeded) {
            metrics("buildPreProjectionTime") +=
              (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
            metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
            totalTime += operatorMetrics.metricsList.get(currentIdx).time
            currentIdx -= 1
          }

          // stream side pre projection
          if (joinParams.streamPreProjectionNeeded) {
            metrics("streamPreProjectionTime") +=
              (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
            metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
            totalTime += operatorMetrics.metricsList.get(currentIdx).time
            currentIdx -= 1
          }

          // update fillingRightJoinSideTime
          MetricsUtil
            .getAllProcessorList(operatorMetrics.metricsList.get(currentIdx))
            .foreach(
              processor => {
                if (processor.name.equalsIgnoreCase("FillingRightJoinSide")) {
                  metrics("fillingRightJoinSideTime") += (processor.time / 1000L).toLong
                }
              })

          // joining
          val joinMetricsData = operatorMetrics.metricsList.get(currentIdx)
          metrics("outputVectors") += joinMetricsData.outputVectors
          metrics("inputWaitTime") += (joinMetricsData.inputWaitTime / 1000L).toLong
          metrics("outputWaitTime") += (joinMetricsData.outputWaitTime / 1000L).toLong
          totalTime += joinMetricsData.time

          MetricsUtil
            .getAllProcessorList(joinMetricsData)
            .foreach(
              processor => {
                if (processor.name.equalsIgnoreCase("FillingRightJoinSide")) {
                  metrics("fillingRightJoinSideTime") += (processor.time / 1000L).toLong
                }
                if (processor.name.equalsIgnoreCase("FilterTransform")) {
                  metrics("conditionTime") += (processor.time / 1000L).toLong
                }
                if (processor.name.equalsIgnoreCase("JoiningTransform")) {
                  metrics("probeTime") += (processor.time / 1000L).toLong
                }
                if (!HashJoinMetricsUpdater.INCLUDING_PROCESSORS.contains(processor.name)) {
                  metrics("extraTime") += (processor.time / 1000L).toLong
                }
                if (HashJoinMetricsUpdater.CH_PLAN_NODE_NAME.contains(processor.name)) {
                  metrics("numOutputRows") += processor.outputRows
                  metrics("outputBytes") += processor.outputBytes
                  metrics("numInputRows") += processor.inputRows
                  metrics("inputBytes") += processor.inputBytes
                }
              })

          currentIdx -= 1

          // post projection
          if (joinParams.postProjectionNeeded) {
            metrics("postProjectTime") +=
              (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
            metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
            totalTime += operatorMetrics.metricsList.get(currentIdx).time
            currentIdx -= 1
          }
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

object HashJoinMetricsUpdater {
  val INCLUDING_PROCESSORS = Array("JoiningTransform", "FillingRightJoinSide", "FilterTransform")
  val CH_PLAN_NODE_NAME = Array("JoiningTransform")
}
