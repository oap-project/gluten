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

import org.apache.spark.sql.execution.metric.SQLMetric

case class InputIteratorMetricsUpdater(metrics: Map[String, SQLMetric]) extends MetricsUpdater {
  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      if (!operatorMetrics.metricsList.isEmpty) {
        val metricsData = operatorMetrics.metricsList.get(0)
        metrics("iterReadTime") += (metricsData.time / 1000L).toLong

        val processors = MetricsUtil.getAllProcessorList(metricsData)
        processors.foreach(
          processor => {
            if (
              InputIteratorMetricsUpdater.CH_PLAN_NODE_NAME
                .exists(processor.name.startsWith(_))
            ) {
              metrics("numInputRows") += processor.inputRows
              metrics("numOutputRows") += processor.outputRows
            }
            if (processor.name.equalsIgnoreCase("FillingRightJoinSide")) {
              metrics("fillingRightJoinSideTime") += (processor.time / 1000L).toLong
            }
          })
      }
    }
  }
}

object InputIteratorMetricsUpdater {
  val INCLUDING_PROCESSORS = Array("BlocksBufferPoolTransform")
  val CH_PLAN_NODE_NAME = Array("BlocksBufferPoolTransform")
}
