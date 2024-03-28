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
import org.apache.spark.sql.utils.OASPackageBridge.InputMetricsWrapper

/**
 * A minimized controller for updating operator's metrics, which means it never persists the
 * SparkPlan instance of the operator then the serialized RDD's size can be therefore minimized.
 *
 * TODO: place it to some other where since it's used not only by whole stage facilities
 */
trait MetricsUpdater extends Serializable {

  def metrics: Map[String, SQLMetric]

  def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit = {}

  def updateNativeMetrics(operatorMetrics: IOperatorMetrics): Unit = {}
}

final case class MetricsUpdaterTree(updater: MetricsUpdater, children: Seq[MetricsUpdaterTree])

object NoopMetricsUpdater extends MetricsUpdater {
  override def metrics: Map[String, SQLMetric] = Map.empty
}
