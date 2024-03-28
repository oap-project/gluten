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
package org.apache.gluten.events

import org.apache.spark.scheduler.SparkListenerEvent

sealed trait GlutenEvent extends SparkListenerEvent {}

case class GlutenBuildInfoEvent(info: Map[String, String]) extends GlutenEvent {}

/**
 * In AQE this event holds the fragment plan fallback reason, the behavior is similar with
 * `SparkListenerSQLAdaptiveExecutionUpdate`. In non-AQE this event holds the whole plan fallback
 * reason.
 *
 * Note that, some nodes may have no both `numFallbackNodes` and `numGlutenNodes`, e.g., `set a=b`
 *
 * @param executionId
 *   The query execution id.
 * @param numGlutenNodes
 *   The number of Gluten plan nodes.
 * @param numFallbackNodes
 *   The number of fallback vanilla Spark plan nodes.
 * @param physicalPlanDescription
 *   The style is similar with `explain formatted`
 * @param fallbackNodeToReason
 *   The fallback reason for each plan node, e.g., 001 File Scan -> columnar FileScan is not enabled
 */
case class GlutenPlanFallbackEvent(
    executionId: Long,
    numGlutenNodes: Int,
    numFallbackNodes: Int,
    physicalPlanDescription: String,
    fallbackNodeToReason: Map[String, String])
  extends GlutenEvent {}
