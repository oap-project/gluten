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
package org.apache.gluten.metrics;

import org.apache.gluten.substrait.AggregationParams;
import org.apache.gluten.substrait.JoinParams;

import java.util.List;

public class OperatorMetrics implements IOperatorMetrics {

  public List<MetricsData> metricsList;
  public JoinParams joinParams;
  public AggregationParams aggParams;

  /** Create an instance for operator metrics. */
  public OperatorMetrics(
      List<MetricsData> metricsList, JoinParams joinParams, AggregationParams aggParams) {
    this.metricsList = metricsList;
    this.aggParams = aggParams;
    this.joinParams = joinParams;
  }
}
