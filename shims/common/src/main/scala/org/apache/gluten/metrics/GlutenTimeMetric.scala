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

import org.apache.gluten.utils.Arm

import org.apache.spark.sql.execution.metric.SQLMetric

import java.util.concurrent.TimeUnit

class GlutenTimeMetric(val metric: SQLMetric, timeConvert: Long => Long) extends AutoCloseable {
  private val start = System.nanoTime()

  override def close(): Unit = {
    val time = System.nanoTime() - start
    metric.add(timeConvert(time))
  }
}
object GlutenTimeMetric {
  def nano[V](metric: SQLMetric)(block: GlutenTimeMetric => V): V =
    Arm.withResource(new GlutenTimeMetric(metric, t => t))(block)
  def millis[V](metric: SQLMetric)(block: GlutenTimeMetric => V): V =
    Arm.withResource(new GlutenTimeMetric(metric, t => TimeUnit.NANOSECONDS.toMillis(t)))(block)

  def withNanoTime[U](block: => U)(nanoTime: Long => Unit): U = {
    val start = System.nanoTime()
    val result = block
    nanoTime(System.nanoTime() - start)
    result
  }
  def withMillisTime[U](block: => U)(millisTime: Long => Unit): U =
    withNanoTime(block)(t => millisTime(TimeUnit.NANOSECONDS.toMillis(t)))
}
