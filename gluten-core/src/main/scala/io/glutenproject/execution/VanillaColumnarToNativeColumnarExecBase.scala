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
package io.glutenproject.execution

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.GlutenPlan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Provides a common executor to translate an [[RDD]] of Vanilla [[ColumnarBatch]] into an [[RDD]]
 * of native [[ColumnarBatch]].
 */
abstract class VanillaColumnarToNativeColumnarExecBase(child: SparkPlan)
  extends GlutenPlan
  with UnaryExecNode {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genVanillaColumnarToNativeColumnarMetrics(sparkContext)

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  final override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }

  final override def output: Seq[Attribute] = child.output

  final override def outputPartitioning: Partitioning = child.outputPartitioning

  final override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  def doExecuteColumnarInternal(): RDD[ColumnarBatch]
}
