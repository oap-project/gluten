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
package org.apache.gluten.utils

import org.apache.gluten.extension.GlutenPlan

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange._

object PlanUtil {
  private def isGlutenTableCacheInternal(i: InMemoryTableScanExec): Boolean = {
    // `ColumnarCachedBatchSerializer` is at velox module, so use class name here
    i.relation.cacheBuilder.serializer.getClass.getSimpleName == "ColumnarCachedBatchSerializer" &&
    i.supportsColumnar
  }

  def isGlutenTableCache(plan: SparkPlan): Boolean = {
    plan match {
      case i: InMemoryTableScanExec =>
        isGlutenTableCacheInternal(i)
      case q: QueryStageExec =>
        // Compatible with Spark3.5 `TableCacheQueryStage`
        isGlutenTableCache(q.plan)
      case _ => false
    }
  }

  def outputNativeColumnarData(plan: SparkPlan): Boolean = {
    plan match {
      case a: AQEShuffleReadExec => outputNativeColumnarData(a.child)
      case s: QueryStageExec => outputNativeColumnarData(s.plan)
      case s: ReusedExchangeExec => outputNativeColumnarData(s.child)
      case s: InputAdapter => outputNativeColumnarData(s.child)
      case s: WholeStageCodegenExec => outputNativeColumnarData(s.child)
      case s: AdaptiveSparkPlanExec => outputNativeColumnarData(s.executedPlan)
      case i: InMemoryTableScanExec => PlanUtil.isGlutenTableCache(i)
      case _: GlutenPlan => true
      case _ => false
    }
  }

  def isVanillaColumnarOp(plan: SparkPlan): Boolean = {
    plan match {
      case i: InMemoryTableScanExec =>
        if (PlanUtil.isGlutenTableCache(i)) {
          // `InMemoryTableScanExec` do not need extra RowToColumnar or ColumnarToRow
          false
        } else {
          !plan.isInstanceOf[GlutenPlan] && plan.supportsColumnar
        }
      case a: AQEShuffleReadExec => isVanillaColumnarOp(a.child)
      case s: QueryStageExec => isVanillaColumnarOp(s.plan)
      case _: RowToColumnarExec => false
      case _: InputAdapter => false
      case _: WholeStageCodegenExec => false
      case r: ReusedExchangeExec => isVanillaColumnarOp(r.child)
      case _ => !plan.isInstanceOf[GlutenPlan] && plan.supportsColumnar
    }
  }

  def isGlutenColumnarOp(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[GlutenPlan]
  }
}
