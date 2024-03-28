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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.execution.{ProjectExecTransformer, SortExecTransformer, TransformSupport, WholeStageTransformer}
import org.apache.gluten.execution.datasource.GlutenFormatWriterInjects
import org.apache.gluten.extension.ColumnarOverrideRules
import org.apache.gluten.extension.columnar.AddTransformHintRule
import org.apache.gluten.extension.columnar.MiscColumnarRules.TransformPreOverrides

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, SparkPlan}
import org.apache.spark.sql.execution.ColumnarCollapseTransformStages.transformStageCounter
import org.apache.spark.sql.execution.datasources.GlutenWriterColumnarRules.NativeWritePostRule

trait GlutenFormatWriterInjectsBase extends GlutenFormatWriterInjects {

  /**
   * FileFormatWriter wraps some Project & Sort on the top of the original output spark plan, we
   * need to replace them with Columnar version
   * @param plan
   *   must be a FakeRowAdaptor
   * @return
   */
  override def executeWriterWrappedSparkPlan(plan: SparkPlan): RDD[InternalRow] = {
    if (plan.isInstanceOf[FakeRowAdaptor]) {
      // here, the FakeRowAdaptor is simply a R2C converter
      return plan.execute()
    }

    val rules = List(
      ColumnarOverrideRules.rewriteSparkPlanRule(),
      AddTransformHintRule(),
      TransformPreOverrides()
    )
    val transformed = rules.foldLeft(plan) { case (latestPlan, rule) => rule.apply(latestPlan) }
    if (!transformed.isInstanceOf[TransformSupport]) {
      throw new IllegalStateException(
        "Cannot transform the SparkPlans wrapped by FileFormatWriter, " +
          "consider disabling native writer to workaround this issue.")
    }

    def injectAdapter(p: SparkPlan): SparkPlan = p match {
      case p: ProjectExecTransformer => p.mapChildren(injectAdapter)
      case s: SortExecTransformer => s.mapChildren(injectAdapter)
      case _ => ColumnarCollapseTransformStages.wrapInputIteratorTransformer(p)
    }

    // why materializeInput = true? this is for CH backend.
    // in this wst, a Sort will be executed by the underlying native engine.
    // In CH, a SortingTransform cannot handle Const Columns,
    // unless it can get its const-ness from input's header
    // and use const_columns_to_remove to skip const columns.
    // Unfortunately, in our case this wst's input is SourceFromJavaIter
    // and cannot provide const-ness.
    val transformedWithAdapter = injectAdapter(transformed)
    val wst = WholeStageTransformer(transformedWithAdapter, materializeInput = true)(
      transformStageCounter.incrementAndGet())
    FakeRowAdaptor(wst).execute()
  }

  override def getExtendedColumnarPostRule(session: SparkSession): Rule[SparkPlan] = {
    NativeWritePostRule(session)
  }
}
