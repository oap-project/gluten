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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.{BackendsApiManager, ValidatorApi}
import org.apache.gluten.expression.ExpressionConverter
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.SelectionNode
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.utils.CHExpressionUtil
import org.apache.gluten.validate.NativePlanValidationInfo
import org.apache.gluten.vectorized.CHNativeExpressionEvaluator

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.utils.RangePartitionerBoundsGenerator
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.delta.DeltaLogFileIndex
import org.apache.spark.sql.execution.{CommandResultExec, FileSourceScanExec, RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

class CHValidatorApi extends ValidatorApi with AdaptiveSparkPlanHelper with Logging {

  override def doNativeValidateWithFailureReason(plan: PlanNode): NativePlanValidationInfo = {
    val validator = new CHNativeExpressionEvaluator()
    if (validator.doValidate(plan.toProtobuf.toByteArray)) {
      new NativePlanValidationInfo(1, "")
    } else {
      new NativePlanValidationInfo(0, "CH native check failed.")
    }
  }

  /**
   * Validate expression for specific backend, including input type. If the expression isn't
   * implemented by the backend or it returns mismatched results with Vanilla Spark, it will fall
   * back to Vanilla Spark.
   *
   * @return
   *   true by default
   */
  override def doExprValidate(substraitExprName: String, expr: Expression): Boolean = {
    CHExpressionUtil.CH_BLACKLIST_SCALAR_FUNCTION.get(substraitExprName) match {
      case Some(validator) =>
        return validator.doValidate(expr)
      case _ =>
    }
    CHExpressionUtil.CH_AGGREGATE_FUNC_BLACKLIST.get(substraitExprName) match {
      case Some(validator) =>
        return validator.doValidate(expr)
      case _ =>
    }
    true
  }

  /** Validate against a whole Spark plan, before being interpreted by Gluten. */
  override def doSparkPlanValidate(plan: SparkPlan): Boolean = {
    // TODO: Currently there are some fallback issues on CH backend when SparkPlan is
    // TODO: SerializeFromObjectExec, ObjectHashAggregateExec and V2CommandExec.
    // For example:
    //   val tookTimeArr = Array(12, 23, 56, 100, 500, 20)
    //   import spark.implicits._
    //   val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
    //   df.summary().show(100, false)

    def includedDeltaOperator(scanExec: FileSourceScanExec): Boolean = {
      scanExec.relation.location.isInstanceOf[DeltaLogFileIndex]
    }

    val includedUnsupportedPlans = collect(plan) {
      // case s: SerializeFromObjectExec => true
      // case d: DeserializeToObjectExec => true
      // case o: ObjectHashAggregateExec => true
      case rddScanExec: RDDScanExec if rddScanExec.nodeName.contains("Delta Table State") => true
      case f: FileSourceScanExec if includedDeltaOperator(f) => true
      case v2CommandExec: V2CommandExec => true
      case commandResultExec: CommandResultExec => true
    }

    !includedUnsupportedPlans.contains(true)
  }

  /** Validate whether the compression method support splittable at clickhouse backend. */
  override def doCompressionSplittableValidate(compressionMethod: String): Boolean = {
    false
  }

  override def doColumnarShuffleExchangeExecValidate(
      outputPartitioning: Partitioning,
      child: SparkPlan): Option[String] = {
    val outputAttributes = child.output
    // check repartition expression
    val substraitContext = new SubstraitContext
    outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        val allSelectionNodes = exprs.forall {
          expr =>
            val node = ExpressionConverter
              .replaceWithExpressionTransformer(expr, outputAttributes)
              .doTransform(substraitContext.registeredFunction)
            node.isInstanceOf[SelectionNode]
        }
        if (
          allSelectionNodes ||
          BackendsApiManager.getSettings.supportShuffleWithProject(outputPartitioning, child)
        ) {
          None
        } else {
          Some("expressions are not supported in HashPartitioning")
        }
      case rangePartitoning: RangePartitioning =>
        if (
          GlutenConfig.getConf.enableColumnarSort &&
          RangePartitionerBoundsGenerator.supportedOrderings(rangePartitoning, child)
        ) {
          None
        } else {
          Some("do not support range partitioning columnar sort")
        }
      case _ => None
    }
  }
}
