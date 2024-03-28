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
package org.apache.gluten.backendsapi

import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.validate.NativePlanValidationInfo

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType

/**
 * Determine if a plan or expression can be accepted by the backend, or we fallback the execution to
 * vanilla Spark.
 */
trait ValidatorApi {

  /**
   * Validate expression for specific backend, including input type. If the expression isn't
   * implemented by the backend or it returns mismatched results with Vanilla Spark, it will fall
   * back to Vanilla Spark.
   *
   * @return
   *   true by default
   */
  def doExprValidate(substraitExprName: String, expr: Expression): Boolean = true

  /** Validate against a whole Spark plan, before being interpreted by Gluten. */
  def doSparkPlanValidate(plan: SparkPlan): Boolean

  /** Validate against Substrait plan node in native backend. */
  def doNativeValidateWithFailureReason(plan: PlanNode): NativePlanValidationInfo

  /** Validate against Compression method, such as bzip2. */
  def doCompressionSplittableValidate(compressionMethod: String): Boolean = false

  /**
   * Validate the input schema. Transformers like UnionExecTransformer that do not generate
   * Substrait plan need to validate the input schema and fall back if there are any unsupported
   * types.
   *
   * @return
   *   An option contain a validation failure reason, none means ok
   */
  def doSchemaValidate(schema: DataType): Option[String] = None

  /** Validate against ColumnarShuffleExchangeExec. */
  def doColumnarShuffleExchangeExecValidate(
      outputPartitioning: Partitioning,
      child: SparkPlan): Option[String]
}
