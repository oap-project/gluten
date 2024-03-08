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

import io.glutenproject.extension.ValidationResult
import io.glutenproject.substrait.{JoinParams, SubstraitContext}
import io.glutenproject.utils.CHJoinValidateUtil

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution._

case class CHSortMergeJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false,
    projectList: Seq[NamedExpression] = null)
  extends SortMergeJoinExecTransformerBase(
    leftKeys,
    rightKeys,
    joinType,
    condition,
    left,
    right,
    isSkewJoin,
    projectList) {

  override protected def doValidateInternal(): ValidationResult = {
    val shouldFallback =
      CHJoinValidateUtil.shouldFallback(joinType, left.outputSet, right.outputSet, condition, true)
    if (shouldFallback) {
      return ValidationResult.notOk("ch join validate fail")
    }
    super.doValidateInternal()
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    // ClickHouse MergeJoinTransform use nulls biggest and not configurable,
    // While spark use nulls smallest.So need adjust here
    def adjustNullsOrder(plan: SparkPlan): SortExecTransformer = {
      assert(plan.isInstanceOf[SortExecTransformer])
      val adjustedStreamdPlan = plan.asInstanceOf[SortExecTransformer]
      SortExecTransformer(
        adjustedStreamdPlan.sortOrder,
        adjustedStreamdPlan.global,
        adjustedStreamdPlan.child,
        adjustedStreamdPlan.testSpillFrequency,
        true)
    }
    val streamedPlanContext = adjustNullsOrder(streamedPlan).doTransform(context)
    val (inputStreamedRelNode, inputStreamedOutput) =
      (streamedPlanContext.root, streamedPlanContext.outputAttributes)

    val bufferedPlanContext = adjustNullsOrder(bufferedPlan).doTransform(context)
    val (inputBuildRelNode, inputBuildOutput) =
      (bufferedPlanContext.root, bufferedPlanContext.outputAttributes)

    // Get the operator id of this Join.
    val operatorId = context.nextOperatorId(this.nodeName)

    val joinParams = new JoinParams
    if (JoinUtils.preProjectionNeeded(leftKeys)) {
      joinParams.streamPreProjectionNeeded = true
    }
    if (JoinUtils.preProjectionNeeded(rightKeys)) {
      joinParams.buildPreProjectionNeeded = true
    }

    val joinRel = JoinUtils.createJoinRel(
      streamedKeys,
      bufferedKeys,
      condition,
      substraitJoinType,
      false,
      joinType,
      genJoinParameters(),
      inputStreamedRelNode,
      inputBuildRelNode,
      inputStreamedOutput,
      inputBuildOutput,
      context,
      operatorId
    )

    context.registerJoinParam(operatorId, joinParams)

    JoinUtils.createTransformContext(false, output, joinRel, inputStreamedOutput, inputBuildOutput)
  }
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): CHSortMergeJoinExecTransformer =
    copy(left = newLeft, right = newRight)
}
