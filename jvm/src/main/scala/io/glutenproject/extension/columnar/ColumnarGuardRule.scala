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

package io.glutenproject.extension.columnar

import io.glutenproject.GlutenConfig
import io.glutenproject.execution._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec

// A guard to prevent a plan being converted into the plan transformer.
case class RowGuard(child: SparkPlan) extends UnaryExecNode {

  def output: Seq[Attribute] = child.output

  protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override protected def withNewChildInternal(newChild: SparkPlan): RowGuard =
    copy(child = newChild)
}

// This rule will try to convert a plan into plan transformer.
// The doValidate function will be called to check if the conversion is supported.
// If false is returned or any unsupported exception is thrown, a row guard will
// be added on the top of that plan to prevent actual conversion.
case class TransformGuardRule() extends Rule[SparkPlan] {
  val columnarConf: GlutenConfig = GlutenConfig.getSessionConf
  val preferColumnar: Boolean = columnarConf.enablePreferColumnar
  val optimizeLevel: Integer = columnarConf.joinOptimizationThrottle
  val enableColumnarShuffle: Boolean = columnarConf.enableColumnarShuffle
  val enableColumnarSort: Boolean = columnarConf.enableColumnarSort
  val enableColumnarWindow: Boolean = columnarConf.enableColumnarWindow
  val enableColumnarSortMergeJoin: Boolean = columnarConf.enableColumnarSortMergeJoin
  val enableColumnarBatchScan: Boolean = columnarConf.enableColumnarBatchScan
  val enableColumnarFileScan: Boolean = columnarConf.enableColumnarFileScan
  val enableColumnarProject: Boolean = columnarConf.enableColumnarProject
  val enableColumnarFilter: Boolean = columnarConf.enableColumnarFilter
  val enableColumnarHashAgg: Boolean = columnarConf.enableColumnarHashAgg
  val enableColumnarUnion: Boolean = columnarConf.enableColumnarUnion
  val enableColumnarExpand: Boolean = columnarConf.enableColumnarExpand
  val enableColumnarShuffledHashJoin: Boolean = columnarConf.enableColumnarShuffledHashJoin
  val enableColumnarBroadcastExchange: Boolean = columnarConf.enableColumnarBroadcastExchange
  val enableColumnarBroadcastJoin: Boolean = columnarConf.enableColumnarBroadcastJoin
  val enableColumnarArrowUDF: Boolean = columnarConf.enableColumnarArrowUDF

  private def tryConvertToTransformer(plan: SparkPlan): Boolean = {
    try {
      plan match {
        /* case plan: ArrowEvalPythonExec =>
          if (!enableColumnarArrowUDF) return false
          val transformer = ArrowEvalPythonExecTransformer(
            plan.udfs, plan.resultAttrs, plan.child, plan.evalType)
          if (!transformer.doValidate()) return false
          transformer */
        case plan: BatchScanExec =>
          if (!enableColumnarBatchScan) return false
          val transformer = new BatchScanExecTransformer(plan.output, plan.scan, plan.runtimeFilters)
          transformer.doValidate()
        case plan: FileSourceScanExec =>
          if (!enableColumnarFileScan) return false
          val transformer = new FileSourceScanExecTransformer(plan.relation,
            plan.output,
            plan.requiredSchema,
            plan.partitionFilters,
            plan.optionalBucketSet,
            plan.optionalNumCoalescedBuckets,
            plan.dataFilters,
            plan.tableIdentifier,
            plan.disableBucketedScan)
          transformer.doValidate()
        case plan: InMemoryTableScanExec =>
          false
        case plan: ProjectExec =>
          if (!enableColumnarProject) return false
          val transformer = ProjectExecTransformer(plan.projectList, plan.child)
          transformer.doValidate()
        case plan: FilterExec =>
          if (!enableColumnarFilter) return false
          plan.child match {
            case batchScan: BatchScanExec =>
              val childTransformer = new BatchScanExecTransformer(batchScan.output, batchScan.scan, batchScan.runtimeFilters)
              if (childTransformer.doValidate()) {
                // If the BatchScan passes validation, all the filters can be pushed down and
                // the computing of this Filter is not needed.
                return true
              }
            case _ =>
          }
          val transformer = FilterExecTransformer(plan.condition, plan.child)
          transformer.doValidate()
        case plan: HashAggregateExec =>
          if (!enableColumnarHashAgg) return false
          val transformer = HashAggregateExecTransformer(
            plan.requiredChildDistributionExpressions,
            plan.groupingExpressions,
            plan.aggregateExpressions,
            plan.aggregateAttributes,
            plan.initialInputBufferOffset,
            plan.resultExpressions,
            plan.child)
          transformer.doValidate()
        case plan: UnionExec =>
          if (!enableColumnarUnion) return false
          val transformer = UnionExecTransformer(plan.children)
          transformer.doValidate()
        case plan: ExpandExec =>
          if (!enableColumnarExpand) return false
          val transformer = ExpandExecTransformer(plan.projections, plan.output, plan.child)
          transformer.doValidate()
        case plan: SortExec =>
          if (!enableColumnarSort) return false
          val transformer = SortExecTransformer(
            plan.sortOrder, plan.global, plan.child, plan.testSpillFrequency)
          transformer.doValidate()
        case plan: ShuffleExchangeExec =>
          if (!enableColumnarShuffle) return false
          val exec = new ColumnarShuffleExchangeExec(
            plan.outputPartitioning,
            plan.child)
          exec.doValidate()
        case plan: ShuffledHashJoinExec =>
          if (!enableColumnarShuffledHashJoin) return false
          val transformer = ShuffledHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            plan.left,
            plan.right)
          transformer.doValidate()
        case plan: SortMergeJoinExec =>
          if (!enableColumnarSortMergeJoin || plan.joinType == FullOuter) return false
          val transformer = SortMergeJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.condition,
            plan.left,
            plan.right,
            plan.isSkewJoin)
          transformer.doValidate()
        case plan: WindowExec =>
          if (!enableColumnarWindow) return false
          val transformer = WindowExecTransformer(
            plan.windowExpression,
            plan.partitionSpec,
            plan.orderSpec,
            plan.child)
          transformer.doValidate()
        case plan: CoalesceExec =>
          val transformer = CoalesceExecTransformer(plan.numPartitions, plan.child)
          transformer.doValidate()
        case _ =>
          true
      }
    } catch {
      case e: UnsupportedOperationException =>
        System.out.println(
          s"Fall back to use row-based operators, error is ${e.getMessage}," +
            s"original sparkplan is ${plan.getClass}(${plan.children.toList.map(_.getClass)})")
        false
    }
  }

  private def insertRowGuard(plan: SparkPlan): SparkPlan = {
    RowGuard(plan.withNewChildren(plan.children.map(insertRowGuardOrNot)))
  }

  /**
   * Inserts a RowGuard on top of those that are not supported.
   */
  private def insertRowGuardOrNot(plan: SparkPlan): SparkPlan = {
    plan match {
      // For operators that will output domain object, do not insert WholeStageCodegen for it as
      // domain object can not be written into unsafe row.
      case plan if !tryConvertToTransformer(plan) =>
        insertRowGuard(plan)
      case p: BroadcastQueryStageExec =>
        p
      case other =>
        other.withNewChildren(other.children.map(insertRowGuardOrNot))
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    insertRowGuardOrNot(plan)
  }
}
