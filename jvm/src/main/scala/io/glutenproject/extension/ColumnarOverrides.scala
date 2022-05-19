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

package io.glutenproject.extension

import io.glutenproject.{GlutenConfig, GlutenSparkExtensionsInjector}
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution._
import io.glutenproject.extension.columnar.{RowGuard, TransformGuardRule}
import org.apache.spark.SparkConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, V2CommandExec}
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf

// This rule will conduct the conversion from Spark plan to the plan transformer.
// The plan with a row guard on the top of it will not be converted.
case class TransformPreOverrides() extends Rule[SparkPlan] {
  val columnarConf: GlutenConfig = GlutenConfig.getSessionConf
  var isSupportAdaptive: Boolean = true

  def replaceWithTransformerPlan(plan: SparkPlan): SparkPlan = plan match {
    case RowGuard(child: AQEShuffleReadExec) =>
      replaceWithTransformerPlan(child)
    case plan: RowGuard =>
      val actualPlan = plan.child
      logDebug(s"Columnar Processing for ${actualPlan.getClass} is under RowGuard.")
      actualPlan.withNewChildren(actualPlan.children.map(replaceWithTransformerPlan))
    /* case plan: ArrowEvalPythonExec =>
      val columnarChild = replaceWithTransformerPlan(plan.child)
      ArrowEvalPythonExecTransformer(plan.udfs, plan.resultAttrs, columnarChild, plan.evalType) */
    case plan: BatchScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new BatchScanExecTransformer(plan.output, plan.scan, plan.runtimeFilters)
    case plan: FileSourceScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new FileSourceScanExecTransformer(plan.relation,
        plan.output,
        plan.requiredSchema,
        plan.partitionFilters,
        plan.optionalBucketSet,
        plan.optionalNumCoalescedBuckets,
        plan.dataFilters,
        plan.tableIdentifier,
        plan.disableBucketedScan)
    case plan: CoalesceExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      CoalesceExecTransformer(plan.numPartitions, replaceWithTransformerPlan(plan.child))
    case plan: InMemoryTableScanExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarInMemoryTableScanExec(plan.attributes, plan.predicates, plan.relation)
    case plan: ProjectExec =>
      val columnarChild = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ProjectExecTransformer(plan.projectList, columnarChild)
    case plan: FilterExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      FilterExecTransformer(plan.condition, child)
    case plan: HashAggregateExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      HashAggregateExecTransformer(
        plan.requiredChildDistributionExpressions,
        plan.groupingExpressions,
        plan.aggregateExpressions,
        plan.aggregateAttributes,
        plan.initialInputBufferOffset,
        plan.resultExpressions,
        child)
    case plan: UnionExec =>
      val children = plan.children.map(replaceWithTransformerPlan)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      UnionExecTransformer(children)
    case plan: ExpandExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ExpandExecTransformer(plan.projections, plan.output, child)
    case plan: SortExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      SortExecTransformer(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
    case plan: ShuffleExchangeExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      if ((child.supportsColumnar || columnarConf.enablePreferColumnar) &&
           columnarConf.enableColumnarShuffle) {
        if (isSupportAdaptive) {
          new ColumnarShuffleExchangeAdaptor(
            plan.outputPartitioning,
            child)
        } else {
          CoalesceBatchesExec(
            ColumnarShuffleExchangeExec(
              plan.outputPartitioning,
              child))
        }
      } else {
        plan.withNewChildren(Seq(child))
      }
    case plan: ShuffledHashJoinExec =>
      val left = replaceWithTransformerPlan(plan.left)
      val right = replaceWithTransformerPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ShuffledHashJoinExecTransformer(
        plan.leftKeys,
        plan.rightKeys,
        plan.joinType,
        plan.buildSide,
        plan.condition,
        left,
        right)
    case plan: SortMergeJoinExec =>
      val left = replaceWithTransformerPlan(plan.left)
      val right = replaceWithTransformerPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      SortMergeJoinExecTransformer(
        plan.leftKeys,
        plan.rightKeys,
        plan.joinType,
        plan.condition,
        left,
        right,
        plan.isSkewJoin)
    case plan: ShuffleQueryStageExec =>
      logDebug(s"Columnar Processing for ${plan.getClass} is currently not supported.")
      plan
    case plan: AQEShuffleReadExec if columnarConf.enableColumnarShuffle =>
      plan.child match {
        case shuffle: ColumnarShuffleExchangeAdaptor =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          CoalesceBatchesExec(
            ColumnarAQEShuffleReadExec(plan.child, plan.partitionSpecs))
        //case ShuffleQueryStageExec(_, shuffle: ColumnarShuffleExchangeAdaptor) =>
        //  logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        //  CoalesceBatchesExec(
        //    ColumnarAQEShuffleReadExec(plan.child, plan.partitionSpecs))
        //case ShuffleQueryStageExec(_, reused: ReusedExchangeExec) =>
        //  reused match {
        //    case ReusedExchangeExec(_, shuffle: ColumnarShuffleExchangeAdaptor) =>
        //      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        //      CoalesceBatchesExec(
        //        ColumnarAQEShuffleReadExec(
        //          plan.child,
        //          plan.partitionSpecs))
        //    case _ =>
        //      plan
        //  }
        case _ =>
          plan
      }
    case plan: WindowExec =>
      WindowExecTransformer(
        plan.windowExpression,
        plan.partitionSpec,
        plan.orderSpec,
        replaceWithTransformerPlan(plan.child))
    case plan: BroadcastQueryStageExec =>
      plan
    case plan: BroadcastExchangeExec =>
      val child = replaceWithTransformerPlan(plan.child)
      plan.withNewChildren(Seq(child))
    case p =>
      val children = plan.children.map(replaceWithTransformerPlan)
      logDebug(s"Transformation for ${p.getClass} is currently not supported.")
      p.withNewChildren(children)
  }
  def setAdaptiveSupport(enable: Boolean): Unit = { isSupportAdaptive = enable }

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithTransformerPlan(plan)
  }
}

// This rule will try to convert the row-to-columnar and columnar-to-row
// into columnar implementations.
case class TransformPostOverrides() extends Rule[SparkPlan] {
  val columnarConf = GlutenConfig.getSessionConf
  var isSupportAdaptive: Boolean = true

  def replaceWithTransformerPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithTransformerPlan(plan.child)
      logDebug(s"ColumnarPostOverrides RowToArrowColumnarExec(${child.getClass})")
        BackendsApiManager.getSparkPlanExecApiInstance.genRowToArrowColumnarExec(child)
    case ColumnarToRowExec(child: ColumnarShuffleExchangeAdaptor) =>
      replaceWithTransformerPlan(child)
    case ColumnarToRowExec(child: CoalesceBatchesExec) =>
      plan.withNewChildren(Seq(replaceWithTransformerPlan(child.child)))
    case plan: ColumnarToRowExec =>
      if (columnarConf.enableNativeColumnarToRow) {
        val child = replaceWithTransformerPlan(plan.child)
        logDebug(s"ColumnarPostOverrides NativeColumnarToRowExec(${child.getClass})")
        val nativeConversion =
          BackendsApiManager.getSparkPlanExecApiInstance.genNativeColumnarToRowExec(child)
        if (nativeConversion.doValidate()) {
            nativeConversion
          } else {
            logInfo("NativeColumnarToRow : Falling back to ColumnarToRow...")
            plan.withNewChildren(plan.children.map(replaceWithTransformerPlan))
          }
      } else {
        val children = plan.children.map(replaceWithTransformerPlan)
        plan.withNewChildren(children)
      }
    case r: SparkPlan
        if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar && r.children.exists(c =>
          c.isInstanceOf[ColumnarToRowExec]) =>
      // This is a fix for when DPP and AQE both enabled,
      // ColumnarExchange maybe child as a Row SparkPlan
      val children = r.children.map {
        case c: ColumnarToRowExec =>
          if (columnarConf.enableNativeColumnarToRow) {
            val child = replaceWithTransformerPlan(c.child)
            val nativeConversion =
              BackendsApiManager.getSparkPlanExecApiInstance.genNativeColumnarToRowExec(child)
            if (nativeConversion.doValidate()) {
              nativeConversion
            } else {
              logInfo("NativeColumnarToRow : Falling back to ColumnarToRow...")
              c.withNewChildren(c.children.map(replaceWithTransformerPlan))
            }
          } else {
            c.withNewChildren(c.children.map(replaceWithTransformerPlan))
          }
        case other =>
          replaceWithTransformerPlan(other)
      }
      r.withNewChildren(children)
    case p =>
      val children = p.children.map(replaceWithTransformerPlan)
      p.withNewChildren(children)
  }

  def setAdaptiveSupport(enable: Boolean): Unit = { isSupportAdaptive = enable }

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithTransformerPlan(plan)
  }

}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {
  def nativeEngineEnabled: Boolean = GlutenConfig.getSessionConf.enableNativeEngine
  def conf: SparkConf = session.sparkContext.getConf

  // Do not create rules in class initialization as we should access SQLConf
  // while creating the rules. At this time SQLConf may not be there yet.
  def rowGuardOverrides: TransformGuardRule = TransformGuardRule()
  def preOverrides: TransformPreOverrides = TransformPreOverrides()
  def postOverrides: TransformPostOverrides = TransformPostOverrides()

  val columnarWholeStageEnabled: Boolean = conf.getBoolean(
    "spark.gluten.sql.columnar.wholestagetransform", defaultValue = true)
  val isCH: Boolean = conf.get(GlutenConfig.GLUTEN_BACKEND_LIB, "")
    .equalsIgnoreCase(GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)
  def collapseOverrides: ColumnarCollapseCodegenStages =
    ColumnarCollapseCodegenStages(columnarWholeStageEnabled)

  var isSupportAdaptive: Boolean = true

  private def supportAdaptive(plan: SparkPlan): Boolean = {
    // TODO migrate dynamic-partition-pruning onto adaptive execution.
    // Only QueryStage will have Exchange as Leaf Plan
    val isLeafPlanExchange = plan match {
      case e: Exchange => true
      case other => false
    }
    isLeafPlanExchange || (SQLConf.get.adaptiveExecutionEnabled && (sanityCheck(plan) &&
    !plan.logicalLink.exists(_.isStreaming) &&
    !plan.expressions.exists(_.find(_.isInstanceOf[DynamicPruningSubquery]).isDefined) &&
    plan.children.forall(supportAdaptive)))
  }

  private def sanityCheck(plan: SparkPlan): Boolean =
    plan.logicalLink.isDefined



  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    // TODO: Currently there are some fallback issues on CH backend when SparkPlan is
    // TODO: SerializeFromObjectExec, ObjectHashAggregateExec and V2CommandExec.
    // For example:
    //   val tookTimeArr = Array(12, 23, 56, 100, 500, 20)
    //   import spark.implicits._
    //   val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
    //   df.summary().show(100, false)
    val chSupported = isCH && nativeEngineEnabled &&
      !plan.isInstanceOf[SerializeFromObjectExec] &&
      !plan.isInstanceOf[ObjectHashAggregateExec] &&
      !plan.isInstanceOf[V2CommandExec]
    val otherSupported = !isCH && nativeEngineEnabled
    if (chSupported || otherSupported) {
      isSupportAdaptive = supportAdaptive(plan)
      val rule = preOverrides
      rule.setAdaptiveSupport(isSupportAdaptive)
      rule(rowGuardOverrides(plan))
    } else {
      plan
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    // TODO: same as above.
    val chSupported = isCH && nativeEngineEnabled &&
      !plan.isInstanceOf[SerializeFromObjectExec] &&
      !plan.isInstanceOf[ObjectHashAggregateExec] &&
      !plan.isInstanceOf[V2CommandExec]
    val otherSupported = !isCH && nativeEngineEnabled
    if (chSupported || otherSupported) {
      val rule = postOverrides
      rule.setAdaptiveSupport(isSupportAdaptive)
      val tmpPlan = rule(plan)
      collapseOverrides(tmpPlan)
    } else {
      plan
    }
  }
}

object ColumnarOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(ColumnarOverrideRules)
  }
}
