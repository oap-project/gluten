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
package org.apache.gluten.extension.columnar

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.expression.ExpressionUtils.getExpressionTreeDepth
import org.apache.gluten.extension.{GlutenPlan, ValidationResult}
import org.apache.gluten.extension.columnar.TransformHints.EncodeTransformableTagImplicits
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.types.StringType

import org.apache.commons.lang3.exception.ExceptionUtils

sealed trait TransformHint {
  val stacktrace: Option[String] =
    if (TransformHints.DEBUG) {
      Some(ExceptionUtils.getStackTrace(new Throwable()))
    } else None
}

case class TRANSFORM_UNSUPPORTED(reason: Option[String], appendReasonIfExists: Boolean = true)
  extends TransformHint

object TransformHints {
  val TAG: TreeNodeTag[TransformHint] =
    TreeNodeTag[TransformHint]("org.apache.gluten.transformhint")

  val DEBUG = false

  /**
   * If true, the plan node will be guaranteed fallback to Vanilla plan node while being
   * implemented.
   *
   * If false, the plan still has chance to be turned into "non-transformable" in any another
   * validation rule. So user should not consider the plan "transformable" unless all validation
   * rules are passed.
   */
  def isNotTransformable(plan: SparkPlan): Boolean = {
    getHintOption(plan) match {
      case Some(TRANSFORM_UNSUPPORTED(_, _)) => true
      case _ => false
    }
  }

  /**
   * NOTE: To be deprecated. Do not create new usages of this method.
   *
   * Since it's usually not safe to consider a plan "transformable" during validation phase. Another
   * validation rule could turn "transformable" to "non-transformable" before implementing the plan
   * within Gluten transformers.
   */
  def isTransformable(plan: SparkPlan): Boolean = {
    getHintOption(plan) match {
      case None => true
      case _ => false
    }
  }

  def tag(plan: SparkPlan, hint: TransformHint): Unit = {
    val mergedHint = getHintOption(plan)
      .map {
        case originalHint @ TRANSFORM_UNSUPPORTED(Some(originalReason), originAppend) =>
          hint match {
            case TRANSFORM_UNSUPPORTED(Some(newReason), append) =>
              if (originAppend && append) {
                TRANSFORM_UNSUPPORTED(Some(originalReason + "; " + newReason))
              } else if (originAppend) {
                TRANSFORM_UNSUPPORTED(Some(originalReason))
              } else if (append) {
                TRANSFORM_UNSUPPORTED(Some(newReason))
              } else {
                TRANSFORM_UNSUPPORTED(Some(originalReason), false)
              }
            case TRANSFORM_UNSUPPORTED(None, _) =>
              originalHint
            case _ =>
              throw new GlutenNotSupportException(
                "Plan was already tagged as non-transformable, " +
                  s"cannot mark it as transformable after that:\n${plan.toString()}")
          }
        case _ =>
          hint
      }
      .getOrElse(hint)
    plan.setTagValue(TAG, mergedHint)
  }

  def untag(plan: SparkPlan): Unit = {
    plan.unsetTagValue(TAG)
  }

  def tagNotTransformable(plan: SparkPlan, validationResult: ValidationResult): Unit = {
    if (!validationResult.isValid) {
      tag(plan, TRANSFORM_UNSUPPORTED(validationResult.reason))
    }
  }

  def tagNotTransformable(plan: SparkPlan, reason: String): Unit = {
    tag(plan, TRANSFORM_UNSUPPORTED(Some(reason)))
  }

  def tagAllNotTransformable(plan: SparkPlan, hint: TRANSFORM_UNSUPPORTED): Unit = {
    plan.foreach {
      case _: GlutenPlan => // ignore
      case other => tag(other, hint)
    }
  }

  def getHint(plan: SparkPlan): TransformHint = {
    getHintOption(plan).getOrElse(
      throw new IllegalStateException("Transform hint tag not set in plan: " + plan.toString()))
  }

  def getHintOption(plan: SparkPlan): Option[TransformHint] = {
    plan.getTagValue(TAG)
  }

  implicit class EncodeTransformableTagImplicits(validationResult: ValidationResult) {
    def tagOnFallback(plan: SparkPlan): Unit = {
      if (validationResult.isValid) {
        return
      }
      val newTag = TRANSFORM_UNSUPPORTED(validationResult.reason)
      tag(plan, newTag)
    }
  }
}

case class FallbackOnANSIMode(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (GlutenConfig.getConf.enableAnsiMode) {
      plan.foreach(TransformHints.tagNotTransformable(_, "does not support ansi mode"))
    }
    plan
  }
}

case class FallbackMultiCodegens(session: SparkSession) extends Rule[SparkPlan] {
  lazy val columnarConf: GlutenConfig = GlutenConfig.getConf
  lazy val physicalJoinOptimize = columnarConf.enablePhysicalJoinOptimize
  lazy val optimizeLevel: Integer = columnarConf.physicalJoinOptimizationThrottle

  def existsMultiCodegens(plan: SparkPlan, count: Int = 0): Boolean =
    plan match {
      case plan: CodegenSupport if plan.supportCodegen =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.exists(existsMultiCodegens(_, count + 1))
      case plan: ShuffledHashJoinExec =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.exists(existsMultiCodegens(_, count + 1))
      case other => false
    }

  def tagNotTransformable(plan: SparkPlan): SparkPlan = {
    TransformHints.tagNotTransformable(plan, "fallback multi codegens")
    plan
  }

  def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport =>
      plan.supportCodegen
    case _ => false
  }

  def isAQEShuffleReadExec(plan: SparkPlan): Boolean = {
    plan match {
      case _: AQEShuffleReadExec => true
      case _ => false
    }
  }

  def tagNotTransformableRecursive(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: ShuffleExchangeExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens)))
      case p: BroadcastExchangeExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens)))
      case p: ShuffledHashJoinExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableRecursive)))
      case p if !supportCodegen(p) =>
        p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens))
      case p if isAQEShuffleReadExec(p) =>
        p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens))
      case p: QueryStageExec => p
      case p => tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableRecursive)))
    }
  }

  def tagNotTransformableForMultiCodegens(plan: SparkPlan): SparkPlan = {
    plan match {
      case plan if existsMultiCodegens(plan) =>
        tagNotTransformableRecursive(plan)
      case other =>
        other.withNewChildren(other.children.map(tagNotTransformableForMultiCodegens))
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (physicalJoinOptimize) {
      tagNotTransformableForMultiCodegens(plan)
    } else plan
  }
}

/**
 * This rule plans [[RDDScanExec]] with a fake schema to make gluten work, because gluten does not
 * support empty output relation, see [[FallbackEmptySchemaRelation]].
 */
case class PlanOneRowRelation(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.getConf.enableOneRowRelationColumnar) {
      return plan
    }

    plan.transform {
      // We should make sure the output does not change, e.g.
      // Window
      //   OneRowRelation
      case u: UnaryExecNode
          if u.child.isInstanceOf[RDDScanExec] &&
            u.child.asInstanceOf[RDDScanExec].name == "OneRowRelation" &&
            u.outputSet != u.child.outputSet =>
        val rdd = spark.sparkContext.parallelize(InternalRow(null) :: Nil, 1)
        val attr = AttributeReference("fake_column", StringType)()
        u.withNewChildren(RDDScanExec(attr :: Nil, rdd, "OneRowRelation") :: Nil)
    }
  }
}

/**
 * FIXME To be removed: Since Velox backend is the only one to use the strategy, and we already
 * support offloading zero-column batch in ColumnarBatchInIterator via PR #3309.
 *
 * We'd make sure all Velox operators be able to handle zero-column input correctly then remove the
 * rule together with [[PlanOneRowRelation]].
 */
case class FallbackEmptySchemaRelation() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case p =>
      if (BackendsApiManager.getSettings.fallbackOnEmptySchema(p)) {
        if (p.children.exists(_.output.isEmpty)) {
          // Some backends are not eligible to offload plan with zero-column input.
          // If any child have empty output, mark the plan and that child as UNSUPPORTED.
          TransformHints.tagNotTransformable(p, "at least one of its children has empty output")
          p.children.foreach {
            child =>
              if (child.output.isEmpty && !child.isInstanceOf[WriteFilesExec]) {
                TransformHints.tagNotTransformable(
                  child,
                  "at least one of its children has empty output")
              }
          }
        }
      }
      p
  }
}

/**
 * Velox BloomFilter's implementation is different from Spark's. So if might_contain falls back, we
 * need fall back related bloom filter agg.
 */
case class FallbackBloomFilterAggIfNeeded() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan =
    if (
      GlutenConfig.getConf.enableNativeBloomFilter &&
      BackendsApiManager.getSettings.enableBloomFilterAggFallbackRule()
    ) {
      plan.transformDown {
        case p if TransformHints.isNotTransformable(p) =>
          handleBloomFilterFallback(p)
          p
      }
    } else {
      plan
    }

  object SubPlanFromBloomFilterMightContain {
    def unapply(expr: Expression): Option[SparkPlan] =
      SparkShimLoader.getSparkShims.extractSubPlanFromMightContain(expr)
  }

  private def handleBloomFilterFallback(plan: SparkPlan): Unit = {
    def tagNotTransformableRecursive(p: SparkPlan): Unit = {
      p match {
        case agg: org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
            if SparkShimLoader.getSparkShims.hasBloomFilterAggregate(agg) =>
          TransformHints.tagNotTransformable(agg, "related BloomFilterMightContain falls back")
          tagNotTransformableRecursive(agg.child)
        case a: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec =>
          tagNotTransformableRecursive(a.executedPlan)
        case _ =>
          p.children.map(tagNotTransformableRecursive)
      }
    }

    plan.transformExpressions {
      case expr @ SubPlanFromBloomFilterMightContain(p: SparkPlan) =>
        tagNotTransformableRecursive(p)
        expr
    }
  }
}

// This rule will try to convert a plan into plan transformer.
// The doValidate function will be called to check if the conversion is supported.
// If false is returned or any unsupported exception is thrown, a row guard will
// be added on the top of that plan to prevent actual conversion.
case class AddTransformHintRule() extends Rule[SparkPlan] {
  val columnarConf: GlutenConfig = GlutenConfig.getConf
  val scanOnly: Boolean = columnarConf.enableScanOnly
  val enableColumnarShuffle: Boolean =
    !scanOnly && BackendsApiManager.getSettings.supportColumnarShuffleExec()
  val enableColumnarSort: Boolean = !scanOnly && columnarConf.enableColumnarSort
  val enableColumnarWindow: Boolean = !scanOnly && columnarConf.enableColumnarWindow
  val enableColumnarSortMergeJoin: Boolean = !scanOnly &&
    BackendsApiManager.getSettings.supportSortMergeJoinExec()
  val enableColumnarBatchScan: Boolean = columnarConf.enableColumnarBatchScan
  val enableColumnarFileScan: Boolean = columnarConf.enableColumnarFileScan
  val enableColumnarHiveTableScan: Boolean = columnarConf.enableColumnarHiveTableScan
  val enableColumnarProject: Boolean = !scanOnly && columnarConf.enableColumnarProject
  val enableColumnarFilter: Boolean = columnarConf.enableColumnarFilter
  val fallbackExpressionsThreshold: Int = columnarConf.fallbackExpressionsThreshold
  val enableColumnarHashAgg: Boolean = !scanOnly && columnarConf.enableColumnarHashAgg
  val enableColumnarUnion: Boolean = !scanOnly && columnarConf.enableColumnarUnion
  val enableColumnarExpand: Boolean = !scanOnly && columnarConf.enableColumnarExpand
  val enableColumnarShuffledHashJoin: Boolean =
    !scanOnly && columnarConf.enableColumnarShuffledHashJoin
  val enableColumnarBroadcastExchange: Boolean = !scanOnly &&
    columnarConf.enableColumnarBroadcastExchange
  val enableColumnarBroadcastJoin: Boolean = !scanOnly &&
    columnarConf.enableColumnarBroadcastJoin
  val enableColumnarLimit: Boolean = !scanOnly && columnarConf.enableColumnarLimit
  val enableColumnarGenerate: Boolean = !scanOnly && columnarConf.enableColumnarGenerate
  val enableColumnarCoalesce: Boolean = !scanOnly && columnarConf.enableColumnarCoalesce
  val enableTakeOrderedAndProject: Boolean =
    !scanOnly && columnarConf.enableTakeOrderedAndProject &&
      enableColumnarSort && enableColumnarLimit && enableColumnarShuffle && enableColumnarProject
  val enableColumnarWrite: Boolean = BackendsApiManager.getSettings.enableNativeWriteFiles()
  val enableCartesianProduct: Boolean =
    BackendsApiManager.getSettings.supportCartesianProductExec() &&
      columnarConf.cartesianProductTransformerEnabled
  val enableBroadcastNestedLoopJoin: Boolean =
    BackendsApiManager.getSettings.supportBroadcastNestedLoopJoinExec() &&
      columnarConf.broadcastNestedLoopJoinTransformerTransformerEnabled &&
      enableColumnarBroadcastJoin

  def apply(plan: SparkPlan): SparkPlan = {
    addTransformableTags(plan)
  }

  /** Inserts a transformable tag on top of those that are not supported. */
  private def addTransformableTags(plan: SparkPlan): SparkPlan = {
    // Walk the tree with post-order
    val out = plan.mapChildren(addTransformableTags)
    addTransformableTag(out)
    out
  }

  private def addTransformableTag(plan: SparkPlan): Unit = {
    if (TransformHints.isNotTransformable(plan)) {
      logDebug(
        s"Skip adding transformable tag, since plan already tagged as " +
          s"${TransformHints.getHint(plan)}: ${plan.toString()}")
      return
    }
    try {
      plan match {
        case plan: BatchScanExec =>
          if (!enableColumnarBatchScan) {
            TransformHints.tagNotTransformable(plan, "columnar BatchScan is disabled")
          } else {
            // IF filter expressions aren't empty, we need to transform the inner operators.
            if (plan.runtimeFilters.isEmpty) {
              val transformer =
                ScanTransformerFactory
                  .createBatchScanTransformer(plan, validation = true)
                  .asInstanceOf[BasicScanExecTransformer]
              transformer.doValidate().tagOnFallback(plan)
            }
          }
        case plan: FileSourceScanExec =>
          if (!enableColumnarFileScan) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar FileScan is not enabled in FileSourceScanExec")
          } else {
            // IF filter expressions aren't empty, we need to transform the inner operators.
            if (plan.partitionFilters.isEmpty) {
              val transformer =
                ScanTransformerFactory.createFileSourceScanTransformer(plan, validation = true)
              transformer.doValidate().tagOnFallback(plan)
            }
          }
        case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
          if (!enableColumnarHiveTableScan) {
            TransformHints.tagNotTransformable(plan, "columnar hive table scan is disabled")
          } else {
            HiveTableScanExecTransformer.validate(plan).tagOnFallback(plan)
          }
        case plan: ProjectExec =>
          if (!enableColumnarProject) {
            TransformHints.tagNotTransformable(plan, "columnar project is disabled")
          } else if (
            plan.projectList.size > 0 && plan.projectList
              .map(getExpressionTreeDepth(_))
              .max >= fallbackExpressionsThreshold
          ) {
            TransformHints.tagNotTransformable(
              plan,
              "Fall back project plan because its" +
                " max nested expressions number reaches the configured threshold")
          } else {
            val transformer = ProjectExecTransformer(plan.projectList, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: FilterExec =>
          val childIsScan = plan.child.isInstanceOf[FileSourceScanExec] ||
            plan.child.isInstanceOf[BatchScanExec]
          if (!enableColumnarFilter) {
            TransformHints.tagNotTransformable(plan, "columnar Filter is not enabled in FilterExec")
          } else if (getExpressionTreeDepth(plan.condition) >= fallbackExpressionsThreshold) {
            TransformHints.tagNotTransformable(
              plan,
              "Fall back filter plan because its" +
                " nested expressions number reaches the configured threshold")
          } else if (scanOnly && !childIsScan) {
            // When scanOnly is enabled, filter after scan will be offloaded.
            TransformHints.tagNotTransformable(
              plan,
              "ScanOnly enabled and plan child is not Scan in FilterExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genFilterExecTransformer(plan.condition, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: HashAggregateExec =>
          if (!enableColumnarHashAgg) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar HashAggregate is not enabled in HashAggregateExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genHashAggregateExecTransformer(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                plan.child
              )
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: SortAggregateExec =>
          if (!BackendsApiManager.getSettings.replaceSortAggWithHashAgg) {
            TransformHints.tagNotTransformable(plan, "replaceSortAggWithHashAgg is not enabled")
          } else if (!enableColumnarHashAgg) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar HashAgg is not enabled in SortAggregateExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genHashAggregateExecTransformer(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                plan.child
              )
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: ObjectHashAggregateExec =>
          if (!enableColumnarHashAgg) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar HashAgg is not enabled in ObjectHashAggregateExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genHashAggregateExecTransformer(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                plan.child
              )
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: UnionExec =>
          if (!enableColumnarUnion) {
            TransformHints.tagNotTransformable(plan, "columnar Union is not enabled in UnionExec")
          } else {
            val transformer = ColumnarUnionExec(plan.children)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: ExpandExec =>
          if (!enableColumnarExpand) {
            TransformHints.tagNotTransformable(plan, "columnar Expand is not enabled in ExpandExec")
          } else {
            val transformer = ExpandExecTransformer(plan.projections, plan.output, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }

        case plan: WriteFilesExec =>
          if (!enableColumnarWrite || !BackendsApiManager.getSettings.supportTransformWriteFiles) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar Write is not enabled in WriteFilesExec")
          } else {
            val transformer = WriteFilesExecTransformer(
              plan.child,
              plan.fileFormat,
              plan.partitionColumns,
              plan.bucketSpec,
              plan.options,
              plan.staticPartitions)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: SortExec =>
          if (!enableColumnarSort) {
            TransformHints.tagNotTransformable(plan, "columnar Sort is not enabled in SortExec")
          } else {
            val transformer =
              SortExecTransformer(plan.sortOrder, plan.global, plan.child, plan.testSpillFrequency)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: ShuffleExchangeExec =>
          if (!enableColumnarShuffle) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar Shuffle is not enabled in ShuffleExchangeExec")
          } else {
            val transformer = ColumnarShuffleExchangeExec(plan, plan.child, plan.child.output)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: ShuffledHashJoinExec =>
          if (!enableColumnarShuffledHashJoin) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar shufflehashjoin is not enabled in ShuffledHashJoinExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genShuffledHashJoinExecTransformer(
                plan.leftKeys,
                plan.rightKeys,
                plan.joinType,
                plan.buildSide,
                plan.condition,
                plan.left,
                plan.right,
                plan.isSkewJoin)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: BroadcastExchangeExec =>
          // columnar broadcast is enabled only when columnar bhj is enabled.
          if (!enableColumnarBroadcastExchange) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar BroadcastExchange is not enabled in BroadcastExchangeExec")
          } else {
            val transformer = ColumnarBroadcastExchangeExec(plan.mode, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case bhj: BroadcastHashJoinExec =>
          if (!enableColumnarBroadcastJoin) {
            TransformHints.tagNotTransformable(
              bhj,
              "columnar BroadcastJoin is not enabled in BroadcastHashJoinExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genBroadcastHashJoinExecTransformer(
                bhj.leftKeys,
                bhj.rightKeys,
                bhj.joinType,
                bhj.buildSide,
                bhj.condition,
                bhj.left,
                bhj.right,
                isNullAwareAntiJoin = bhj.isNullAwareAntiJoin)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: SortMergeJoinExec =>
          if (!enableColumnarSortMergeJoin || plan.joinType == FullOuter) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar sort merge join is not enabled or join type is FullOuter")
          } else {
            val transformer = SortMergeJoinExecTransformer(
              plan.leftKeys,
              plan.rightKeys,
              plan.joinType,
              plan.condition,
              plan.left,
              plan.right,
              plan.isSkewJoin)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: CartesianProductExec =>
          if (!enableCartesianProduct) {
            TransformHints.tagNotTransformable(
              plan,
              "conversion to CartesianProductTransformer is not enabled.")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genCartesianProductExecTransformer(plan.left, plan.right, plan.condition)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: BroadcastNestedLoopJoinExec =>
          if (!enableBroadcastNestedLoopJoin) {
            TransformHints.tagNotTransformable(
              plan,
              "conversion to BroadcastNestedLoopJoinTransformer is not enabled.")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance
              .genBroadcastNestedLoopJoinExecTransformer(
                plan.left,
                plan.right,
                plan.buildSide,
                plan.joinType,
                plan.condition)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: WindowExec =>
          if (!enableColumnarWindow) {
            TransformHints.tagNotTransformable(plan, "columnar window is not enabled in WindowExec")
          } else {
            val transformer = WindowExecTransformer(
              plan.windowExpression,
              plan.partitionSpec,
              plan.orderSpec,
              plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: CoalesceExec =>
          if (!enableColumnarCoalesce) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar coalesce is not enabled in CoalesceExec")
          } else {
            val transformer = CoalesceExecTransformer(plan.numPartitions, plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: GlobalLimitExec =>
          if (!enableColumnarLimit) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar limit is not enabled in GlobalLimitExec")
          } else {
            val (limit, offset) =
              SparkShimLoader.getSparkShims.getLimitAndOffsetFromGlobalLimit(plan)
            val transformer = LimitTransformer(plan.child, offset, limit)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: LocalLimitExec =>
          if (!enableColumnarLimit) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar limit is not enabled in GlobalLimitExec")
          } else {
            val transformer = LimitTransformer(plan.child, 0L, plan.limit)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: GenerateExec =>
          if (!enableColumnarGenerate) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar generate is not enabled in GenerateExec")
          } else {
            val transformer = BackendsApiManager.getSparkPlanExecApiInstance.genGenerateTransformer(
              plan.generator,
              plan.requiredChildOutput,
              plan.outer,
              plan.generatorOutput,
              plan.child)
            transformer.doValidate().tagOnFallback(plan)
          }
        case plan: EvalPythonExec =>
          val transformer = EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, plan.child)
          transformer.doValidate().tagOnFallback(plan)
        case _: AQEShuffleReadExec =>
        // Considered transformable by default.
        case plan: TakeOrderedAndProjectExec =>
          if (!enableTakeOrderedAndProject) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar topK is not enabled in TakeOrderedAndProjectExec")
          } else {
            val (limit, offset) =
              SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(plan)
            val transformer = TakeOrderedAndProjectExecTransformer(
              limit,
              plan.sortOrder,
              plan.projectList,
              plan.child,
              offset)
            transformer.doValidate().tagOnFallback(plan)
          }
        case _ =>
        // Currently we assume a plan to be transformable by default.
      }
    } catch {
      case e @ (_: GlutenNotSupportException | _: UnsupportedOperationException) =>
        TransformHints.tagNotTransformable(
          plan,
          s"${e.getMessage}, original Spark plan is " +
            s"${plan.getClass}(${plan.children.toList.map(_.getClass)})")
        if (!e.isInstanceOf[GlutenNotSupportException]) {
          logDebug("This exception may need to be fixed: " + e.getMessage)
        }
    }
  }
}

case class RemoveTransformHintRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach(TransformHints.untag)
    plan
  }
}
