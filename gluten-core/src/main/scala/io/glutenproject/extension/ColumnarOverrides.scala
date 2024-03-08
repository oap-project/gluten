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
import io.glutenproject.extension.columnar._
import io.glutenproject.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, TransformPostOverrides, TransformPreOverrides}
import io.glutenproject.metrics.GlutenTimeMetric
import io.glutenproject.utils.{LogLevelUtil, PhysicalPlanSelector, PlanUtil}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SparkRuleUtil

import scala.collection.mutable.ListBuffer

private[extension] object ColumnarToRowLike {
  def unapply(plan: SparkPlan): Option[SparkPlan] = {
    plan match {
      case c2r: ColumnarToRowTransition =>
        Some(c2r.child)
      case _ => None
    }
  }
}
// This rule will try to add RowToColumnarExecBase and ColumnarToRowExec
// to support vanilla columnar operators.
case class InsertColumnarToColumnarTransitions(session: SparkSession) extends Rule[SparkPlan] {
  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  private def replaceWithVanillaColumnarToRow(p: SparkPlan): SparkPlan = p.transformUp {
    case plan if PlanUtil.isGlutenColumnarOp(plan) =>
      plan.withNewChildren(plan.children.map {
        case child if PlanUtil.isVanillaColumnarOp(child) =>
          BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(
            ColumnarToRowExec(child))
        case other => other
      })
  }

  private def replaceWithVanillaRowToColumnar(p: SparkPlan): SparkPlan = p.transformUp {
    case plan if PlanUtil.isVanillaColumnarOp(plan) =>
      plan.withNewChildren(plan.children.map {
        case child if PlanUtil.isGlutenColumnarOp(child) =>
          RowToColumnarExec(
            BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToRowExec(child))
        case other => other
      })
  }

  def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = replaceWithVanillaRowToColumnar(replaceWithVanillaColumnarToRow(plan))
    planChangeLogger.logRule(ruleName, plan, newPlan)
    newPlan
  }
}

object ColumnarOverrideRules {
  val GLUTEN_IS_ADAPTIVE_CONTEXT = "gluten.isAdaptiveContext"

  def rewriteSparkPlanRule(): Rule[SparkPlan] = {
    val rewriteRules = Seq(
      RewriteIn,
      RewriteMultiChildrenCount,
      RewriteCollect,
      RewriteTypedImperativeAggregate,
      PullOutPreProject,
      PullOutPostProject)
    new RewriteSparkPlanRulesManager(rewriteRules)
  }

  // Utilities to infer columnar rule's caller's property:
  // ApplyColumnarRulesAndInsertTransitions#outputsColumnar.

  case class DummyRowOutputExec(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = false
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }

  case class DummyColumnarOutputExec(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = true
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }

  object OutputsColumnarTester {
    def wrap(plan: SparkPlan): SparkPlan = {
      if (plan.supportsColumnar) {
        DummyColumnarOutputExec(plan)
      } else {
        DummyRowOutputExec(plan)
      }
    }

    def inferOutputsColumnar(plan: SparkPlan): Boolean = plan match {
      case DummyRowOutputExec(_) => false
      case RowToColumnarExec(DummyRowOutputExec(_)) => true
      case DummyColumnarOutputExec(_) => true
      case ColumnarToRowExec(DummyColumnarOutputExec(_)) => false
      case _ =>
        throw new IllegalStateException(
          "This should not happen. Please leave a issue at" +
            " https://github.com/apache/incubator-gluten.")
    }

    def unwrap(plan: SparkPlan): SparkPlan = plan match {
      case DummyRowOutputExec(child) => child
      case RowToColumnarExec(DummyRowOutputExec(child)) => child
      case DummyColumnarOutputExec(child) => child
      case ColumnarToRowExec(DummyColumnarOutputExec(child)) => child
      case _ =>
        throw new IllegalStateException(
          "This should not happen. Please leave a issue at" +
            " https://github.com/apache/incubator-gluten.")
    }
  }
}

case class ColumnarOverrideRules(session: SparkSession)
  extends ColumnarRule
  with Logging
  with LogLevelUtil {

  import ColumnarOverrideRules._

  private lazy val transformPlanLogLevel = GlutenConfig.getConf.transformPlanLogLevel
  @transient private lazy val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  // This is an empirical value, may need to be changed for supporting other versions of spark.
  private val aqeStackTraceIndex = 18

  // Holds the original plan for possible entire fallback.
  private val localOriginalPlans: ThreadLocal[ListBuffer[SparkPlan]] =
    ThreadLocal.withInitial(() => ListBuffer.empty[SparkPlan])
  private val localIsAdaptiveContextFlags: ThreadLocal[ListBuffer[Boolean]] =
    ThreadLocal.withInitial(() => ListBuffer.empty[Boolean])

  // Do not create rules in class initialization as we should access SQLConf
  // while creating the rules. At this time SQLConf may not be there yet.

  // Just for test use.
  def enableAdaptiveContext(): ColumnarOverrideRules = {
    session.sparkContext.setLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT, "true")
    this
  }

  private def isAdaptiveContext: Boolean =
    Option(session.sparkContext.getLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT))
      .getOrElse("false")
      .toBoolean ||
      localIsAdaptiveContextFlags.get().head

  private def setAdaptiveContext(): Unit = {
    val traceElements = Thread.currentThread.getStackTrace
    assert(
      traceElements.length > aqeStackTraceIndex,
      s"The number of stack trace elements is expected to be more than $aqeStackTraceIndex")
    // ApplyColumnarRulesAndInsertTransitions is called by either QueryExecution or
    // AdaptiveSparkPlanExec. So by checking the stack trace, we can know whether
    // columnar rule will be applied in adaptive execution context. This part of code
    // needs to be carefully checked when supporting higher versions of spark to make
    // sure the calling stack has not been changed.
    localIsAdaptiveContextFlags
      .get()
      .prepend(
        traceElements(aqeStackTraceIndex).getClassName
          .equals(AdaptiveSparkPlanExec.getClass.getName))
  }

  private def resetAdaptiveContext(): Unit =
    localIsAdaptiveContextFlags.get().remove(0)

  private def setOriginalPlan(plan: SparkPlan): Unit = {
    localOriginalPlans.get.prepend(plan)
  }

  private def originalPlan: SparkPlan = {
    val plan = localOriginalPlans.get.head
    assert(plan != null)
    plan
  }

  private def resetOriginalPlan(): Unit = localOriginalPlans.get.remove(0)

  /**
   * Rules to let planner create a suggested Gluten plan being sent to `fallbackPolicies` in which
   * the plan will be breakdown and decided to be fallen back or not.
   */
  private def transformRules(outputsColumnar: Boolean): List[SparkSession => Rule[SparkPlan]] = {
    List(
      (_: SparkSession) => RemoveTransitions,
      (spark: SparkSession) => FallbackOnANSIMode(spark),
      (spark: SparkSession) => FallbackMultiCodegens(spark),
      (spark: SparkSession) => PlanOneRowRelation(spark),
      (_: SparkSession) => FallbackEmptySchemaRelation()
    ) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarValidationRules() :::
      List(
        (spark: SparkSession) => MergeTwoPhasesHashBaseAggregate(spark),
        (_: SparkSession) => rewriteSparkPlanRule(),
        (_: SparkSession) => AddTransformHintRule(),
        (_: SparkSession) => FallbackBloomFilterAggIfNeeded(),
        // We are planning to merge rule "TransformPreOverrides" and "InsertTransitions"
        // together. So temporarily have both `InsertTransitions` and `RemoveTransitions`
        // set in there to make sure the rule list (after insert transitions) is compatible
        // with input plans that have C2Rs/R2Cs inserted.
        (_: SparkSession) => TransformPreOverrides(),
        (_: SparkSession) => InsertTransitions(outputsColumnar),
        (_: SparkSession) => RemoveTransitions,
        (_: SparkSession) => RemoveNativeWriteFilesSortAndProject(),
        (spark: SparkSession) => RewriteTransformer(spark),
        (_: SparkSession) => EnsureLocalSortRequirements,
        (_: SparkSession) => CollapseProjectExecTransformer
      ) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarTransformRules() :::
      SparkRuleUtil.extendedColumnarRules(
        session,
        GlutenConfig.getConf.extendedColumnarTransformRules) :::
      List((_: SparkSession) => InsertTransitions(outputsColumnar))
  }

  /**
   * Rules to add wrapper `FallbackNode`s on top of the input plan, as hints to make planner fall
   * back the whole input plan to the original vanilla Spark plan.
   */
  private def fallbackPolicies(): List[SparkSession => Rule[SparkPlan]] = {
    List((_: SparkSession) => ExpandFallbackPolicy(isAdaptiveContext, originalPlan))
  }

  /**
   * Rules applying to non-fallen-back Gluten plans. To do some post cleanup works on the plan to
   * make sure it be able to run and be compatible with Spark's execution engine.
   */
  private def postRules(): List[SparkSession => Rule[SparkPlan]] =
    List(
      (_: SparkSession) => TransformPostOverrides(),
      (s: SparkSession) => InsertColumnarToColumnarTransitions(s),
      (s: SparkSession) => RemoveTopmostColumnarToRow(s, isAdaptiveContext)
    ) :::
      BackendsApiManager.getSparkPlanExecApiInstance.genExtendedColumnarPostRules() :::
      List((_: SparkSession) => ColumnarCollapseTransformStages(GlutenConfig.getConf)) :::
      SparkRuleUtil.extendedColumnarRules(session, GlutenConfig.getConf.extendedColumnarPostRules)

  /*
   * Rules consistently applying to all input plans after all other rules have been applied, despite
   * whether the input plan is fallen back or not.
   */
  private def finalRules(): List[SparkSession => Rule[SparkPlan]] = {
    List(
      // The rule is required despite whether the stage is fallen back or not. Since
      // ColumnarCachedBatchSerializer is statically registered to Spark without a columnar rule
      // when columnar table cache is enabled.
      (s: SparkSession) => RemoveGlutenTableCacheColumnarToRow(s),
      (s: SparkSession) => GlutenFallbackReporter(GlutenConfig.getConf, s),
      (_: SparkSession) => RemoveTransformHintRule()
    )
  }

  private def prepareFallback[T](plan: SparkPlan)(f: SparkPlan => T): T = {
    setAdaptiveContext()
    setOriginalPlan(plan)
    try {
      f(plan)
    } finally {
      resetOriginalPlan()
      resetAdaptiveContext()
    }
  }

  /**
   * Note: Do not implement this API. We basically inject all of Gluten's physical rules through
   * `postColumnarTransitions`.
   *
   * See: https://github.com/oap-project/gluten/pull/4790
   */
  final override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    // To infer caller's property: ApplyColumnarRulesAndInsertTransitions#outputsColumnar.
    OutputsColumnarTester.wrap(plan)
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    val outputsColumnar = OutputsColumnarTester.inferOutputsColumnar(plan)
    val unwrapped = OutputsColumnarTester.unwrap(plan)
    val vanillaPlan = ColumnarTransitions.insertTransitions(unwrapped, outputsColumnar)
    withTransformRules(transformRules(outputsColumnar)).apply(vanillaPlan)
  }

  // Visible for testing.
  def withTransformRules(transformRules: List[SparkSession => Rule[SparkPlan]]): Rule[SparkPlan] =
    plan =>
      PhysicalPlanSelector.maybe(session, plan) {
        val finalPlan = prepareFallback(plan) {
          p =>
            val suggestedPlan = transformPlan(transformRules, p, "transform")
            transformPlan(fallbackPolicies(), suggestedPlan, "fallback") match {
              case FallbackNode(fallbackPlan) =>
                // we should use vanilla c2r rather than native c2r,
                // and there should be no `GlutenPlan` any more,
                // so skip the `postRules()`.
                fallbackPlan
              case plan =>
                transformPlan(postRules(), plan, "post")
            }
        }
        transformPlan(finalRules(), finalPlan, "final")
      }

  private def transformPlan(
      getRules: List[SparkSession => Rule[SparkPlan]],
      plan: SparkPlan,
      step: String) = GlutenTimeMetric.withMillisTime {
    logOnLevel(
      transformPlanLogLevel,
      s"${step}ColumnarTransitions preOverriden plan:\n${plan.toString}")
    val overridden = getRules.foldLeft(plan) {
      (p, getRule) =>
        val rule = getRule(session)
        val newPlan = rule(p)
        planChangeLogger.logRule(rule.ruleName, p, newPlan)
        newPlan
    }
    logOnLevel(
      transformPlanLogLevel,
      s"${step}ColumnarTransitions afterOverriden plan:\n${overridden.toString}")
    overridden
  }(t => logOnLevel(transformPlanLogLevel, s"${step}Transform SparkPlan took: $t ms."))
}

object ColumnarOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(spark => ColumnarOverrideRules(spark))
  }
}
