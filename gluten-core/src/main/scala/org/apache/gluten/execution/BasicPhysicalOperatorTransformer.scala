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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import org.apache.gluten.extension.{GlutenPlan, ValidationResult}
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.utils.StructTypeFWD
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

abstract class FilterExecTransformerBase(val cond: Expression, val input: SparkPlan)
  extends UnaryTransformSupport
  with OrderPreservingNodeShim
  with PartitioningPreservingNodeShim
  with PredicateHelper
  with Logging {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetrics(sparkContext)

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(cond).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetricsUpdater(metrics)

  def getRelNode(
      context: SubstraitContext,
      condExpr: Expression,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    assert(condExpr != null)
    val args = context.registeredFunction
    val condExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(condExpr, attributeSeq = originalInputAttributes)
      .doTransform(args)

    if (!validation) {
      RelBuilder.makeFilterRel(input, condExprNode, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeFilterRel(input, condExprNode, extensionNode, context, operatorId)
    }
  }

  override def output: Seq[Attribute] = {
    child.output.map {
      a =>
        if (a.nullable && notNullAttributes.contains(a.exprId)) {
          a.withNullability(false)
        } else {
          a
        }
    }
  }

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = child.output

  protected def getRemainingCondition: Expression

  override protected def doValidateInternal(): ValidationResult = {
    val remainingCondition = getRemainingCondition
    if (remainingCondition == null) {
      // All the filters can be pushed down and the computing of this Filter
      // is not needed.
      return ValidationResult.ok
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val relNode = getRelNode(
      substraitContext,
      remainingCondition,
      child.output,
      operatorId,
      null,
      validation = true)
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)
    val remainingCondition = getRemainingCondition
    val operatorId = context.nextOperatorId(this.nodeName)
    if (remainingCondition == null) {
      // The computing for this filter is not needed.
      context.registerEmptyRelToOperator(operatorId)
      // Since some columns' nullability will be removed after this filter, we need to update the
      // outputAttributes of child context.
      return TransformContext(childCtx.inputAttributes, output, childCtx.root)
    }
    val currRel = getRelNode(
      context,
      remainingCondition,
      child.output,
      operatorId,
      childCtx.root,
      validation = false)
    assert(currRel != null, "Filter rel should be valid.")
    TransformContext(childCtx.outputAttributes, output, currRel)
  }
}

case class ProjectExecTransformer private (projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryTransformSupport
  with OrderPreservingNodeShim
  with PartitioningPreservingNodeShim
  with PredicateHelper
  with Logging {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetrics(sparkContext)

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    val relNode =
      getRelNode(substraitContext, projectList, child.output, operatorId, null, validation = true)
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetricsUpdater(metrics)

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    if ((projectList == null || projectList.isEmpty) && childCtx != null) {
      // The computing for this project is not needed.
      // the child may be an input adapter and childCtx is null. In this case we want to
      // make a read node with non-empty base_schema.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val currRel =
      getRelNode(context, projectList, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Project Rel should be valid")
    TransformContext(childCtx.outputAttributes, output, currRel)
  }

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  def getRelNode(
      context: SubstraitContext,
      projectList: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    val columnarProjExprs: Seq[ExpressionTransformer] = ExpressionConverter
      .replaceWithExpressionTransformer(projectList, attributeSeq = originalInputAttributes)
    val projExprNodeList = columnarProjExprs.map(_.doTransform(args)).asJava
    val emitStartIndex = originalInputAttributes.size
    if (!validation) {
      RelBuilder.makeProjectRel(input, projExprNodeList, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        input,
        projExprNodeList,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", projectList)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |""".stripMargin
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ProjectExecTransformer =
    copy(child = newChild)
}
object ProjectExecTransformer {
  private def processProjectExecTransformer(
      projectList: Seq[NamedExpression]): Seq[NamedExpression] = {

    // When there is a MergeScalarSubqueries which will create the named_struct with the same name,
    // looks like {'bloomFilter', BF1, 'bloomFilter', BF2}
    // or {'count(1)', count(1)#111L, 'avg(a)', avg(a)#222L, 'count(1)', count(1)#333L},
    // it will cause problem for some backends, e.g. ClickHouse,
    // which cannot tolerate duplicate type names in struct type,
    // so we need to rename 'nameExpr' in the named_struct to make them unique
    // after executing the MergeScalarSubqueries.
    var needToReplace = false
    val newProjectList = projectList.map {
      case alias @ Alias(cns @ CreateNamedStruct(children: Seq[Expression]), "mergedValue") =>
        // check whether there are some duplicate names
        if (cns.nameExprs.distinct.size == cns.nameExprs.size) {
          alias
        } else {
          val newChildren = children
            .grouped(2)
            .flatMap {
              case Seq(name: Literal, value: NamedExpression) =>
                val newLiteral = Literal(name.toString() + "#" + value.exprId.id)
                Seq(newLiteral, value)
              case Seq(name, value) => Seq(name, value)
            }
            .toSeq
          needToReplace = true
          Alias.apply(CreateNamedStruct(newChildren), "mergedValue")(alias.exprId)
        }
      case other: NamedExpression => other
    }

    if (!needToReplace) {
      projectList
    } else {
      newProjectList
    }
  }

  def apply(projectList: Seq[NamedExpression], child: SparkPlan): ProjectExecTransformer =
    new ProjectExecTransformer(processProjectExecTransformer(projectList), child)
}

// An alternatives for UnionExec.
case class ColumnarUnionExec(children: Seq[SparkPlan]) extends SparkPlan with GlutenPlan {
  children.foreach(
    child =>
      child match {
        case w: WholeStageTransformer =>
          w.setOutputSchemaForPlan(output)
        case _ =>
      })

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map {
      attrs =>
        val firstAttr = attrs.head
        val nullable = attrs.exists(_.nullable)
        val newDt = attrs.map(_.dataType).reduce(StructTypeFWD.unionLikeMerge)
        if (firstAttr.dataType == newDt) {
          firstAttr.withNullability(nullable)
        } else {
          AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
            firstAttr.exprId,
            firstAttr.qualifier)
        }
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): ColumnarUnionExec =
    copy(children = newChildren)

  def columnarInputRDD: RDD[ColumnarBatch] = {
    if (children.isEmpty) {
      throw new IllegalArgumentException(s"Empty children")
    }
    sparkContext.union(children.map(c => c.executeColumnar()))
  }

  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = columnarInputRDD

  override protected def doValidateInternal(): ValidationResult = {
    BackendsApiManager.getValidatorApiInstance
      .doSchemaValidate(schema)
      .map {
        reason => ValidationResult.notOk(s"Found schema check failure for $schema, due to: $reason")
      }
      .getOrElse(ValidationResult.ok)
  }
}

/**
 * Contains functions for the comparision and separation of the filter conditions in Scan and
 * Filter. Contains the function to manually push down the conditions into Scan.
 */
object FilterHandler extends PredicateHelper {

  /**
   * Get the original filter conditions in Scan for the comparison with those in Filter.
   *
   * @param plan
   *   : the Spark plan
   * @return
   *   If the plan is FileSourceScanExec or BatchScanExec, return the filter conditions in it.
   *   Otherwise, return empty sequence.
   */
  def getScanFilters(plan: SparkPlan): Seq[Expression] = {
    plan match {
      case fileSourceScan: FileSourceScanExec =>
        fileSourceScan.dataFilters
      case batchScan: BatchScanExec =>
        batchScan.scan match {
          case scan: FileScan =>
            scan.dataFilters
          case _ =>
            throw new GlutenNotSupportException(
              s"${batchScan.scan.getClass.toString} is not supported")
        }
      case _ =>
        Seq()
    }
  }

  /**
   * Compare the semantics of the filter conditions pushed down to Scan and in the Filter.
   *
   * @param scanFilters
   *   : the conditions pushed down into Scan
   * @param filters
   *   : the conditions in the Filter after the Scan
   * @return
   *   the filter conditions not pushed down into Scan.
   */
  def getRemainingFilters(scanFilters: Seq[Expression], filters: Seq[Expression]): Seq[Expression] =
    (ExpressionSet(filters) -- ExpressionSet(scanFilters)).toSeq

  // Separate and compare the filter conditions in Scan and Filter.
  // Try to push down the remaining conditions in Filter into Scan.
  def applyFilterPushdownToScan(filter: FilterExec): SparkPlan =
    filter.child match {
      case fileSourceScan: FileSourceScanExec =>
        val pushDownFilters =
          BackendsApiManager.getSparkPlanExecApiInstance.postProcessPushDownFilter(
            splitConjunctivePredicates(filter.condition),
            fileSourceScan)
        ScanTransformerFactory.createFileSourceScanTransformer(
          fileSourceScan,
          allPushDownFilters = Some(pushDownFilters))
      case batchScan: BatchScanExec =>
        val pushDownFilters =
          BackendsApiManager.getSparkPlanExecApiInstance.postProcessPushDownFilter(
            splitConjunctivePredicates(filter.condition),
            batchScan)
        ScanTransformerFactory.createBatchScanTransformer(
          batchScan,
          allPushDownFilters = Some(pushDownFilters))
      case other =>
        throw new GlutenNotSupportException(s"${other.getClass.toString} is not supported.")
    }
}
