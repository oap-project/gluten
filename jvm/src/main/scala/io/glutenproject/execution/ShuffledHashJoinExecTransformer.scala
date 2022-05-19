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

import java.util

import scala.collection.JavaConverters._

import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.GlutenConfig
import io.glutenproject.expression._
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.vectorized.ExpressionEvaluator
import io.substrait.proto.JoinRel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BaseJoinExec, HashJoin, ShuffledJoin}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ColumnarShuffledJoin extends BaseJoinExec {
  def isSkewJoin: Boolean

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }

  //override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).iterator

  override def requiredChildDistribution: Seq[Distribution] = {
    if (isSkewJoin) {
      // We re-arrange the shuffle partitions to deal with skew join, and the new children
      // partitioning doesn't satisfy `HashClusteredDistribution`.
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else {
      HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
    }
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"ShuffledJoin should not take $x as the JoinType")
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        (left.output ++ right.output).map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} not take $x as the JoinType")
    }
  }
}

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class ShuffledHashJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    projectList: Seq[NamedExpression] = null)
    extends BaseJoinExec
    with TransformSupport
    with ColumnarShuffledJoin {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_hashjoin"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"),
    "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "join time"))

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  def isSkewJoin: Boolean = false

  val (buildKeyExprs, streamedKeyExprs) = {
    require(
      leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = HashJoin.rewriteKeyExpr(leftKeys)
    val rkeys = HashJoin.rewriteKeyExpr(rightKeys)
    buildSide match {
      case BuildLeft => (lkeys, rkeys)
      case BuildRight => (rkeys, lkeys)
    }
  }

  private val joinExpression = (streamedKeyExprs zip buildKeyExprs)
    .map { case (l, r) => EqualTo(l, r) }
    .reduce(And)

  // Direct output order of substrait join operation
  private val joinOutput = streamedPlan.output ++ buildPlan.output

  private val substraitJoinType = joinType match {
    case Inner =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter | RightOuter =>
      JoinRel.JoinType.JOIN_TYPE_LEFT
    case LeftSemi =>
      JoinRel.JoinType.JOIN_TYPE_SEMI
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_ANTI
    case _ =>
      // TODO: Support cross join with Cross Rel
      JoinRel.JoinType.UNRECOGNIZED
  }

  override def output: Seq[Attribute] =
    if (projectList == null || projectList.isEmpty) super.output
    else projectList.map(_.toAttribute)

  override def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {
    val numOutputRows = longMetric("numOutputRows")
    val procTime = longMetric("processTime")
    procTime.set(process_time / 1000000)
    numOutputRows += out_num_rows
  }

  override def outputPartitioning: Partitioning = buildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike | RightOuter => right.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin =>
          left.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building right side")
      }
  }

  override def supportsColumnar: Boolean = true

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val getInputRDDs = (plan: SparkPlan) => {
      plan match {
        case c: TransformSupport =>
          c.columnarInputRDDs
        case _ =>
          Seq(plan.executeColumnar())
      }
    }
    getInputRDDs(streamedPlan) ++ getInputRDDs(buildPlan)
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = buildPlan match {
    case c: TransformSupport =>
      val childPlans = c.getBuildPlans
      childPlans :+ (this, null)
    case _ =>
      Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = streamedPlan match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def getChild: SparkPlan = streamedPlan

  override def doValidate(): Boolean = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    if (substraitJoinType == JoinRel.JoinType.UNRECOGNIZED) {
      return false
    }
    val relNode =
      try {
        getJoinRel(null, null, substraitContext, validation = true)
      } catch {
        case e: Throwable =>
          logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
          return false
      }
    // Then, validate the generated plan in native engine.
    if (GlutenConfig.getConf.enableNativeValidation) {
      val validator = new ExpressionEvaluator()
      val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
      validator.doValidate(planNode.toProtobuf.toByteArray)
    } else {
      true
    }
  }

  override def doTransform(substraitContext: SubstraitContext): TransformContext = {
    def transformAndGetOutput(plan: SparkPlan): (RelNode, Seq[Attribute]) = {
      plan match {
        case p: TransformSupport =>
          val transformContext = p.doTransform(substraitContext)
          (transformContext.root, transformContext.outputAttributes)
        case _ =>
          val readRel =
            RelBuilder.makeReadRel(
              new util.ArrayList[Attribute](plan.output.asJava),
              substraitContext)
          (readRel, plan.output)
      }
    }

    val (streamedRelNode, streamedOutput) = transformAndGetOutput(streamedPlan)
    val (buildRelNode, buildOutput) = transformAndGetOutput(buildPlan)
    val joinRel = getJoinRel(streamedRelNode, buildRelNode, substraitContext)

    val (inputAttributes, outputAttributes, resultProjection) = buildSide match {
      case BuildLeft =>
        val (leftOutput, rightOutput) = getNewOutput(buildOutput, streamedOutput)
        val resultOutput = leftOutput ++ rightOutput
        val resultProjection = leftOutput.indices.map(idx =>
          ExpressionBuilder.makeSelection(idx + streamedOutput.size)) ++
          rightOutput.indices
            .map(ExpressionBuilder.makeSelection(_))
        (buildOutput ++ streamedOutput, resultOutput, resultProjection)
      case BuildRight =>
        val (leftOutput, rightOutput) = getNewOutput(streamedOutput, buildOutput)
        val resultOutput = leftOutput ++ rightOutput
        val resultProjection =
          resultOutput.indices.map(ExpressionBuilder.makeSelection(_))
        (streamedOutput ++ buildOutput, resultOutput, resultProjection)
    }

    val resultProjectRel =
      RelBuilder.makeProjectRel(
        joinRel,
        new java.util.ArrayList[ExpressionNode](resultProjection.asJava))

    TransformContext(inputAttributes, outputAttributes, resultProjectRel)
  }

  private def getJoinRel(
      streamedRelNode: RelNode,
      buildRelNode: RelNode,
      substraitContext: SubstraitContext,
      validation: Boolean = false): RelNode = {
    val joinExpressionNode = ExpressionConverter
      .replaceWithExpressionTransformer(joinExpression, joinOutput)
      .asInstanceOf[ExpressionTransformer]
      .doTransform(substraitContext.registeredFunction)

    val postJoinFilter = condition.map { expr =>
      ExpressionConverter
        .replaceWithExpressionTransformer(expr, joinOutput)
        .asInstanceOf[ExpressionTransformer]
        .doTransform(substraitContext.registeredFunction)
    }

    if (!validation) {
      RelBuilder.makeJoinRel(
        streamedRelNode,
        buildRelNode,
        substraitJoinType,
        joinExpressionNode,
        postJoinFilter.orNull)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodes = joinOutput.map { attr =>
        ConverterUtils.getTypeNode(attr.dataType, attr.nullable)
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(
          TypeBuilder.makeStruct(new util.ArrayList[TypeNode](inputTypeNodes.asJava)).toProtobuf))
      RelBuilder.makeJoinRel(
        streamedRelNode,
        buildRelNode,
        substraitJoinType,
        joinExpressionNode,
        postJoinFilter.orNull,
        extensionNode)
    }
  }

  private def getNewOutput(
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute]): (Seq[Attribute], Seq[Attribute]) = {
    joinType match {
      case _: InnerLike =>
        (leftOutput, rightOutput)
      case LeftOuter =>
        (leftOutput, rightOutput.map(_.withNullability(true)))
      case RightOuter =>
        (leftOutput.map(_.withNullability(true)), rightOutput)
      case FullOuter =>
        (leftOutput.map(_.withNullability(true)), rightOutput.map(_.withNullability(true)))
      case j: ExistenceJoin =>
        (leftOutput :+ j.exists, Nil)
      case LeftExistence(_) =>
        (leftOutput, Nil)
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} not take $x as the JoinType")
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): ShuffledHashJoinExecTransformer =
    copy(left = newLeft, right = newRight)
}
