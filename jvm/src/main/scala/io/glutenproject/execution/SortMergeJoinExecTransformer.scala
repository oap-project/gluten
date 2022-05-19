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

import io.glutenproject.substrait.SubstraitContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class SortMergeJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false,
    projectList: Seq[NamedExpression] = null)
    extends BinaryExecNode
    with TransformSupport {

  val sparkConf = sparkContext.getConf
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "prepareTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to prepare left list"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to process"),
    "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to merge join"),
    "totaltime_sortmergejoin" -> SQLMetrics
      .createTimingMetric(sparkContext, "totaltime_sortmergejoin"))

  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")
  val joinTime = longMetric("joinTime")
  val prepareTime = longMetric("prepareTime")
  val totaltime_sortmegejoin = longMetric("totaltime_sortmergejoin")
  val resultSchema = this.schema

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"ColumnarSortMergeJoinExec doesn't support doExecute")
  }
  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }

  override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).iterator

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType ($opId)".trim
  }

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) {
      s"${condition.get}"
    } else "None"
    s"""
      |(${ExplainUtils.getOpId(this)}) $nodeName
      |${ExplainUtils.generateFieldString("Left keys", leftKeys)}
      |${ExplainUtils.generateFieldString("Right keys", rightKeys)}
      |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
    """.stripMargin
  }

  override def output: Seq[Attribute] = {
    if (projectList != null && projectList.nonEmpty) {
      return projectList.map(_.toAttribute)
    }
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
          s"${getClass.getSimpleName} should not take $x as the JoinType")
    }
  }
  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    // For left and right outer joins, the output is partitioned by the streamed input's join keys.
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (isSkewJoin) {
      // We re-arrange the shuffle partitions to deal with skew join, and the new children
      // partitioning doesn't satisfy `HashClusteredDistribution`.
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else {
      HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
    }
  }

  override def outputOrdering: Seq[SortOrder] = joinType match {
    // For inner join, orders of both sides keys should be kept.
    case _: InnerLike =>
      val leftKeyOrdering = getKeyOrdering(leftKeys, left.outputOrdering)
      val rightKeyOrdering = getKeyOrdering(rightKeys, right.outputOrdering)
      leftKeyOrdering.zip(rightKeyOrdering).map {
        case (lKey, rKey) =>
          // Also add the right key and its `sameOrderExpressions`
          val sameOrderExpressions = ExpressionSet(lKey.sameOrderExpressions ++ rKey.children)
          SortOrder(
            lKey.child, Ascending, sameOrderExpressions.toSeq)
      }
    // For left and right outer joins, the output is ordered by the streamed input's join keys.
    case LeftOuter => getKeyOrdering(leftKeys, left.outputOrdering)
    case RightOuter => getKeyOrdering(rightKeys, right.outputOrdering)
    // There are null rows in both streams, so there is no order.
    case FullOuter => Nil
    case LeftExistence(_) => getKeyOrdering(leftKeys, left.outputOrdering)
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  private def getKeyOrdering(
      keys: Seq[Expression],
      childOutputOrdering: Seq[SortOrder]): Seq[SortOrder] = {
    val requiredOrdering = requiredOrders(keys)
    if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
      keys.zip(childOutputOrdering).map {
        case (key, childOrder) =>
        val sameOrderExpressionsSet = ExpressionSet(childOrder.children) - key
          SortOrder(key, Ascending, sameOrderExpressionsSet.toSeq)
      }
    } else {
      requiredOrdering
    }
  }

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  val (buildKeys, streamedKeys, buildPlan, streamedPlan) = joinType match {
    case LeftSemi =>
      (rightKeys, leftKeys, right, left)
    case LeftOuter =>
      (rightKeys, leftKeys, right, left)
    case LeftAnti =>
      (rightKeys, leftKeys, right, left)
    case j: ExistenceJoin =>
      (rightKeys, leftKeys, right, left)
    case LeftExistence(_) =>
      (rightKeys, leftKeys, right, left)
    case _ =>
      left match {
        case p: SortMergeJoinExecTransformer =>
          (rightKeys, leftKeys, right, left)
        case FilterExecTransformer(_, child: SortMergeJoinExecTransformer) =>
          (rightKeys, leftKeys, right, left)
        case ProjectExecTransformer(_, child: SortMergeJoinExecTransformer) =>
          (rightKeys, leftKeys, right, left)
        case other =>
          (leftKeys, rightKeys, left, right)
      }
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = streamedPlan match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(streamedPlan.executeColumnar())
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {

    val curBuildPlan: Seq[(SparkPlan, SparkPlan)] = buildPlan match {
      case s: SortExecTransformer =>
        Seq((s, this))
      case c: TransformSupport if !c.isInstanceOf[SortExecTransformer] =>
        c.getBuildPlans
      case other =>
        /* should be InputAdapterTransformer or others */
        Seq((other, this))
    }
    streamedPlan match {
      case c: TransformSupport if c.isInstanceOf[SortExecTransformer] =>
        curBuildPlan ++ Seq((c, this))
      case c: TransformSupport if !c.isInstanceOf[SortExecTransformer] =>
        c.getBuildPlans ++ curBuildPlan
      case _ =>
        curBuildPlan
    }
  }

  override def getStreamedLeafPlan: SparkPlan = streamedPlan match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {
    val numOutputRows = longMetric("numOutputRows")
    val procTime = longMetric("processTime")
    procTime.set(process_time / 1000000)
    numOutputRows += out_num_rows
  }

  override def getChild: SparkPlan = streamedPlan

  override def doValidate(): Boolean = false

  override def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(s"This operator doesn't support doTransform.")
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SortMergeJoinExecTransformer =
    copy(left = newLeft, right = newRight)
}
