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
package io.glutenproject.backendsapi.clickhouse

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.{BackendsApiManager, SparkPlanExecApi}
import io.glutenproject.execution._
import io.glutenproject.expression._
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.extension.{FallbackBroadcastHashJoin, FallbackBroadcastHashJoinPrepQueryStage}
import io.glutenproject.extension.CountDistinctWithoutExpand
import io.glutenproject.extension.columnar.AddTransformHintRule
import io.glutenproject.extension.columnar.MiscColumnarRules.TransformPreOverrides
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode, WindowFunctionNode}
import io.glutenproject.utils.CHJoinValidateUtil
import io.glutenproject.vectorized.CHColumnarBatchSerializer

import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper, HashPartitioningWrapper}
import org.apache.spark.shuffle.utils.CHShuffleUtil
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.datasources.{FileFormat, WriteFilesExec}
import org.apache.spark.sql.execution.datasources.GlutenWriterColumnarRules.NativeWritePostRule
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, ClickHouseBuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.{CHExecUtil, PushDownUtil}
import org.apache.spark.sql.extension.{CommonSubexpressionEliminateRule, RewriteDateTimestampComparisonRule}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.collect.Lists
import org.apache.commons.lang3.ClassUtils

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.mutable.ArrayBuffer

class CHSparkPlanExecApi extends SparkPlanExecApi {

  /** Transform GetArrayItem to Substrait. */
  override def genGetArrayItemExpressionNode(
      substraitExprName: String,
      functionMap: JMap[String, JLong],
      leftNode: ExpressionNode,
      rightNode: ExpressionNode,
      original: GetArrayItem): ExpressionNode = {
    val functionName = ConverterUtils.makeFuncName(
      substraitExprName,
      Seq(original.left.dataType, original.right.dataType),
      FunctionConfig.OPT)
    val exprNodes = Lists.newArrayList(leftNode, rightNode)
    ExpressionBuilder.makeScalarFunction(
      ExpressionBuilder.newScalarFunction(functionMap, functionName),
      exprNodes,
      ConverterUtils.getTypeNode(original.dataType, original.nullable))
  }

  /**
   * Generate ColumnarToRowExecBase.
   *
   * @param child
   * @return
   */
  override def genColumnarToRowExec(child: SparkPlan): ColumnarToRowExecBase = {
    CHColumnarToRowExec(child)
  }

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  override def genRowToColumnarExec(child: SparkPlan): RowToColumnarExecBase = {
    RowToCHNativeColumnarExec(child)
  }

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition
   *   : the filter condition
   * @param child
   *   : the child of FilterExec
   * @return
   *   the transformer of FilterExec
   */
  override def genFilterExecTransformer(
      condition: Expression,
      child: SparkPlan): FilterExecTransformerBase = {
    child match {
      case scan: FileSourceScanExec
          if (scan.relation.location.isInstanceOf[TahoeFileIndex] &&
            scan.relation.fileFormat.isInstanceOf[DeltaMergeTreeFileFormat]) =>
        CHFilterExecTransformer(condition, child)
      case _ =>
        FilterExecTransformer(condition, child)
    }
  }

  /** Generate HashAggregateExecTransformer. */
  override def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer =
    CHHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child)

  /** Generate HashAggregateExecPullOutHelper */
  override def genHashAggregateExecPullOutHelper(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute]): HashAggregateExecPullOutBaseHelper =
    CHHashAggregateExecPullOutHelper(groupingExpressions, aggregateExpressions, aggregateAttributes)

  /**
   * If there are expressions (not field reference) in the partitioning's children, add a projection
   * before shuffle exchange and make a new partitioning with the old expressions replaced by the
   * result columns from the projection.
   */
  private def addProjectionForShuffleExchange(
      plan: ShuffleExchangeExec): (Int, Partitioning, SparkPlan) = {
    def selectExpressions(
        exprs: Seq[Expression],
        attributes: Seq[Attribute]): (Seq[NamedExpression], Seq[Int]) = {
      var expressionPos = Seq[Int]()
      var projectExpressions = Seq[NamedExpression]()

      exprs.foreach(
        expr => {
          if (!expr.isInstanceOf[AttributeReference]) {
            val n = projectExpressions.size
            val namedExpression = Alias(expr, s"projected_partitioning_value_$n")()
            projectExpressions = projectExpressions :+ namedExpression
            expressionPos = expressionPos :+ (attributes.size + n)
          } else {
            // the new projected columns are appended at the end
            expressionPos = expressionPos :+ BindReferences
              .bindReference(expr, attributes)
              .asInstanceOf[BoundReference]
              .ordinal
          }
        })
      (projectExpressions, expressionPos)
    }

    plan.outputPartitioning match {
      case HashPartitioning(exprs, numPartitions) =>
        val (projectExpressions, newExpressionsPosition) = {
          selectExpressions(
            exprs,
            BackendsApiManager.getTransformerApiInstance
              .getPlanOutput(plan.child))
        }
        if (projectExpressions.isEmpty) {
          return (0, plan.outputPartitioning, plan.child)
        }
        val project = TransformPreOverrides().replaceWithTransformerPlan(
          AddTransformHintRule().apply(
            ProjectExec(plan.child.output ++ projectExpressions, plan.child)))
        var newExprs = Seq[Expression]()
        for (i <- exprs.indices) {
          val pos = newExpressionsPosition(i)
          newExprs = newExprs :+ project.output(pos)
        }
        (
          projectExpressions.size,
          new HashPartitioningWrapper(exprs, newExprs, numPartitions),
          project)
      case RangePartitioning(orderings, numPartitions) =>
        val exprs = orderings.map(ordering => ordering.child)
        val (projectExpressions, newExpressionsPosition) = {
          selectExpressions(
            exprs,
            BackendsApiManager.getTransformerApiInstance
              .getPlanOutput(plan.child))
        }
        if (projectExpressions.isEmpty) {
          return (0, plan.outputPartitioning, plan.child)
        }
        val project = TransformPreOverrides().replaceWithTransformerPlan(
          AddTransformHintRule().apply(
            ProjectExec(plan.child.output ++ projectExpressions, plan.child)))
        var newOrderings = Seq[SortOrder]()
        for (i <- orderings.indices) {
          val oldOrdering = orderings(i)
          val pos = newExpressionsPosition(i)
          val ordering = SortOrder(
            project.output(pos),
            oldOrdering.direction,
            oldOrdering.nullOrdering,
            oldOrdering.sameOrderExpressions)
          newOrderings = newOrderings :+ ordering
        }
        (projectExpressions.size, RangePartitioning(newOrderings, numPartitions), project)
      case _ =>
        // no change for other cases
        (0, plan.outputPartitioning, plan.child)
    }
  }

  override def genColumnarShuffleExchange(
      shuffle: ShuffleExchangeExec,
      child: SparkPlan): SparkPlan = {
    if (
      BackendsApiManager.getSettings.supportShuffleWithProject(
        shuffle.outputPartitioning,
        shuffle.child)
    ) {
      val (projectColumnNumber, newPartitioning, newChild) =
        addProjectionForShuffleExchange(shuffle)

      if (projectColumnNumber != 0) {
        if (newChild.supportsColumnar) {
          val newPlan = ShuffleExchangeExec(newPartitioning, newChild, shuffle.shuffleOrigin)
          // the new projections columns are appended at the end.
          ColumnarShuffleExchangeExec(
            newPlan,
            newChild,
            newChild.output.dropRight(projectColumnNumber))
        } else {
          // It's the case that partitioning expressions could be offloaded into native.
          shuffle.withNewChildren(Seq(child))
        }
      } else {
        ColumnarShuffleExchangeExec(shuffle, child, null)
      }
    } else {
      ColumnarShuffleExchangeExec(shuffle, child, null)
    }
  }

  /** Generate ShuffledHashJoinExecTransformer. */
  def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase =
    CHShuffledHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isSkewJoin)

  /** Generate BroadcastHashJoinExecTransformer. */
  def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformer =
    CHBroadcastHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isNullAwareAntiJoin)

  override def genSortMergeJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean = false,
      projectList: Seq[NamedExpression] = null): SortMergeJoinExecTransformerBase =
    CHSortMergeJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      condition,
      left,
      right,
      isSkewJoin,
      projectList
    )

  /** Generate CartesianProductExecTransformer */
  override def genCartesianProductExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      condition: Option[Expression]): CartesianProductExecTransformer =
    throw new UnsupportedOperationException(
      "CartesianProductExecTransformer is not supported in ch backend.")

  override def genBroadcastNestedLoopJoinExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]): BroadcastNestedLoopJoinExecTransformer =
    throw new UnsupportedOperationException(
      "BroadcastNestedLoopJoinExecTransformer is not supported in ch backend.")

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer =
    GetMapValueTransformer(substraitExprName, left, right, original.failOnError, original)

  override def genRandTransformer(
      substraitExprName: String,
      explicitSeed: ExpressionTransformer,
      original: Rand): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(explicitSeed), original)
  }

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  // scalastyle:off argcount
  override def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      projectOutputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric]
  ): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    CHExecUtil.genShuffleDependency(
      rdd,
      childOutputAttributes,
      projectOutputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      metrics
    )
  }
  // scalastyle:on argcount

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {
    CHShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(
      schema: StructType,
      metrics: Map[String, SQLMetric]): Serializer = {
    val readBatchNumRows = metrics("avgReadBatchNumRows")
    val numOutputRows = metrics("numOutputRows")
    val dataSize = metrics("dataSize")
    if (GlutenConfig.getConf.isUseCelebornShuffleManager) {
      val clazz = ClassUtils.getClass("org.apache.spark.shuffle.CHCelebornColumnarBatchSerializer")
      val constructor =
        clazz.getConstructor(classOf[SQLMetric], classOf[SQLMetric], classOf[SQLMetric])
      constructor.newInstance(readBatchNumRows, numOutputRows, dataSize).asInstanceOf[Serializer]
    } else {
      new CHColumnarBatchSerializer(readBatchNumRows, numOutputRows, dataSize)
    }
  }

  /** Create broadcast relation for BroadcastExchangeExec */
  override def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation = {
    val hashedRelationBroadcastMode = mode.asInstanceOf[HashedRelationBroadcastMode]
    val (newChild, newOutput, newBuildKeys) =
      if (
        hashedRelationBroadcastMode.key
          .forall(k => k.isInstanceOf[AttributeReference] || k.isInstanceOf[BoundReference])
      ) {
        (child, child.output, Seq.empty[Expression])
      } else {
        // pre projection in case of expression join keys
        val buildKeys = hashedRelationBroadcastMode.key
        val appendedProjections = new ArrayBuffer[NamedExpression]()
        val preProjectionBuildKeys = buildKeys.zipWithIndex.map {
          case (e, idx) =>
            e match {
              case b: BoundReference => child.output(b.ordinal)
              case o: Expression =>
                val newExpr = Alias(o, "col_" + idx)()
                appendedProjections += newExpr
                newExpr
            }
        }

        def wrapChild(child: SparkPlan): WholeStageTransformer = {
          val childWithAdapter = ColumnarCollapseTransformStages.wrapInputIteratorTransformer(child)
          WholeStageTransformer(
            ProjectExecTransformer(child.output ++ appendedProjections, childWithAdapter))(
            ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet()
          )
        }

        val newChild = child match {
          case wt: WholeStageTransformer =>
            wt.withNewChildren(
              Seq(ProjectExecTransformer(child.output ++ appendedProjections, wt.child)))
          case w: WholeStageCodegenExec =>
            w.withNewChildren(Seq(ProjectExec(child.output ++ appendedProjections, w.child)))
          case r: AQEShuffleReadExec if r.supportsColumnar =>
            // when aqe is open
            // TODO: remove this after pushdowning preprojection
            wrapChild(r)
          case r2c: RowToCHNativeColumnarExec =>
            wrapChild(r2c)
          case union: ColumnarUnionExec =>
            wrapChild(union)
          case other =>
            throw new UnsupportedOperationException(
              s"Not supported operator ${other.nodeName} for BroadcastRelation")
        }
        (newChild, (child.output ++ appendedProjections).map(_.toAttribute), preProjectionBuildKeys)
      }
    val countsAndBytes =
      CHExecUtil.buildSideRDD(dataSize, newChild).collect

    val batches = countsAndBytes.map(_._2)
    val rawSize = dataSize.value
    if (rawSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${rawSize >> 30} GB")
    }
    val rowCount = countsAndBytes.map(_._1).sum
    numOutputRows += rowCount
    ClickHouseBuildSideRelation(mode, newOutput, batches.flatten, rowCount, newBuildKeys)
  }

  /**
   * Generate extended DataSourceV2 Strategies. Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedDataSourceV2Strategies(): List[SparkSession => Strategy] = {
    List.empty
  }

  /**
   * Generate extended query stage preparation rules.
   *
   * @return
   */
  override def genExtendedQueryStagePrepRules(): List[SparkSession => Rule[SparkPlan]] =
    List(spark => FallbackBroadcastHashJoinPrepQueryStage(spark))

  /**
   * Generate extended Analyzers. Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedAnalyzers(): List[SparkSession => Rule[LogicalPlan]] = {
    List(spark => new RewriteDateTimestampComparisonRule(spark, spark.sessionState.conf))
  }

  /**
   * Generate extended Optimizers.
   *
   * @return
   */
  override def genExtendedOptimizers(): List[SparkSession => Rule[LogicalPlan]] = {
    List(
      spark => new CommonSubexpressionEliminateRule(spark, spark.sessionState.conf),
      _ => CountDistinctWithoutExpand
    )
  }

  /**
   * Generate extended columnar pre-rules, in the validation phase.
   *
   * @return
   */
  override def genExtendedColumnarValidationRules(): List[SparkSession => Rule[SparkPlan]] =
    List(spark => FallbackBroadcastHashJoin(spark))

  /**
   * Generate extended columnar pre-rules.
   *
   * @return
   */
  override def genExtendedColumnarTransformRules(): List[SparkSession => Rule[SparkPlan]] =
    List()

  /**
   * Generate extended columnar post-rules.
   *
   * @return
   */
  override def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] =
    List(spark => NativeWritePostRule(spark))

  /**
   * Generate extended Strategies.
   *
   * @return
   */
  override def genExtendedStrategies(): List[SparkSession => Strategy] =
    List()

  override def genStringTranslateTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      matchingExpr: ExpressionTransformer,
      replaceExpr: ExpressionTransformer,
      original: StringTranslate): ExpressionTransformer = {
    CHStringTranslateTransformer(substraitExprName, srcExpr, matchingExpr, replaceExpr, original)
  }

  override def genSizeExpressionTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Size): ExpressionTransformer = {
    CHSizeExpressionTransformer(substraitExprName, child, original)
  }

  /** Generate an ExpressionTransformer to transform TruncTimestamp expression for CH. */
  override def genTruncTimestampTransformer(
      substraitExprName: String,
      format: ExpressionTransformer,
      timestamp: ExpressionTransformer,
      timeZoneId: Option[String],
      original: TruncTimestamp): ExpressionTransformer = {
    CHTruncTimestampTransformer(substraitExprName, format, timestamp, timeZoneId, original)
  }

  override def genPosExplodeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: PosExplode,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    CHPosExplodeTransformer(substraitExprName, child, original, attributeSeq)
  }

  override def createColumnarWriteFilesExec(
      child: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): WriteFilesExec = {
    throw new UnsupportedOperationException("ColumnarWriteFilesExec is not support in ch backend.")
  }

  /**
   * Define whether the join operator is fallback because of the join operator is not supported by
   * backend
   */
  override def joinFallback(
      JoinType: JoinType,
      leftOutputSet: AttributeSet,
      rightOutputSet: AttributeSet,
      condition: Option[Expression]): Boolean = {
    CHJoinValidateUtil.shouldFallback(JoinType, leftOutputSet, rightOutputSet, condition)
  }

  /** Generate window function node */
  override def genWindowFunctionsNode(
      windowExpression: Seq[NamedExpression],
      windowExpressionNodes: JList[WindowFunctionNode],
      originalInputAttributes: Seq[Attribute],
      args: JMap[String, JLong]): Unit = {

    windowExpression.map {
      windowExpr =>
        val aliasExpr = windowExpr.asInstanceOf[Alias]
        val columnName = s"${aliasExpr.name}_${aliasExpr.exprId.id}"
        val wExpression = aliasExpr.child.asInstanceOf[WindowExpression]
        wExpression.windowFunction match {
          case wf @ (RowNumber() | Rank(_) | DenseRank(_) | CumeDist() | PercentRank(_)) =>
            val aggWindowFunc = wf.asInstanceOf[AggregateWindowFunction]
            val frame = aggWindowFunc.frame.asInstanceOf[SpecifiedWindowFrame]
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, aggWindowFunc).toInt,
              new JArrayList[ExpressionNode](),
              columnName,
              ConverterUtils.getTypeNode(aggWindowFunc.dataType, aggWindowFunc.nullable),
              WindowExecTransformer.getFrameBound(frame.upper),
              WindowExecTransformer.getFrameBound(frame.lower),
              frame.frameType.sql
            )
            windowExpressionNodes.add(windowFunctionNode)
          case aggExpression: AggregateExpression =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val aggregateFunc = aggExpression.aggregateFunction
            val substraitAggFuncName = ExpressionMappings.expressionsMap.get(aggregateFunc.getClass)
            if (substraitAggFuncName.isEmpty) {
              throw new UnsupportedOperationException(s"Not currently supported: $aggregateFunc.")
            }

            val childrenNodeList = new JArrayList[ExpressionNode]()
            aggregateFunc.children.foreach(
              expr =>
                childrenNodeList.add(
                  ExpressionConverter
                    .replaceWithExpressionTransformer(expr, originalInputAttributes)
                    .doTransform(args)))

            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              AggregateFunctionsBuilder.create(args, aggExpression.aggregateFunction).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(aggExpression.dataType, aggExpression.nullable),
              WindowExecTransformer.getFrameBound(frame.upper),
              WindowExecTransformer.getFrameBound(frame.lower),
              frame.frameType.sql
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ (Lead(_, _, _, _) | Lag(_, _, _, _)) =>
            val (offsetWf, frame) = wf match {
              case lead @ Lead(input, offset, default, ignoreNulls) =>
                // When the offset value of the lead is negative, will convert to lag function
                lead.offset match {
                  case IntegerLiteral(value) if value < 0 =>
                    val newWf = Lag(input, Literal(math.abs(value)), default, ignoreNulls)
                    (newWf, newWf.frame.asInstanceOf[SpecifiedWindowFrame])
                  case other => (lead, lead.frame.asInstanceOf[SpecifiedWindowFrame])
                }
              case lag @ Lag(input, offset, default, ignoreNulls) =>
                // When the offset value of the lag is negative, will convert to lead function
                lag.offset match {
                  case IntegerLiteral(value) if value > 0 =>
                    val newWf = Lead(input, Literal(value), default, ignoreNulls)
                    (newWf, newWf.frame.asInstanceOf[SpecifiedWindowFrame])
                  case other => (lag, lag.frame.asInstanceOf[SpecifiedWindowFrame])
                }
            }

            val childrenNodeList = new JArrayList[ExpressionNode]()
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.input,
                  attributeSeq = originalInputAttributes)
                .doTransform(args))
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.offset,
                  attributeSeq = originalInputAttributes)
                .doTransform(args))
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.default,
                  attributeSeq = originalInputAttributes)
                .doTransform(args))
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, offsetWf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(offsetWf.dataType, offsetWf.nullable),
              WindowExecTransformer.getFrameBound(frame.upper),
              WindowExecTransformer.getFrameBound(frame.lower),
              frame.frameType.sql
            )
            windowExpressionNodes.add(windowFunctionNode)
          case _ =>
            throw new UnsupportedOperationException(
              "unsupported window function type: " +
                wExpression.windowFunction)
        }
    }
  }

  /** Clickhouse Backend only supports part of filters for parquet. */
  override def postProcessPushDownFilter(
      extraFilters: Seq[Expression],
      sparkExecNode: LeafExecNode): Seq[Expression] = {
    // FIXME: DeltaMergeTreeFileFormat should not inherit from ParquetFileFormat.
    def isParquetFormat(fileFormat: FileFormat): Boolean = fileFormat match {
      case p: ParquetFileFormat if p.shortName().equals("parquet") => true
      case _ => false
    }

    // TODO: datasource v2 ?
    // TODO: Push down conditions with scalar subquery
    // For example, consider TPCH 22 'c_acctbal > (select avg(c_acctbal) from customer where ...)'.
    // Vanilla Spark only pushes down the Parquet Filter not Catalyst Filter, which can not get the
    // subquery result, while gluten pushes down the Catalyst Filter can benefit from this case.
    //
    // Let's make push down functionally same as vanilla Spark for now.

    sparkExecNode match {
      case fileSourceScan: FileSourceScanExec
          if isParquetFormat(fileSourceScan.relation.fileFormat) =>
        PushDownUtil.removeNotSupportPushDownFilters(
          fileSourceScan.conf,
          fileSourceScan.output,
          fileSourceScan.dataFilters)
      case _ => super.postProcessPushDownFilter(extraFilters, sparkExecNode)
    }
  }
}
