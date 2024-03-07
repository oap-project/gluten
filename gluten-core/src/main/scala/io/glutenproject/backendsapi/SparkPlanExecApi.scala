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
package io.glutenproject.backendsapi

import io.glutenproject.execution._
import io.glutenproject.expression._
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode, WindowFunctionNode}

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.{FileFormat, WriteFilesExec}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.types.{BooleanType, LongType, NullType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._

trait SparkPlanExecApi {

  /**
   * Generate ColumnarToRowExecBase.
   *
   * @param child
   * @return
   */
  def genColumnarToRowExec(child: SparkPlan): ColumnarToRowExecBase

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  def genRowToColumnarExec(child: SparkPlan): RowToColumnarExecBase

  /**
   * Generate ColumnarToColumnarExec.
   *
   * @param child
   * @return
   */
  def genVanillaColumnarToNativeColumnarExec(
      child: SparkPlan): VanillaColumnarToNativeColumnarExecBase

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
  def genFilterExecTransformer(condition: Expression, child: SparkPlan): FilterExecTransformerBase

  def genHiveTableScanExecTransformer(plan: SparkPlan): HiveTableScanExecTransformer =
    HiveTableScanExecTransformer(plan)

  /** Generate HashAggregateExecTransformer. */
  def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer

  /** Generate HashAggregateExecPullOutHelper */
  def genHashAggregateExecPullOutHelper(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute]): HashAggregateExecPullOutBaseHelper

  def genColumnarShuffleExchange(shuffle: ShuffleExchangeExec, newChild: SparkPlan): SparkPlan

  /** Generate ShuffledHashJoinExecTransformer. */
  def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase

  /** Generate BroadcastHashJoinExecTransformer. */
  def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformer

  /** Generate CartesianProductExecTransformer. */
  def genCartesianProductExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      condition: Option[Expression]): CartesianProductExecTransformer

  def genBroadcastNestedLoopJoinExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]): BroadcastNestedLoopJoinExecTransformer

  def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): ExpressionTransformer =
    AliasTransformer(substraitExprName, child, original)

  /** Generate SplitTransformer. */
  def genStringSplitTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      regexExpr: ExpressionTransformer,
      limitExpr: ExpressionTransformer,
      original: StringSplit): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(srcExpr, regexExpr, limitExpr), original)
  }

  def genRandTransformer(
      substraitExprName: String,
      explicitSeed: ExpressionTransformer,
      original: Rand): ExpressionTransformer = {
    RandTransformer(substraitExprName, explicitSeed, original)
  }

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer

  def genStringToMapTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  /** Transform GetArrayItem to Substrait. */
  def genGetArrayItemExpressionNode(
      substraitExprName: String,
      functionMap: JMap[String, JLong],
      leftNode: ExpressionNode,
      rightNode: ExpressionNode,
      original: GetArrayItem): ExpressionNode

  def genPosExplodeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: PosExplode,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    PosExplodeTransformer(substraitExprName, child, original, attributeSeq)
  }

  /** Transform NaNvl to Substrait. */
  def genNaNvlTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: NaNvl): ExpressionTransformer = {
    throw new UnsupportedOperationException("NaNvl is not supported")
  }

  /** Transform map_entries to Substrait. */
  def genMapEntriesTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    throw new UnsupportedOperationException("map_entries is not supported")
  }

  /** Transform inline to Substrait. */
  def genInlineTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    throw new UnsupportedOperationException("map_entries is not supported")
  }

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * childOutputAttributes may be different from outputAttributes, for example, the
   * childOutputAttributes include additional shuffle key columns
   * @return
   */
  // scalastyle:off argcount
  def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch]

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V]

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  def createColumnarBatchSerializer(schema: StructType, metrics: Map[String, SQLMetric]): Serializer

  /** Create broadcast relation for BroadcastExchangeExec */
  def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation

  /** Create ColumnarWriteFilesExec */
  def createColumnarWriteFilesExec(
      child: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): WriteFilesExec

  /**
   * Generate extended DataSourceV2 Strategies. Currently only for ClickHouse backend.
   *
   * @return
   */
  def genExtendedDataSourceV2Strategies(): List[SparkSession => Strategy]

  /**
   * Generate extended query stage preparation rules.
   *
   * @return
   */
  def genExtendedQueryStagePrepRules(): List[SparkSession => Rule[SparkPlan]]

  /**
   * Generate extended Analyzers. Currently only for ClickHouse backend.
   *
   * @return
   */
  def genExtendedAnalyzers(): List[SparkSession => Rule[LogicalPlan]]

  /**
   * Generate extended Optimizers. Currently only for Velox backend.
   *
   * @return
   */
  def genExtendedOptimizers(): List[SparkSession => Rule[LogicalPlan]]

  /**
   * Generate extended Strategies
   *
   * @return
   */
  def genExtendedStrategies(): List[SparkSession => Strategy]

  /**
   * Generate extended columnar pre-rules, in the validation phase.
   *
   * @return
   */
  def genExtendedColumnarValidationRules(): List[SparkSession => Rule[SparkPlan]]

  /**
   * Generate extended columnar transform-rules.
   *
   * @return
   */
  def genExtendedColumnarTransformRules(): List[SparkSession => Rule[SparkPlan]]

  /**
   * Generate extended columnar post-rules.
   *
   * @return
   */
  def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]]

  def genDecimalRoundTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Round): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), original)
  }

  def genGetStructFieldTransformer(
      substraitExprName: String,
      childTransformer: ExpressionTransformer,
      ordinal: Int,
      original: GetStructField): ExpressionTransformer = {
    GetStructFieldTransformer(substraitExprName, childTransformer, ordinal, original)
  }

  def genNamedStructTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: CreateNamedStruct,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, original)
  }

  def genEqualNullSafeTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: EqualNullSafe): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
  }

  def genMd5Transformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Md5): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), original)
  }

  def genStringTranslateTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      matchingExpr: ExpressionTransformer,
      replaceExpr: ExpressionTransformer,
      original: StringTranslate): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(srcExpr, matchingExpr, replaceExpr),
      original)
  }

  def genStringLocateTransformer(
      substraitExprName: String,
      first: ExpressionTransformer,
      second: ExpressionTransformer,
      third: ExpressionTransformer,
      original: StringLocate): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(first, second, third), original)
  }

  /**
   * Generate an ExpressionTransformer to transform Sha2 expression. Sha2Transformer is the default
   * implementation.
   */
  def genSha2Transformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Sha2): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
  }

  /**
   * Generate an ExpressionTransformer to transform Sha1 expression. Sha1Transformer is the default
   * implementation.
   */
  def genSha1Transformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Sha1): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), original)
  }

  def genSizeExpressionTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Size): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), original)
  }

  /**
   * Generate an ExpressionTransformer to transform TruncTimestamp expression.
   * TruncTimestampTransformer is the default implementation.
   */
  def genTruncTimestampTransformer(
      substraitExprName: String,
      format: ExpressionTransformer,
      timestamp: ExpressionTransformer,
      timeZoneId: Option[String] = None,
      original: TruncTimestamp): ExpressionTransformer = {
    TruncTimestampTransformer(substraitExprName, format, timestamp, timeZoneId, original)
  }

  def genCastWithNewChild(c: Cast): Cast = c

  def genHashExpressionTransformer(
      substraitExprName: String,
      exprs: Seq[ExpressionTransformer],
      original: Expression): ExpressionTransformer = {
    HashExpressionTransformer(substraitExprName, exprs, original)
  }

  def genUnixTimestampTransformer(
      substraitExprName: String,
      timeExp: ExpressionTransformer,
      format: ExpressionTransformer,
      original: ToUnixTimestamp): ExpressionTransformer = {
    ToUnixTimestampTransformer(
      substraitExprName,
      timeExp,
      format,
      original.timeZoneId,
      original.failOnError,
      original)
  }

  /** Define backend specfic expression mappings. */
  def extraExpressionMappings: Seq[Sig] = Seq.empty

  /**
   * Define whether the join operator is fallback because of the join operator is not supported by
   * backend
   */
  def joinFallback(
      JoinType: JoinType,
      leftOutputSet: AttributeSet,
      right: AttributeSet,
      condition: Option[Expression]): Boolean = false

  /** default function to generate window function node */
  def genWindowFunctionsNode(
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

            val childrenNodeList = aggregateFunc.children
              .map(
                ExpressionConverter
                  .replaceWithExpressionTransformer(_, originalInputAttributes)
                  .doTransform(args))
              .asJava

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
            val offsetWf = wf.asInstanceOf[FrameLessOffsetWindowFunction]
            val frame = offsetWf.frame.asInstanceOf[SpecifiedWindowFrame]
            val childrenNodeList = new JArrayList[ExpressionNode]()
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.input,
                  attributeSeq = originalInputAttributes)
                .doTransform(args))
            // Spark only accepts foldable offset. Converts it to LongType literal.
            val offsetNode = ExpressionBuilder.makeLiteral(
              // Velox always expects positive offset.
              Math.abs(offsetWf.offset.eval(EmptyRow).asInstanceOf[Int].toLong),
              LongType,
              false)
            childrenNodeList.add(offsetNode)
            // NullType means Null is the default value. Don't pass it to native.
            if (offsetWf.default.dataType != NullType) {
              childrenNodeList.add(
                ExpressionConverter
                  .replaceWithExpressionTransformer(
                    offsetWf.default,
                    attributeSeq = originalInputAttributes)
                  .doTransform(args))
            }
            // Always adds ignoreNulls at the end.
            childrenNodeList.add(
              ExpressionBuilder.makeLiteral(offsetWf.ignoreNulls, BooleanType, false))
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
          case wf @ NthValue(input, offset: Literal, ignoreNulls: Boolean) =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val childrenNodeList = new JArrayList[ExpressionNode]()
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(input, attributeSeq = originalInputAttributes)
                .doTransform(args))
            childrenNodeList.add(LiteralTransformer(offset).doTransform(args))
            // Always adds ignoreNulls at the end.
            childrenNodeList.add(ExpressionBuilder.makeLiteral(ignoreNulls, BooleanType, false))
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, wf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(wf.dataType, wf.nullable),
              frame.upper.sql,
              frame.lower.sql,
              frame.frameType.sql
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ NTile(buckets: Expression) =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val childrenNodeList = new JArrayList[ExpressionNode]()
            val literal = buckets.asInstanceOf[Literal]
            childrenNodeList.add(LiteralTransformer(literal).doTransform(args))
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, wf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(wf.dataType, wf.nullable),
              frame.upper.sql,
              frame.lower.sql,
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

  def genInjectedFunctions(): Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = Seq.empty

  def rewriteSpillPath(path: String): String = path

  /**
   * Vanilla spark just push down part of filter condition into scan, however gluten can push down
   * all filters. This function calculates the remaining conditions in FilterExec, add into the
   * dataFilters of the leaf node.
   * @param extraFilters:
   *   Conjunctive Predicates, which are split from the upper FilterExec
   * @param sparkExecNode:
   *   The vanilla leaf node of the plan tree, which is FileSourceScanExec or BatchScanExec
   * @return
   *   return all push down filters
   */
  def postProcessPushDownFilter(
      extraFilters: Seq[Expression],
      sparkExecNode: LeafExecNode): Seq[Expression] = {
    sparkExecNode match {
      case fileSourceScan: FileSourceScanExec =>
        fileSourceScan.dataFilters ++ FilterHandler.getRemainingFilters(
          fileSourceScan.dataFilters,
          extraFilters)
      case batchScan: BatchScanExec =>
        batchScan.scan match {
          case fileScan: FileScan =>
            fileScan.dataFilters ++ FilterHandler.getRemainingFilters(
              fileScan.dataFilters,
              extraFilters)
          case _ =>
            // TODO: For data lake format use pushedFilters in SupportsPushDownFilters
            extraFilters
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"${sparkExecNode.getClass.toString} is not supported.")
    }
  }
}
