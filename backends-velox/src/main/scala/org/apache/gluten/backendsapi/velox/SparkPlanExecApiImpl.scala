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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.SparkPlanExecApi
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.expression._
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.extension.columnar.TransformHints
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode, IfThenNode}
import org.apache.gluten.vectorized.{ColumnarBatchSerializer, ColumnarBatchSerializeResult}

import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.ShuffleUtil
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.{AggregateFunctionRewriteRule, FlushableHashAggregateRule, FunctionIdentifier}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, Cast, CreateNamedStruct, ElementAt, Expression, ExpressionInfo, Generator, GetArrayItem, GetMapValue, GetStructField, If, IsNaN, Literal, Murmur3Hash, NamedExpression, NaNvl, PosExplode, Round, SortOrder, StringSplit, StringTrim, Uuid}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, HLLAdapter}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, HashPartitioning, Partitioning, RoundRobinPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{FileFormat, WriteFilesExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.ExecUtil
import org.apache.spark.sql.expression.{UDFExpression, UDFResolver}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.collect.Lists
import org.apache.commons.lang3.ClassUtils

import javax.ws.rs.core.UriBuilder

import java.lang.{Long => JLong}
import java.util.{Map => JMap}

import scala.collection.mutable.ListBuffer

class SparkPlanExecApiImpl extends SparkPlanExecApi {

  /**
   * Transform GetArrayItem to Substrait.
   *
   * arrCol[index] => IF(index < 0, null, ElementAt(arrCol, index + 1))
   */
  override def genGetArrayItemExpressionNode(
      substraitExprName: String,
      functionMap: JMap[String, JLong],
      leftNode: ExpressionNode,
      rightNode: ExpressionNode,
      original: GetArrayItem): ExpressionNode = {
    if (original.dataType.isInstanceOf[DecimalType]) {
      val decimalType = original.dataType.asInstanceOf[DecimalType]
      val precision = decimalType.precision
      if (precision > 18) {
        throw new GlutenNotSupportException(
          "GetArrayItem not support decimal precision more than 18")
      }
    }
    // ignore origin substraitExprName
    val functionName = ConverterUtils.makeFuncName(
      ExpressionMappings.expressionsMap(classOf[ElementAt]),
      Seq(original.dataType),
      FunctionConfig.OPT)
    val exprNodes = Lists.newArrayList(leftNode, rightNode)
    val resultNode = ExpressionBuilder.makeScalarFunction(
      ExpressionBuilder.newScalarFunction(functionMap, functionName),
      exprNodes,
      ConverterUtils.getTypeNode(original.dataType, original.nullable))
    val nullNode = ExpressionBuilder.makeLiteral(null, original.dataType, false)
    val lessThanFuncId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        ExpressionNames.LESS_THAN,
        Seq(original.right.dataType, IntegerType),
        FunctionConfig.OPT))
    // right node already add 1
    val literalNode = ExpressionBuilder.makeLiteral(1.toInt, IntegerType, false)
    val lessThanFuncNode = ExpressionBuilder.makeScalarFunction(
      lessThanFuncId,
      Lists.newArrayList(rightNode, literalNode),
      ConverterUtils.getTypeNode(BooleanType, true))
    new IfThenNode(Lists.newArrayList(lessThanFuncNode), Lists.newArrayList(nullNode), resultNode)
  }

  /** Transform NaNvl to Substrait. */
  override def genNaNvlTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: NaNvl): ExpressionTransformer = {
    val condExpr = IsNaN(original.left)
    val condFuncName = ExpressionMappings.expressionsMap(classOf[IsNaN])
    val newExpr = If(condExpr, original.right, original.left)
    IfTransformer(
      GenericExpressionTransformer(condFuncName, Seq(left), condExpr),
      right,
      left,
      newExpr
    )
  }

  /** Transform Uuid to Substrait. */
  override def genUuidTransformer(
      substraitExprName: String,
      original: Uuid): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(LiteralTransformer(Literal(original.randomSeed.get))),
      original)
  }

  /** Transform map_entries to Substrait. */
  override def genMapEntriesTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), expr)
  }

  /** Transform posexplode to Substrait. */
  override def genPosExplodeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: PosExplode,
      attrSeq: Seq[Attribute]): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), attrSeq.head)
  }

  /** Transform inline to Substrait. */
  override def genInlineTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), expr)
  }

  /** Transform make_timestamp to Substrait. */
  override def genMakeTimestampTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  /**
   * * Plans.
   */

  /**
   * Generate ColumnarToRowExecBase.
   *
   * @param child
   * @return
   */
  override def genColumnarToRowExec(child: SparkPlan): ColumnarToRowExecBase =
    VeloxColumnarToRowExec(child)

  /**
   * Generate RowToColumnarExec.
   *
   * @param child
   * @return
   */
  override def genRowToColumnarExec(child: SparkPlan): RowToColumnarExecBase =
    RowToVeloxColumnarExec(child)

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
      child: SparkPlan): FilterExecTransformerBase = FilterExecTransformer(condition, child)

  /** Generate HashAggregateExecTransformer. */
  override def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer =
    RegularHashAggregateExecTransformer(
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
    HashAggregateExecPullOutHelper(groupingExpressions, aggregateExpressions, aggregateAttributes)

  override def genColumnarShuffleExchange(
      shuffle: ShuffleExchangeExec,
      newChild: SparkPlan): SparkPlan = {
    shuffle.outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        val hashExpr = new Murmur3Hash(exprs)
        val projectList = Seq(Alias(hashExpr, "hash_partition_key")()) ++ newChild.output
        val projectTransformer = ProjectExecTransformer(projectList, newChild)
        val validationResult = projectTransformer.doValidate()
        if (validationResult.isValid) {
          ColumnarShuffleExchangeExec(
            shuffle,
            projectTransformer,
            projectTransformer.output.drop(1))
        } else {
          TransformHints.tagNotTransformable(shuffle, validationResult)
          shuffle.withNewChildren(newChild :: Nil)
        }
      case RoundRobinPartitioning(num) if SQLConf.get.sortBeforeRepartition && num > 1 =>
        val hashExpr = new Murmur3Hash(newChild.output)
        val projectList = Seq(Alias(hashExpr, "hash_partition_key")()) ++ newChild.output
        val projectTransformer = ProjectExecTransformer(projectList, newChild)
        val sortOrder = SortOrder(projectTransformer.output.head, Ascending)
        val sortByHashCode = SortExecTransformer(Seq(sortOrder), global = false, projectTransformer)
        val dropSortColumnTransformer = ProjectExecTransformer(projectList.drop(1), sortByHashCode)
        val validationResult = dropSortColumnTransformer.doValidate()
        if (validationResult.isValid) {
          ColumnarShuffleExchangeExec(
            shuffle,
            dropSortColumnTransformer,
            dropSortColumnTransformer.output)
        } else {
          TransformHints.tagNotTransformable(shuffle, validationResult)
          shuffle.withNewChildren(newChild :: Nil)
        }
      case _ =>
        ColumnarShuffleExchangeExec(shuffle, newChild, null)
    }
  }

  /** Generate ShuffledHashJoinExecTransformer. */
  override def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase =
    ShuffledHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isSkewJoin)

  /** Generate BroadcastHashJoinExecTransformer. */
  override def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformerBase =
    BroadcastHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isNullAwareAntiJoin)

  override def genCartesianProductExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      condition: Option[Expression]): CartesianProductExecTransformer = {
    CartesianProductExecTransformer(
      ColumnarCartesianProductBridge(left),
      ColumnarCartesianProductBridge(right),
      condition
    )
  }

  override def genBroadcastNestedLoopJoinExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]): BroadcastNestedLoopJoinExecTransformer =
    GlutenBroadcastNestedLoopJoinExecTransformer(
      left,
      right,
      buildSide,
      joinType,
      condition
    )

  override def genHashExpressionTransformer(
      substraitExprName: String,
      exprs: Seq[ExpressionTransformer],
      original: Expression): ExpressionTransformer = {
    VeloxHashExpressionTransformer(substraitExprName, exprs, original)
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
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
    ExecUtil.genShuffleDependency(
      rdd,
      childOutputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      metrics)
  }
  // scalastyle:on argcount

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {
    ShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  override def createColumnarWriteFilesExec(
      child: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): WriteFilesExec = {
    new VeloxColumnarWriteFilesExec(
      child,
      fileFormat,
      partitionColumns,
      bucketSpec,
      options,
      staticPartitions)
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
    val decompressTime = metrics("decompressTime")
    val ipcTime = metrics("ipcTime")
    val deserializeTime = metrics("deserializeTime")
    if (GlutenConfig.getConf.isUseCelebornShuffleManager) {
      val clazz = ClassUtils.getClass("org.apache.spark.shuffle.CelebornColumnarBatchSerializer")
      val constructor =
        clazz.getConstructor(classOf[StructType], classOf[SQLMetric], classOf[SQLMetric])
      constructor.newInstance(schema, readBatchNumRows, numOutputRows).asInstanceOf[Serializer]
    } else {
      new ColumnarBatchSerializer(
        schema,
        readBatchNumRows,
        numOutputRows,
        decompressTime,
        ipcTime,
        deserializeTime)
    }
  }

  /** Create broadcast relation for BroadcastExchangeExec */
  override def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation = {
    val serialized: Array[ColumnarBatchSerializeResult] = child
      .executeColumnar()
      .mapPartitions(itr => Iterator(BroadcastUtils.serializeStream(itr)))
      .filter(_.getNumRows != 0)
      .collect
    val rawSize = serialized.map(_.getSerialized.length).sum
    if (rawSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${rawSize >> 30} GB")
    }
    numOutputRows += serialized.map(_.getNumRows).sum
    dataSize += rawSize
    ColumnarBuildSideRelation(child.output, serialized.map(_.getSerialized))
  }

  /**
   * * Expressions.
   */

  /** Generates a transformer for decimal round. */
  override def genDecimalRoundTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Round): ExpressionTransformer = {
    DecimalRoundTransformer(substraitExprName, child, original)
  }

  /** Generate StringSplit transformer. */
  override def genStringSplitTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      regexExpr: ExpressionTransformer,
      limitExpr: ExpressionTransformer,
      original: StringSplit): ExpressionTransformer = {
    // In velox, split function just support tow args, not support limit arg for now
    VeloxStringSplitTransformer(substraitExprName, srcExpr, regexExpr, limitExpr, original)
  }

  /**
   * Generate Alias transformer.
   *
   * @return
   *   a transformer for alias
   */
  override def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): ExpressionTransformer =
    VeloxAliasTransformer(substraitExprName, child, original)

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  override def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer = {
    GenericExpressionTransformer(
      ExpressionMappings.expressionsMap(classOf[ElementAt]),
      Seq(left, right),
      original)
  }

  override def genStringToMapTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    if (
      SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY)
        != SQLConf.MapKeyDedupPolicy.EXCEPTION.toString
    ) {
      throw new GlutenNotSupportException("Only EXCEPTION policy is supported!")
    }
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  /** Generate an expression transformer to transform NamedStruct to Substrait. */
  override def genNamedStructTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: CreateNamedStruct,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    VeloxNamedStructTransformer(substraitExprName, original, attributeSeq)
  }

  /** Generate an ExpressionTransformer to transform GetStructFiled expression. */
  override def genGetStructFieldTransformer(
      substraitExprName: String,
      childTransformer: ExpressionTransformer,
      ordinal: Int,
      original: GetStructField): ExpressionTransformer = {
    VeloxGetStructFieldTransformer(substraitExprName, childTransformer, ordinal, original)
  }

  /**
   * To align with spark in casting string type input to other types, add trim node for trimming
   * space or whitespace. See spark's Cast.scala.
   */
  override def genCastWithNewChild(c: Cast): Cast = {
    // scalastyle:off nonascii
    // Common whitespace to be trimmed, including: ' ', '\n', '\r', '\f', etc.
    val trimWhitespaceStr = " \t\n\u000B\u000C\u000D\u001C\u001D\u001E\u001F"
    // Space separator.
    val trimSpaceSepStr = "\u1680\u2008\u2009\u200A\u205F\u3000" +
      ('\u2000' to '\u2006').toList.mkString
    // Line separator.
    val trimLineSepStr = "\u2028"
    // Paragraph separator.
    val trimParaSepStr = "\u2029"
    // Needs to be trimmed for casting to float/double/decimal
    val trimSpaceStr = ('\u0000' to '\u0020').toList.mkString
    // scalastyle:on nonascii
    c.dataType match {
      case BinaryType | _: ArrayType | _: MapType | _: StructType | _: UserDefinedType[_] =>
        c
      case FloatType | DoubleType | _: DecimalType =>
        c.child.dataType match {
          case StringType =>
            val trimNode = StringTrim(c.child, Some(Literal(trimSpaceStr)))
            c.withNewChildren(Seq(trimNode)).asInstanceOf[Cast]
          case _ =>
            c
        }
      case _ =>
        c.child.dataType match {
          case StringType =>
            val trimNode = StringTrim(
              c.child,
              Some(
                Literal(trimWhitespaceStr +
                  trimSpaceSepStr + trimLineSepStr + trimParaSepStr)))
            c.withNewChildren(Seq(trimNode)).asInstanceOf[Cast]
          case _ =>
            c
        }
    }
  }

  /**
   * * Rules and strategies.
   */

  /**
   * Generate extended DataSourceV2 Strategy.
   *
   * @return
   */
  override def genExtendedDataSourceV2Strategies(): List[SparkSession => Strategy] = List()

  /**
   * Generate extended query stage preparation rules.
   *
   * @return
   */
  override def genExtendedQueryStagePrepRules(): List[SparkSession => Rule[SparkPlan]] = List()

  /**
   * Generate extended Analyzer.
   *
   * @return
   */
  override def genExtendedAnalyzers(): List[SparkSession => Rule[LogicalPlan]] = List()

  /**
   * Generate extended Optimizer. Currently only for Velox backend.
   *
   * @return
   */
  override def genExtendedOptimizers(): List[SparkSession => Rule[LogicalPlan]] =
    List(AggregateFunctionRewriteRule)

  /**
   * Generate extended columnar pre-rules, in the validation phase.
   *
   * @return
   */
  override def genExtendedColumnarValidationRules(): List[SparkSession => Rule[SparkPlan]] = List()

  /**
   * Generate extended columnar pre-rules.
   *
   * @return
   */
  override def genExtendedColumnarTransformRules(): List[SparkSession => Rule[SparkPlan]] = {
    val buf: ListBuffer[SparkSession => Rule[SparkPlan]] = ListBuffer()
    if (GlutenConfig.getConf.enableVeloxFlushablePartialAggregation) {
      buf += FlushableHashAggregateRule.apply
    }
    buf.result
  }

  /**
   * Generate extended columnar post-rules.
   *
   * @return
   */
  override def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] = {
    SparkShimLoader.getSparkShims.getExtendedColumnarPostRules() ::: List()
  }

  /**
   * Generate extended Strategy.
   *
   * @return
   */
  override def genExtendedStrategies(): List[SparkSession => Strategy] = {
    List()
  }

  /** Define backend specfic expression mappings. */
  override def extraExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[HLLAdapter](ExpressionNames.APPROX_DISTINCT),
      Sig[UDFExpression](ExpressionNames.UDF_PLACEHOLDER),
      Sig[NaNvl](ExpressionNames.NANVL))
  }

  override def genInjectedFunctions()
      : Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = {
    UDFResolver.getFunctionSignatures
  }

  override def rewriteSpillPath(path: String): String = {
    val fs = GlutenConfig.getConf.veloxSpillFileSystem
    fs match {
      case "local" =>
        path
      case "heap-over-local" =>
        val rewritten = UriBuilder
          .fromPath(path)
          .scheme("jol")
          .toString
        rewritten
      case other =>
        throw new IllegalStateException(s"Unsupported fs: $other")
    }
  }

  override def genGenerateTransformer(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan
  ): GenerateExecTransformerBase = {
    GenerateExecTransformer(generator, requiredChildOutput, outer, generatorOutput, child)
  }

  override def genPreProjectForGenerate(generate: GenerateExec): SparkPlan = {
    PullOutGenerateProjectHelper.pullOutPreProject(generate)
  }

  override def genPostProjectForGenerate(generate: GenerateExec): SparkPlan = {
    PullOutGenerateProjectHelper.pullOutPostProject(generate)
  }
}
