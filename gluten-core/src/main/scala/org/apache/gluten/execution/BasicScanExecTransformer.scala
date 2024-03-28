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
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.collect.Lists
import com.google.protobuf.StringValue

import scala.collection.JavaConverters._

trait BasicScanExecTransformer extends LeafTransformSupport with BaseDataSource {
  import org.apache.spark.sql.catalyst.util._

  /** Returns the filters that can be pushed down to native file scan */
  def filterExprs(): Seq[Expression]

  def outputAttributes(): Seq[Attribute]

  def getMetadataColumns(): Seq[AttributeReference]

  /** This can be used to report FileFormat for a file based scan operator. */
  val fileFormat: ReadFileFormat

  // TODO: Remove this expensive call when CH support scan custom partition location.
  def getInputFilePaths: Seq[String] = {
    // This is a heavy operation, and only the required backend executes the corresponding logic.
    if (BackendsApiManager.getSettings.requiredInputFilePaths()) {
      getInputFilePathsInternal
    } else {
      Seq.empty
    }
  }

  /** Returns the file format properties. */
  def getProperties: Map[String, String] = Map.empty

  /** Returns the split infos that will be processed by the underlying native engine. */
  def getSplitInfos: Seq[SplitInfo] = {
    getSplitInfosFromPartitions(getPartitions)
  }

  def getSplitInfosFromPartitions(partitions: Seq[InputPartition]): Seq[SplitInfo] = {
    partitions.map(
      BackendsApiManager.getIteratorApiInstance
        .genSplitInfo(_, getPartitionSchema, fileFormat, getMetadataColumns.map(_.name)))
  }

  def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVectors = longMetric("outputVectors")
    val scanTime = longMetric("scanTime")
    val substraitContext = new SubstraitContext
    val transformContext = doTransform(substraitContext)
    val outNames = outputAttributes().map(ConverterUtils.genColumnNameWithExprId).asJava
    val planNode =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(transformContext.root), outNames)

    BackendsApiManager.getIteratorApiInstance.genNativeFileScanRDD(
      sparkContext,
      WholeStageTransformContext(planNode, substraitContext),
      getSplitInfos,
      this,
      numOutputRows,
      numOutputVectors,
      scanTime
    )
  }

  override protected def doValidateInternal(): ValidationResult = {
    var fields = schema.fields

    this match {
      case transformer: FileSourceScanExecTransformer =>
        fields = appendStringFields(transformer.relation.schema, fields)
      case transformer: HiveTableScanExecTransformer =>
        fields = appendStringFields(transformer.getDataSchema, fields)
      case transformer: BatchScanExecTransformer =>
        fields = appendStringFields(transformer.getDataSchema, fields)
      case _ =>
    }

    val validationResult = BackendsApiManager.getSettings
      .supportFileFormatRead(fileFormat, fields, getPartitionSchema.nonEmpty, getInputFilePaths)
    if (!validationResult.isValid) {
      return validationResult
    }

    val substraitContext = new SubstraitContext
    val relNode = doTransform(substraitContext).root

    doNativeValidation(substraitContext, relNode)
  }

  def appendStringFields(
      schema: StructType,
      existingFields: Array[StructField]): Array[StructField] = {
    val stringFields = schema.fields.filter(_.dataType.isInstanceOf[StringType])
    if (stringFields.nonEmpty) {
      (existingFields ++ stringFields).distinct
    } else {
      existingFields
    }
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val output = outputAttributes()
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val columnTypeNodes = output.map {
      attr =>
        if (getPartitionSchema.exists(_.name.equals(attr.name))) {
          new ColumnTypeNode(1)
        } else if (attr.isMetadataCol) {
          new ColumnTypeNode(2)
        } else {
          new ColumnTypeNode(0)
        }
    }.asJava
    // Will put all filter expressions into an AND expression
    val transformer = filterExprs()
      .map {
        case ar: AttributeReference if ar.dataType == BooleanType =>
          EqualNullSafe(ar, Literal.TrueLiteral)
        case e => e
      }
      .reduceLeftOption(And)
      .map(ExpressionConverter.replaceWithExpressionTransformer(_, output))
    val filterNodes = transformer.map(_.doTransform(context.registeredFunction))
    val exprNode = filterNodes.orNull

    // used by CH backend
    val optimizationContent =
      s"isMergeTree=${if (this.fileFormat == ReadFileFormat.MergeTreeReadFormat) "1" else "0"}\n"

    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder.setValue(optimizationContent).build)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(optimization, null)

    val readNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      columnTypeNodes,
      exprNode,
      extensionNode,
      context,
      context.nextOperatorId(this.nodeName))
    TransformContext(output, output, readNode)
  }
}
