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
import org.apache.gluten.expression._
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.extension.columnar.RewriteTypedImperativeAggregate
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.{AggregationParams, SubstraitContext}
import org.apache.gluten.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode, ScalarFunctionNode}
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}
import org.apache.gluten.utils.VeloxIntermediateData

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import com.google.protobuf.StringValue

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

abstract class HashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)

    val aggParams = new AggregationParams
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getAggRel(context, operatorId, aggParams, childCtx.root)
    TransformContext(childCtx.outputAttributes, output, relNode)
  }

  override protected def checkAggFuncModeSupport(
      aggFunc: AggregateFunction,
      mode: AggregateMode): Boolean = {
    aggFunc match {
      case _: HLLAdapter =>
        mode match {
          case Partial | Final => true
          case _ => false
        }
      case _ =>
        super.checkAggFuncModeSupport(aggFunc, mode)
    }
  }

  // Return whether the outputs partial aggregation should be combined for Velox computing.
  // When the partial outputs are multiple-column, row construct is needed.
  private def rowConstructNeeded(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    aggregateExpressions.exists {
      aggExpr =>
        aggExpr.mode match {
          case PartialMerge | Final =>
            aggExpr.aggregateFunction.inputAggBufferAttributes.size > 1
          case _ => false
        }
    }
  }

  /**
   * Returns whether extracting subfield from struct is needed. True when the intermediate type of
   * Velox aggregation is a compound type.
   * @return
   *   extracting needed or not.
   */
  private def extractStructNeeded(): Boolean = {
    aggregateExpressions.exists {
      expr =>
        expr.aggregateFunction match {
          case aggFunc if aggFunc.aggBufferAttributes.size > 1 =>
            expr.mode match {
              case Partial | PartialMerge => true
              case _ => false
            }
          case _ => false
        }
    }
  }

  /**
   * Add a projection after aggregation to extract subfields from Struct.
   * @param context
   *   the Substrait context
   * @param aggRel
   *   the aggregation rel
   * @param operatorId
   *   the operator id
   * @return
   *   a project rel
   */
  def applyExtractStruct(
      context: SubstraitContext,
      aggRel: RelNode,
      operatorId: Long,
      validation: Boolean): RelNode = {
    val expressionNodes = new JArrayList[ExpressionNode]()
    var colIdx = 0
    while (colIdx < groupingExpressions.size) {
      val groupingExpr: ExpressionNode = ExpressionBuilder.makeSelection(colIdx)
      expressionNodes.add(groupingExpr)
      colIdx += 1
    }

    for (expr <- aggregateExpressions) {
      expr.mode match {
        case Partial | PartialMerge =>
        case _ =>
          throw new GlutenNotSupportException(s"${expr.mode} not supported.")
      }
      val aggFunc = expr.aggregateFunction
      aggFunc match {
        case _ @VeloxIntermediateData.Type(veloxTypes: Seq[DataType]) =>
          val (sparkOrders, sparkTypes) =
            aggFunc.aggBufferAttributes.map(attr => (attr.name, attr.dataType)).unzip
          val veloxOrders = VeloxIntermediateData.veloxIntermediateDataOrder(aggFunc)
          val adjustedOrders = sparkOrders.map(veloxOrders.indexOf(_))
          sparkTypes.zipWithIndex.foreach {
            case (sparkType, idx) =>
              val veloxType = veloxTypes(adjustedOrders(idx))
              if (veloxType != sparkType) {
                // Velox and Spark have different type, adding a cast expression
                expressionNodes.add(
                  ExpressionBuilder
                    .makeCast(
                      ConverterUtils.getTypeNode(sparkType, nullable = false),
                      ExpressionBuilder.makeSelection(colIdx, adjustedOrders(idx)),
                      SQLConf.get.ansiEnabled))
              } else {
                // Velox and Spark have the same type
                expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, adjustedOrders(idx)))
              }
          }
          colIdx += 1
        case _ =>
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx))
          colIdx += 1
      }
    }
    if (!validation) {
      RelBuilder.makeProjectRel(
        aggRel,
        expressionNodes,
        context,
        operatorId,
        groupingExpressions.size + aggregateExpressions.size)
    } else {
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, getPartialAggOutTypes).toProtobuf))
      RelBuilder.makeProjectRel(
        aggRel,
        expressionNodes,
        extensionNode,
        context,
        operatorId,
        groupingExpressions.size + aggregateExpressions.size)
    }
  }

  // Whether the output data allows to be just pre-aggregated rather than
  // fully aggregated. If true, aggregation could flush its in memory
  // aggregated data whenever is needed rather than waiting for all input
  // to be read.
  protected def allowFlush: Boolean

  private def formatExtOptimizationString(isStreaming: Boolean): String = {
    val isStreamingStr = if (isStreaming) "1" else "0"
    val allowFlushStr = if (allowFlush) "1" else "0"
    s"isStreaming=$isStreamingStr\nallowFlush=$allowFlushStr\n"
  }

  // Create aggregate function node and add to list.
  private def addFunctionNode(
      args: java.lang.Object,
      aggregateFunction: AggregateFunction,
      childrenNodeList: JList[ExpressionNode],
      aggregateMode: AggregateMode,
      aggregateNodeList: JList[AggregateFunctionNode]): Unit = {
    val modeKeyWord = modeToKeyWord(aggregateMode)

    def generateMergeCompanionNode(): Unit = {
      aggregateMode match {
        case Partial | PartialMerge =>
          val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
            VeloxAggregateFunctionsBuilder.create(args, aggregateFunction, aggregateMode),
            childrenNodeList,
            modeKeyWord,
            VeloxIntermediateData.getIntermediateTypeNode(aggregateFunction)
          )
          aggregateNodeList.add(aggFunctionNode)
        case Final =>
          val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
            VeloxAggregateFunctionsBuilder.create(args, aggregateFunction, aggregateMode),
            childrenNodeList,
            modeKeyWord,
            ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
          )
          aggregateNodeList.add(aggFunctionNode)
        case other =>
          throw new GlutenNotSupportException(s"$other is not supported.")
      }
    }

    aggregateFunction match {
      case hllAdapter: HLLAdapter =>
        aggregateMode match {
          case Partial =>
            // For Partial mode output type is binary.
            val partialNode = ExpressionBuilder.makeAggregateFunction(
              VeloxAggregateFunctionsBuilder.create(args, aggregateFunction, aggregateMode),
              childrenNodeList,
              modeKeyWord,
              ConverterUtils.getTypeNode(
                hllAdapter.inputAggBufferAttributes.head.dataType,
                hllAdapter.inputAggBufferAttributes.head.nullable)
            )
            aggregateNodeList.add(partialNode)
          case Final =>
            // For Final mode output type is long.
            val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
              VeloxAggregateFunctionsBuilder.create(args, aggregateFunction, aggregateMode),
              childrenNodeList,
              modeKeyWord,
              ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
            )
            aggregateNodeList.add(aggFunctionNode)
          case other =>
            throw new GlutenNotSupportException(s"$other is not supported.")
        }
      case _ if aggregateFunction.aggBufferAttributes.size > 1 =>
        generateMergeCompanionNode()
      case _ =>
        val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
          VeloxAggregateFunctionsBuilder.create(args, aggregateFunction, aggregateMode),
          childrenNodeList,
          modeKeyWord,
          ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
        )
        aggregateNodeList.add(aggFunctionNode)
    }
  }

  /**
   * Return the output types after partial aggregation through Velox.
   * @return
   */
  def getPartialAggOutTypes: JList[TypeNode] = {
    val typeNodeList = new JArrayList[TypeNode]()
    groupingExpressions.foreach(
      expression => {
        typeNodeList.add(ConverterUtils.getTypeNode(expression.dataType, expression.nullable))
      })

    aggregateExpressions.foreach(
      expression => {
        val aggregateFunction = expression.aggregateFunction
        aggregateFunction match {
          case _ if aggregateFunction.aggBufferAttributes.size > 1 =>
            expression.mode match {
              case Partial | PartialMerge =>
                typeNodeList.add(VeloxIntermediateData.getIntermediateTypeNode(aggregateFunction))
              case Final =>
                typeNodeList.add(
                  ConverterUtils
                    .getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
              case other =>
                throw new GlutenNotSupportException(s"$other is not supported.")
            }
          case _ =>
            typeNodeList.add(
              ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
        }
      })
    typeNodeList
  }

  // Return a scalar function node representing row construct function in Velox.
  private def getRowConstructNode(
      args: java.lang.Object,
      childNodes: JList[ExpressionNode],
      rowConstructAttributes: Seq[Attribute],
      aggFunc: AggregateFunction): ScalarFunctionNode = {
    val functionMap = args.asInstanceOf[JHashMap[String, JLong]]
    val functionName = ConverterUtils.makeFuncName(
      VeloxIntermediateData.getRowConstructFuncName(aggFunc),
      rowConstructAttributes.map(attr => attr.dataType))
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)

    // Use struct type to represent Velox RowType.
    val structTypeNodes = rowConstructAttributes
      .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      .asJava

    ExpressionBuilder.makeScalarFunction(
      functionId,
      childNodes,
      TypeBuilder.makeStruct(false, structTypeNodes))
  }

  // Add a projection node before aggregation for row constructing.
  // Mainly used for aggregation whose intermediate type is a compound type in Velox.
  // Pre-projection is always not required for final stage.
  private def getAggRelWithRowConstruct(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      inputRel: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Create a projection for row construct.
    val exprNodes = new JArrayList[ExpressionNode]()
    groupingExpressions.foreach(
      expr => {
        exprNodes.add(
          ExpressionConverter
            .replaceWithExpressionTransformer(expr, originalInputAttributes)
            .doTransform(args))
      })

    for (aggregateExpression <- aggregateExpressions) {
      val aggFunc = aggregateExpression.aggregateFunction
      val functionInputAttributes = aggFunc.inputAggBufferAttributes
      aggFunc match {
        case _ if aggregateExpression.mode == Partial =>
          val childNodes = aggFunc.children
            .map(
              ExpressionConverter
                .replaceWithExpressionTransformer(_, originalInputAttributes)
                .doTransform(args)
            )
            .asJava
          exprNodes.addAll(childNodes)

        case _: HyperLogLogPlusPlus if aggFunc.aggBufferAttributes.size != 1 =>
          throw new GlutenNotSupportException("Only one input attribute is expected.")

        case _ @VeloxIntermediateData.Type(veloxTypes: Seq[DataType]) =>
          val rewrittenInputAttributes =
            rewriteAggBufferAttributes(functionInputAttributes, originalInputAttributes)
          // The process of handling the inconsistency in column types and order between
          // Spark and Velox is exactly the opposite of applyExtractStruct.
          aggregateExpression.mode match {
            case PartialMerge | Final =>
              val newInputAttributes = new ArrayBuffer[Attribute]()
              val childNodes = new JArrayList[ExpressionNode]()
              val (sparkOrders, sparkTypes) =
                aggFunc.aggBufferAttributes.map(attr => (attr.name, attr.dataType)).unzip
              val veloxOrders = VeloxIntermediateData.veloxIntermediateDataOrder(aggFunc)
              val adjustedOrders = veloxOrders.map(sparkOrders.indexOf(_))
              veloxTypes.zipWithIndex.foreach {
                case (veloxType, idx) =>
                  val adjustedIdx = adjustedOrders(idx)
                  if (adjustedIdx == -1) {
                    // The Velox aggregate intermediate buffer column not found in Spark.
                    // For example, skewness and kurtosis share the same aggregate buffer in Velox,
                    // and Kurtosis additionally requires the buffer column of m4, which is
                    // always 0 for skewness. In Spark, the aggregate buffer of skewness does not
                    // have the column of m4, thus a placeholder m4 with a value of 0 must be passed
                    // to Velox, and this value cannot be omitted. Velox will always read m4 column
                    // when accessing the intermediate data.
                    val extraAttr = AttributeReference(veloxOrders(idx), veloxType)()
                    newInputAttributes += extraAttr
                    val lt = Literal.default(veloxType)
                    childNodes.add(ExpressionBuilder.makeLiteral(lt.value, lt.dataType, false))
                  } else {
                    val sparkType = sparkTypes(adjustedIdx)
                    val attr = rewrittenInputAttributes(adjustedIdx)
                    val aggFuncInputAttrNode = ExpressionConverter
                      .replaceWithExpressionTransformer(attr, originalInputAttributes)
                      .doTransform(args)
                    val expressionNode = if (sparkType != veloxType) {
                      newInputAttributes +=
                        attr.copy(dataType = veloxType)(attr.exprId, attr.qualifier)
                      ExpressionBuilder.makeCast(
                        ConverterUtils.getTypeNode(veloxType, attr.nullable),
                        aggFuncInputAttrNode,
                        SQLConf.get.ansiEnabled)
                    } else {
                      newInputAttributes += attr
                      aggFuncInputAttrNode
                    }
                    childNodes.add(expressionNode)
                  }
              }
              exprNodes.add(getRowConstructNode(args, childNodes, newInputAttributes, aggFunc))
            case other =>
              throw new GlutenNotSupportException(s"$other is not supported.")
          }

        case _ =>
          val rewrittenInputAttributes =
            rewriteAggBufferAttributes(functionInputAttributes, originalInputAttributes)
          val childNodes = rewrittenInputAttributes
            .map(
              ExpressionConverter
                .replaceWithExpressionTransformer(_, originalInputAttributes)
                .doTransform(args)
            )
            .asJava
          exprNodes.addAll(childNodes)
      }
    }

    // Create a project rel.
    val emitStartIndex = originalInputAttributes.size
    val projectRel = if (!validation) {
      RelBuilder.makeProjectRel(inputRel, exprNodes, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        inputRel,
        exprNodes,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }

    // Create aggregation rel.
    val groupingList = new JArrayList[ExpressionNode]()
    var colIdx = 0
    groupingExpressions.foreach {
      _ =>
        groupingList.add(ExpressionBuilder.makeSelection(colIdx))
        colIdx += 1
    }

    val aggFilterList = new JArrayList[ExpressionNode]()
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          throw new GlutenNotSupportException("Filter in final aggregation is not supported.")
        } else {
          // The number of filters should be aligned with that of aggregate functions.
          aggFilterList.add(null)
        }

        val aggFunc = aggExpr.aggregateFunction
        val childrenNodes = new JArrayList[ExpressionNode]()
        aggExpr.mode match {
          case PartialMerge | Final =>
            // Only occupies one column due to intermediate results are combined
            // by previous projection.
            childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
            colIdx += 1
          case Partial =>
            aggFunc.children.foreach {
              _ =>
                childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
                colIdx += 1
            }
          case _ =>
            throw new GlutenNotSupportException(
              s"$aggFunc of ${aggExpr.mode.toString} is not supported.")
        }
        addFunctionNode(args, aggFunc, childrenNodes, aggExpr.mode, aggregateFunctionList)
      })

    val extensionNode = getAdvancedExtension()
    RelBuilder.makeAggregateRel(
      projectRel,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      extensionNode,
      context,
      operatorId)
  }

  /**
   * Create and return the Rel for the this aggregation.
   * @param context
   *   the Substrait context
   * @param operatorId
   *   the operator id
   * @param aggParams
   *   the params for aggregation mainly used for metrics updating
   * @param input
   *   tht input rel node
   * @param validation
   *   whether this is for native validation
   * @return
   *   the rel node for this aggregation
   */
  override protected def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode = {
    val originalInputAttributes = child.output

    var aggRel = if (rowConstructNeeded(aggregateExpressions)) {
      aggParams.rowConstructionNeeded = true
      getAggRelWithRowConstruct(context, originalInputAttributes, operatorId, input, validation)
    } else {
      getAggRelInternal(context, originalInputAttributes, operatorId, input, validation)
    }

    if (extractStructNeeded()) {
      aggParams.extractionNeeded = true
      aggRel = applyExtractStruct(context, aggRel, operatorId, validation)
    }

    context.registerAggregationParam(operatorId, aggParams)
    aggRel
  }

  private def rewriteAggBufferAttributes(
      inputAggBufferAttributes: Seq[AttributeReference],
      originalInputAttributes: Seq[Attribute]): Seq[AttributeReference] = {
    inputAggBufferAttributes.map {
      attr =>
        val sameAttr = originalInputAttributes.find(_.exprId == attr.exprId)
        if (sameAttr.isEmpty) {
          // 1. When aggregateExpressions includes subquery, Spark's PlanAdaptiveSubqueries
          // Rule will transform the subquery within the final agg. The aggregateFunction
          // in the aggregateExpressions of the final aggregation will be cloned, resulting
          // in creating new aggregateFunction object. The inputAggBufferAttributes will
          // also generate new AttributeReference instances with larger exprId, which leads
          // to a failure in binding with the output of the partial agg. We need to adapt
          // to this situation; when encountering a failure to bind, it is necessary to
          // allow the binding of inputAggBufferAttribute with the same name but different
          // exprId.
          // 2. After apply `PullOutPreProject`, the aggregate expression may be created a new
          // instance.
          val attrsWithSameName =
            originalInputAttributes.drop(groupingExpressions.size).collect {
              case a if a.name == attr.name => a
            }
          val aggBufferAttrsWithSameName = aggregateExpressions.toIndexedSeq
            .flatMap(_.aggregateFunction.inputAggBufferAttributes)
            .filter(_.name == attr.name)
          assert(
            attrsWithSameName.size == aggBufferAttrsWithSameName.size,
            "The attribute with the same name in final agg inputAggBufferAttribute must" +
              "have the same size of corresponding attributes in originalInputAttributes."
          )
          attrsWithSameName(aggBufferAttrsWithSameName.indexOf(attr))
            .asInstanceOf[AttributeReference]
        } else {
          attr
        }
    }
  }

  private def getAggRelInternal(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode = null,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Get the grouping nodes.
    // Use 'child.output' as based Seq[Attribute], the originalInputAttributes
    // may be different for each backend.
    val groupingList = groupingExpressions
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, child.output)
          .doTransform(args))
      .asJava
    // Get the aggregate function nodes.
    val aggFilterList = new JArrayList[ExpressionNode]()
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          val exprNode = ExpressionConverter
            .replaceWithExpressionTransformer(aggExpr.filter.get, child.output)
            .doTransform(args)
          aggFilterList.add(exprNode)
        } else {
          // The number of filters should be aligned with that of aggregate functions.
          aggFilterList.add(null)
        }
        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodes = aggExpr.mode match {
          case Partial =>
            aggregateFunc.children.toList.map(
              expr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(expr, originalInputAttributes)
                  .doTransform(args)
              })
          case PartialMerge | Final =>
            rewriteAggBufferAttributes(
              aggregateFunc.inputAggBufferAttributes,
              originalInputAttributes).map {
              attr =>
                ExpressionConverter
                  .replaceWithExpressionTransformer(attr, originalInputAttributes)
                  .doTransform(args)
            }
          case other =>
            throw new GlutenNotSupportException(s"$other not supported.")
        }
        addFunctionNode(
          args,
          aggregateFunc,
          childrenNodes.asJava,
          aggExpr.mode,
          aggregateFunctionList)
      })

    val extensionNode = getAdvancedExtension(validation, originalInputAttributes)
    RelBuilder.makeAggregateRel(
      input,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      extensionNode,
      context,
      operatorId)
  }

  private def getAdvancedExtension(
      validation: Boolean = false,
      originalInputAttributes: Seq[Attribute] = Seq.empty): AdvancedExtensionNode = {
    val enhancement = if (validation) {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf)
    } else {
      null
    }

    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder
          .setValue(formatExtOptimizationString(isCapableForStreamingAggregation))
          .build)
    ExtensionBuilder.makeAdvancedExtension(optimization, enhancement)
  }

  def isStreaming: Boolean = false

  def numShufflePartitions: Option[Int] = Some(0)
}

/** An aggregation function builder specifically used by Velox backend. */
object VeloxAggregateFunctionsBuilder {

  /**
   * Create an scalar function for the input aggregate function.
   * @param args:
   *   the function map.
   * @param aggregateFunc:
   *   the input aggregate function.
   * @param forMergeCompanion:
   *   whether this is a special case to solve mixed aggregation phases.
   * @return
   */
  def create(
      args: java.lang.Object,
      aggregateFunc: AggregateFunction,
      mode: AggregateMode): Long = {
    val functionMap = args.asInstanceOf[JHashMap[String, JLong]]

    var sigName = ExpressionMappings.expressionsMap.get(aggregateFunc.getClass)
    if (sigName.isEmpty) {
      throw new GlutenNotSupportException(s"not currently supported: $aggregateFunc.")
    }

    aggregateFunc match {
      case First(_, ignoreNulls) =>
        if (ignoreNulls) sigName = Some(ExpressionNames.FIRST_IGNORE_NULL)
      case Last(_, ignoreNulls) =>
        if (ignoreNulls) sigName = Some(ExpressionNames.LAST_IGNORE_NULL)
      case _ =>
    }

    ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        // Substrait-to-Velox procedure will choose appropriate companion function if needed.
        sigName.get,
        VeloxIntermediateData.getInputTypes(aggregateFunc, mode == PartialMerge || mode == Final),
        FunctionConfig.REQ
      )
    )
  }
}

// Hash aggregation that emits full-aggregated data, this works like regular hash
// aggregation in Vanilla Spark.
case class RegularHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  override protected def allowFlush: Boolean = false

  override def simpleString(maxFields: Int): String =
    s"${super.simpleString(maxFields)}"

  override def verboseString(maxFields: Int): String =
    s"${super.verboseString(maxFields)}"

  override protected def withNewChildInternal(newChild: SparkPlan): HashAggregateExecTransformer = {
    copy(child = newChild)
  }
}

// Hash aggregation that emits pre-aggregated data which allows duplications on grouping keys
// among its output rows.
case class FlushableHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  override protected def allowFlush: Boolean = true

  override def simpleString(maxFields: Int): String =
    s"Flushable${super.simpleString(maxFields)}"

  override def verboseString(maxFields: Int): String =
    s"Flushable${super.verboseString(maxFields)}"

  override protected def withNewChildInternal(newChild: SparkPlan): HashAggregateExecTransformer = {
    copy(child = newChild)
  }
}

case class HashAggregateExecPullOutHelper(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute])
  extends HashAggregateExecPullOutBaseHelper(
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes) {

  /** This method calculates the output attributes of Aggregation. */
  override protected def getAttrForAggregateExprs: List[Attribute] = {
    aggregateExpressions.zipWithIndex.flatMap {
      case (expr, index) =>
        handleSpecialAggregateAttr
          .lift(expr)
          .getOrElse(expr.mode match {
            case Partial | PartialMerge =>
              expr.aggregateFunction.aggBufferAttributes
            case Final =>
              Seq(aggregateAttributes(index))
            case other =>
              throw new GlutenNotSupportException(s"Unsupported aggregate mode: $other.")
          })
    }.toList
  }

  private val handleSpecialAggregateAttr: PartialFunction[AggregateExpression, Seq[Attribute]] = {
    case ae: AggregateExpression if RewriteTypedImperativeAggregate.shouldRewrite(ae) =>
      val aggBufferAttr = ae.aggregateFunction.inputAggBufferAttributes.head
      Seq(
        aggBufferAttr.copy(dataType = ae.aggregateFunction.dataType)(
          aggBufferAttr.exprId,
          aggBufferAttr.qualifier))
  }
}
