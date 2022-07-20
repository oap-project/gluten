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
import java.util.ArrayList

import scala.collection.JavaConverters._

import com.google.protobuf.Any
import io.glutenproject.expression._
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode, ScalarFunctionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import java.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.LongType

case class VeloxHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan) extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  // Create aggregate function node and add to list.
  override protected def addFunctionNode(
    args: java.lang.Object,
    aggregateFunction: AggregateFunction,
    childrenNodeList: ArrayList[ExpressionNode],
    aggregateMode: AggregateMode,
    aggregateNodeList: java.util.ArrayList[AggregateFunctionNode]): Unit = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    aggregateFunction match {
      case _: Average =>
        aggregateMode match {
          case Partial =>
            // Will use sum and count to replace partial avg.
            assert(childrenNodeList.size() == 1, "Partial Average expects one child node.")
            val inputType = if (aggregateFunction.children.head.isInstanceOf[Cast]) {
              aggregateFunction.children.head.dataType
            } else {
              aggregateFunction.inputAggBufferAttributes.head.dataType
            }

            val inputNullable = aggregateFunction.inputAggBufferAttributes.head.nullable

            val sumNode = ExpressionBuilder.makeAggregateFunction(
              ExpressionBuilder.newScalarFunction(
                functionMap,
                ConverterUtils
                  .makeFuncName(ConverterUtils.SUM, Seq(inputType), FunctionConfig.OPT)),
              childrenNodeList,
              modeToKeyWord(aggregateMode),
              ConverterUtils.getTypeNode(inputType, inputNullable))
            aggregateNodeList.add(sumNode)

            val countNode = ExpressionBuilder.makeAggregateFunction(
              ExpressionBuilder.newScalarFunction(
                functionMap,
                ConverterUtils
                  .makeFuncName(ConverterUtils.COUNT, Seq(inputType), FunctionConfig.OPT)),
              childrenNodeList,
              modeToKeyWord(aggregateMode),
              ConverterUtils.getTypeNode(LongType, nullable = true) /* return type for count */)
            aggregateNodeList.add(countNode)
          case Final =>
            val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
              AggregateFunctionsBuilder.create(args, aggregateFunction),
              childrenNodeList,
              modeToKeyWord(aggregateMode),
              ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
            aggregateNodeList.add(aggFunctionNode)
          case other =>
            throw new UnsupportedOperationException(s"$other is not supported.")
        }
      case _ =>
        val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
          AggregateFunctionsBuilder.create(args, aggregateFunction),
          childrenNodeList,
          modeToKeyWord(aggregateMode),
          ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
        aggregateNodeList.add(aggFunctionNode)
    }
  }

  // Return whether the outputs partial aggregation should be combined for Velox computing.
  // When the partial outputs are multiple-column, row construct is needed.
  private def rowConstructNeeded: Boolean = {
    for (aggregateExpression <- aggregateExpressions) {
      aggregateExpression.mode match {
        case PartialMerge | Final =>
          if (aggregateExpression.aggregateFunction.inputAggBufferAttributes.size > 1) {
            return true
          }
        case _ =>
      }
    }
    false
  }


  // Return a scalar function node representing row construct function in Velox.
  private def getRowConstructNode(
                                   args: java.lang.Object,
                                   childNodes: util.ArrayList[ExpressionNode],
                                   rowConstructAttributes: Seq[Attribute]): ScalarFunctionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.ROW_CONSTRUCTOR,
      rowConstructAttributes.map(attr => attr.dataType),
      FunctionConfig.NON)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)

    // Use struct type to represent Velox RowType.
    val structTypeNodes = new util.ArrayList[TypeNode](rowConstructAttributes.map(attr =>
      ConverterUtils.getTypeNode(attr.dataType, attr.nullable)
    ).asJava)

    ExpressionBuilder.makeScalarFunction(
      functionId, childNodes, TypeBuilder.makeStruct(structTypeNodes))
  }

  // Add a projection node before aggregation for row constructing.
  // Currently mainly used for final average.
  // Pre-projection is always not required for final stage.
  private def getAggRelWithRowConstruct(args: java.lang.Object,
                                        originalInputAttributes: Seq[Attribute],
                                        inputRel: RelNode,
                                        validation: Boolean): RelNode = {
    // Create a projection for row construct.
    val exprNodes = new util.ArrayList[ExpressionNode]()
    groupingExpressions.foreach(expr => {
      exprNodes.add(ExpressionConverter
        .replaceWithExpressionTransformer(expr, originalInputAttributes)
        .asInstanceOf[ExpressionTransformer].doTransform(args))
    })

    for (aggregateExpression <- aggregateExpressions) {
      val functionInputAttributes = aggregateExpression.aggregateFunction.inputAggBufferAttributes
      aggregateExpression.aggregateFunction match {
        case Average(_) =>
          aggregateExpression.mode match {
            case Final =>
              assert(
                functionInputAttributes.size == 2, "Final Average expects two input attributes.")
              // Use a Velox function to combine sum and count.
              val childNodes = new util.ArrayList[ExpressionNode](
                functionInputAttributes.toList.map(attr => {
                val aggExpr: Expression = ExpressionConverter
                  .replaceWithExpressionTransformer(attr, originalInputAttributes)
                aggExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
                }).asJava)
              exprNodes.add(getRowConstructNode(args, childNodes, functionInputAttributes))
            case other =>
              throw new UnsupportedOperationException(s"$other is not supported.")
          }
        case _ =>
          assert(functionInputAttributes.size == 1, "Only one input attribute is expected.")
          val childNodes = new util.ArrayList[ExpressionNode](
            functionInputAttributes.toList.map(attr => {
            val aggExpr: Expression = ExpressionConverter
              .replaceWithExpressionTransformer(attr, originalInputAttributes)
            aggExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
          }).asJava)
          exprNodes.addAll(childNodes)
      }
    }

    // Create a project rel.
    val projectRel = if (!validation) {
      RelBuilder.makeProjectRel(inputRel, exprNodes)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(inputRel, exprNodes, extensionNode)
    }

    // Create aggregation rel.
    val groupingList = new util.ArrayList[ExpressionNode]()
    var colIdx = 0
    groupingExpressions.foreach(_ => {
      groupingList.add(ExpressionBuilder.makeSelection(colIdx))
      colIdx += 1
    })

    val aggregateFunctionList = new util.ArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(aggExpr => {
      val aggregateFunc = aggExpr.aggregateFunction
      val childrenNodes = new util.ArrayList[ExpressionNode]()
      aggregateFunc match {
        case Average(_) =>
          // Only occupies one column due to sum and count are combined by previous projection.
          childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
          colIdx += 1
        case _ =>
          aggregateFunc.children.toList.map(_ => {
            childrenNodes.add(ExpressionBuilder.makeSelection(colIdx))
            colIdx += 1
            aggExpr
          })
      }
      addFunctionNode(args, aggregateFunc, childrenNodes, aggExpr.mode, aggregateFunctionList)
    })
    RelBuilder.makeAggregateRel(projectRel, groupingList, aggregateFunctionList)
  }

  override protected def getAggRel(args: java.lang.Object,
                                   input: RelNode = null,
                                   validation: Boolean = false): RelNode = {
    val originalInputAttributes = child.output
    val preProjectionNeeded = needsPreProjection

    val aggRel = if (preProjectionNeeded) {
      getAggRelWithPreProjection(args, originalInputAttributes, input, validation)
    } else {
      if (rowConstructNeeded) {
        getAggRelWithRowConstruct(args, originalInputAttributes, input, validation)
      } else {
        getAggRelWithoutPreProjection(args, originalInputAttributes, input, validation)
      }
    }

    applyPostProjection(args, aggRel, validation)
  }
}
