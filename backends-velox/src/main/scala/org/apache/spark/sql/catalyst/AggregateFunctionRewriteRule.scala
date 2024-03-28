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
package org.apache.spark.sql.catalyst

import org.apache.gluten.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, HLLAdapter, HyperLogLogPlusPlus}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

case class AggregateFunctionRewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case a: Aggregate =>
      a.transformExpressions {
        case hllExpr @ AggregateExpression(hll: HyperLogLogPlusPlus, _, _, _, _)
            if GlutenConfig.getConf.enableNativeHyperLogLogAggregateFunction &&
              GlutenConfig.getConf.enableColumnarHashAgg &&
              !hasDistinctAggregateFunc(a) && isDataTypeSupported(hll.child.dataType) =>
          AggregateExpression(
            HLLAdapter(
              hll.child,
              Literal(hll.relativeSD),
              hll.mutableAggBufferOffset,
              hll.inputAggBufferOffset),
            hllExpr.mode,
            hllExpr.isDistinct,
            hllExpr.filter,
            hllExpr.resultId
          )
      }
  }

  private def hasDistinctAggregateFunc(agg: Aggregate): Boolean = {
    agg.aggregateExpressions
      .flatMap(_.collect { case ae: AggregateExpression => ae })
      .exists(_.isDistinct)
  }

  private def isDataTypeSupported(dataType: DataType): Boolean = {
    // HLL in velox only supports below data types. we should not offload HLL to velox, if
    // child's data type is not supported. This prevents the case only partail agg is fallbacked.
    // As spark and velox have different HLL binary formats, HLL binary generated by spark can't
    // be parsed by velox, it would cause the error: 'Unexpected type of HLL'.
    dataType match {
      case BooleanType => true
      case ByteType => true
      case _: CharType => true
      case DateType => true
      case DoubleType => true
      case FloatType => true
      case IntegerType => true
      case LongType => true
      case ShortType => true
      case StringType => true
      case _ => false
    }
  }
}
