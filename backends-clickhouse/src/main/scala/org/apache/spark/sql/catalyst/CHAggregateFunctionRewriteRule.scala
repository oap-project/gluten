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

import io.glutenproject.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

/**
 * Avg(Int) function: CH use input type for intermediate sum type, while spark use double so need
 * convert .
 * @param spark
 */
case class CHAggregateFunctionRewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case a: Aggregate =>
      a.transformExpressions {
        case avgExpr @ AggregateExpression(avg: Average, _, _, _, _)
            if GlutenConfig.getConf.enableCastAvgAggregateFunction &&
              GlutenConfig.getConf.enableColumnarHashAgg &&
              !avgExpr.isDistinct && isDataTypeNeedConvert(avg.child.dataType) =>
          AggregateExpression(
            avg.copy(child = Cast(avg.child, DoubleType)),
            avgExpr.mode,
            avgExpr.isDistinct,
            avgExpr.filter,
            avgExpr.resultId
          )
      }
  }

  private def isDataTypeNeedConvert(dataType: DataType): Boolean = {
    dataType match {
      case FloatType => true
      case IntegerType => true
      case LongType => true
      case ShortType => true
      case _ => false
    }
  }
}
