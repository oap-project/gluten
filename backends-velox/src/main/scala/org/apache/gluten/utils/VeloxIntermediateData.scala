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
package org.apache.gluten.utils

import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object VeloxIntermediateData {
  // Agg functions with inconsistent ordering of intermediate data between Velox and Spark.
  // Corr
  private val veloxCorrIntermediateDataOrder: Seq[String] =
    Seq("ck", "n", "xMk", "yMk", "xAvg", "yAvg")
  // CovPopulation, CovSample
  private val veloxCovarIntermediateDataOrder: Seq[String] = Seq("ck", "n", "xAvg", "yAvg")
  // Skewness
  private val veloxSkewnessIntermediateDataOrder: Seq[String] = Seq("n", "avg", "m2", "m3", "m4")

  // Agg functions with inconsistent types of intermediate data between Velox and Spark.
  // StddevSamp, StddevPop, VarianceSamp, VariancePop
  private val veloxVarianceIntermediateTypes: Seq[DataType] = Seq(LongType, DoubleType, DoubleType)
  // CovPopulation, CovSample
  private val veloxCovarIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType)
  // Corr
  private val veloxCorrIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType, DoubleType, DoubleType)
  // Skewness
  private val veloxSkewnessIntermediateTypes: Seq[DataType] =
    Seq(LongType, DoubleType, DoubleType, DoubleType, DoubleType)

  /**
   * Return the intermediate columns order of Velox aggregation functions, with special matching
   * required for some aggregation functions where the intermediate columns order are inconsistent
   * with Spark.
   * @param aggFunc
   *   Spark aggregation function
   * @return
   *   the intermediate columns order of Velox aggregation functions
   */
  def veloxIntermediateDataOrder(aggFunc: AggregateFunction): Seq[String] = {
    aggFunc match {
      case _: Corr =>
        veloxCorrIntermediateDataOrder
      case _: CovPopulation | _: CovSample =>
        veloxCovarIntermediateDataOrder
      case _: Skewness =>
        veloxSkewnessIntermediateDataOrder
      case _ =>
        aggFunc.aggBufferAttributes.map(_.name)
    }
  }

  /**
   * Get the compatible input types for a Velox aggregate function.
   *
   * @param aggregateFunc
   *   The input aggregate function.
   * @param forMergeCompanion
   *   Whether this is a special case to solve mixed aggregation phases.
   * @return
   *   The input types of a Velox aggregate function.
   */
  def getInputTypes(aggregateFunc: AggregateFunction, forMergeCompanion: Boolean): Seq[DataType] = {
    if (!forMergeCompanion) {
      return aggregateFunc.children.map(_.dataType)
    }
    aggregateFunc match {
      case _ @Type(veloxDataTypes: Seq[DataType]) =>
        Seq(StructType(veloxDataTypes.map(StructField("", _)).toArray))
      case _: CollectList | _: CollectSet =>
        // CollectList and CollectSet should use data type of agg function.
        Seq(aggregateFunc.dataType)
      case _ =>
        // Not use StructType for single column agg intermediate data
        aggregateFunc.aggBufferAttributes.map(_.dataType)
    }
  }

  /**
   * Return the intermediate type node of a partial aggregation in Velox.
   *
   * @param aggFunc
   *   Spark aggregation function.
   * @return
   *   The type of partial outputs.
   */
  def getIntermediateTypeNode(aggFunc: AggregateFunction): TypeNode = {
    val structTypeNodes =
      aggFunc match {
        case _ @Type(dataTypes: Seq[DataType]) =>
          dataTypes.map(ConverterUtils.getTypeNode(_, nullable = false))
        case _ =>
          throw new UnsupportedOperationException("Can not get velox intermediate types.")
      }
    TypeBuilder.makeStruct(false, structTypeNodes.asJava)
  }

  /**
   * Obtain the name of the RowConstruct function, only decimal avg and sum currently require the
   * use of row_constructor, while the rest use the Gluten custom modified
   * row_constructor_with_null.
   */
  def getRowConstructFuncName(aggFunc: AggregateFunction): String = aggFunc match {
    case _: Average | _: Sum if aggFunc.dataType.isInstanceOf[DecimalType] => "row_constructor"
    case _ => "row_constructor_with_null"
  }

  object Type {

    /**
     * Return the intermediate types of Velox agg functions, with special matching required for some
     * aggregation functions where the intermediate results are inconsistent with Spark. Only return
     * if the intermediate result has multiple columns.
     * @param aggFunc
     *   Spark aggregation function
     * @return
     *   the intermediate types of Velox aggregation functions.
     */
    def unapply(aggFunc: AggregateFunction): Option[Seq[DataType]] = {
      aggFunc match {
        case _: Corr =>
          Some(veloxCorrIntermediateTypes)
        case _: Covariance =>
          Some(veloxCovarIntermediateTypes)
        case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
          Some(veloxVarianceIntermediateTypes)
        case _: Skewness =>
          Some(veloxSkewnessIntermediateTypes)
        case _ if aggFunc.aggBufferAttributes.size > 1 =>
          Some(aggFunc.aggBufferAttributes.map(_.dataType))
        case _ => None
      }
    }
  }
}
