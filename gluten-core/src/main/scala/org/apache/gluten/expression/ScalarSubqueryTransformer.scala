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
package org.apache.gluten.expression

import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BaseSubqueryExec, ScalarSubquery}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

case class ScalarSubqueryTransformer(plan: BaseSubqueryExec, exprId: ExprId, query: ScalarSubquery)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // don't trigger collect when in validation phase
    if (
      TransformerState.underValidationState &&
      !valueSensitiveDataType(query.dataType)
    ) {
      return ExpressionBuilder.makeLiteral(null, query.dataType, true)
    }
    // the first column in first row from `query`.
    val rows = query.plan.executeCollect()
    if (rows.length > 1) {
      throw new IllegalStateException(
        s"more than one row returned by a subquery used as an expression:\n${query.plan}")
    }
    val result: AnyRef = if (rows.length == 1) {
      assert(
        rows(0).numFields == 1,
        s"Expects 1 field, but got ${rows(0).numFields}; something went wrong in analysis")
      rows(0).get(0, query.dataType)
    } else {
      // If there is no rows returned, the result should be null.
      null
    }
    ExpressionBuilder.makeLiteral(result, query.dataType, result == null)
  }

  /**
   * DataTypes which supported or not depend on actual value
   *
   * @param dataType
   * @return
   */
  def valueSensitiveDataType(dataType: DataType): Boolean = {
    dataType.isInstanceOf[MapType] ||
    dataType.isInstanceOf[ArrayType] ||
    dataType.isInstanceOf[StructType]
  }
}
