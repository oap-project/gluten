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

import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode, IfThenNode}

import org.apache.spark.sql.catalyst.expressions._

import java.util.{ArrayList => JArrayList}

/** A version of substring that supports columnar processing for utf8. */
case class CaseWhenTransformer(
    branches: Seq[(ExpressionTransformer, ExpressionTransformer)],
    elseValue: Option[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // generate branches nodes
    val ifNodes = new JArrayList[ExpressionNode]
    val thenNodes = new JArrayList[ExpressionNode]
    branches.foreach(
      branch => {
        ifNodes.add(branch._1.doTransform(args))
        thenNodes.add(branch._2.doTransform(args))
      })
    val branchDataType = original.asInstanceOf[CaseWhen].inputTypesForMerging(0)
    // generate else value node, maybe null
    val elseValueNode = elseValue
      .map(_.doTransform(args))
      .getOrElse(ExpressionBuilder.makeLiteral(null, branchDataType, true))
    new IfThenNode(ifNodes, thenNodes, elseValueNode)
  }
}

case class IfTransformer(
    predicate: ExpressionTransformer,
    trueValue: ExpressionTransformer,
    falseValue: ExpressionTransformer,
    original: Expression)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val ifNodes = new JArrayList[ExpressionNode]
    ifNodes.add(predicate.doTransform(args))

    val thenNodes = new JArrayList[ExpressionNode]
    thenNodes.add(trueValue.doTransform(args))

    val elseValueNode = falseValue.doTransform(args)
    new IfThenNode(ifNodes, thenNodes, elseValueNode)
  }
}
