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
package org.apache.gluten.planner

import org.apache.gluten.cbo.{Cbo, CboSuiteBase}
import org.apache.gluten.cbo.path.CboPath
import org.apache.gluten.cbo.property.PropertySet
import org.apache.gluten.cbo.rule.{CboRule, Shape, Shapes}
import org.apache.gluten.planner.property.GlutenProperties.{Conventions, Schemas}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution._
import org.apache.spark.sql.test.SharedSparkSession

class VeloxCboSuite extends SharedSparkSession {
  import VeloxCboSuite._

  test("C2R, R2C - basic") {
    val in = RowUnary(RowLeaf())
    val planner = newCbo().newPlanner(in)
    val out = planner.plan()
    assert(out == RowUnary(RowLeaf()))
  }

  test("C2R, R2C - explicitly requires any properties") {
    val in = RowUnary(RowLeaf())
    val planner =
      newCbo().newPlanner(in, PropertySet(List(Conventions.ANY, Schemas.ANY)))
    val out = planner.plan()
    assert(out == RowUnary(RowLeaf()))
  }

  test("C2R, R2C - requires columnar output") {
    val in = RowUnary(RowLeaf())
    val planner =
      newCbo().newPlanner(in, PropertySet(List(Conventions.VANILLA_COLUMNAR, Schemas.ANY)))
    val out = planner.plan()
    assert(out == RowToColumnarExec(RowUnary(RowLeaf())))
  }

  test("C2R, R2C - insert c2rs / r2cs") {
    val in =
      ColumnarUnary(RowUnary(RowUnary(ColumnarUnary(RowUnary(RowUnary(ColumnarUnary(RowLeaf())))))))
    val planner =
      newCbo().newPlanner(in, PropertySet(List(Conventions.ROW_BASED, Schemas.ANY)))
    val out = planner.plan()
    assert(out == ColumnarToRowExec(ColumnarUnary(
      RowToColumnarExec(RowUnary(RowUnary(ColumnarToRowExec(ColumnarUnary(RowToColumnarExec(
        RowUnary(RowUnary(ColumnarToRowExec(ColumnarUnary(RowToColumnarExec(RowLeaf()))))))))))))))
    val paths = planner.newState().memoState().collectAllPaths(CboPath.INF_DEPTH).toList
    val pathCount = paths.size
    assert(pathCount == 165)
  }

  test("C2R, R2C - Row unary convertible to Columnar") {
    object ConvertRowUnaryToColumnar extends CboRule[SparkPlan] {
      override def shift(node: SparkPlan): Iterable[SparkPlan] = node match {
        case RowUnary(child) => List(ColumnarUnary(child))
        case other => List.empty
      }

      override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
    }

    val in =
      ColumnarUnary(RowUnary(RowUnary(ColumnarUnary(RowUnary(RowUnary(ColumnarUnary(RowLeaf())))))))
    val planner =
      newCbo(List(ConvertRowUnaryToColumnar))
        .newPlanner(in, PropertySet(List(Conventions.ROW_BASED, Schemas.ANY)))
    val out = planner.plan()
    assert(out == ColumnarToRowExec(ColumnarUnary(ColumnarUnary(ColumnarUnary(
      ColumnarUnary(ColumnarUnary(ColumnarUnary(ColumnarUnary(RowToColumnarExec(RowLeaf()))))))))))
    val paths = planner.newState().memoState().collectAllPaths(CboPath.INF_DEPTH).toList
    val pathCount = paths.size
    assert(pathCount == 1094)
  }
}

object VeloxCboSuite extends CboSuiteBase {
  def newCbo(): Cbo[SparkPlan] = {
    GlutenOptimization().asInstanceOf[Cbo[SparkPlan]]
  }

  def newCbo(cboRules: Seq[CboRule[SparkPlan]]): Cbo[SparkPlan] = {
    GlutenOptimization(cboRules).asInstanceOf[Cbo[SparkPlan]]
  }

  case class RowLeaf() extends LeafExecNode {
    override def supportsColumnar: Boolean = false
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = Seq.empty
  }

  case class RowUnary(child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = false
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): RowUnary =
      copy(child = newChild)
  }

  case class ColumnarUnary(child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = true
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): ColumnarUnary =
      copy(child = newChild)
  }
}
