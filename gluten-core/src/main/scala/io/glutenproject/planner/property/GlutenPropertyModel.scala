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
package io.glutenproject.planner.property

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.cbo._
import io.glutenproject.cbo.rule.{CboRule, Shape, Shapes}
import io.glutenproject.extension.columnar.ColumnarTransitions
import io.glutenproject.planner.plan.GlutenPlanModel.GroupLeafExec
import io.glutenproject.planner.property.GlutenProperties.{Convention, CONVENTION_DEF, ConventionEnforcerRule, SCHEMA_DEF}
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.utils.PlanUtil

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution._

object GlutenProperties {
  val SCHEMA_DEF: PropertyDef[SparkPlan, Schema] = new PropertyDef[SparkPlan, Schema] {
    override def getProperty(plan: SparkPlan): Schema = plan match {
      case _: GroupLeafExec => throw new IllegalStateException()
      case _ => Schema(plan.output)
    }
    override def getChildrenConstraints(
        constraint: Property[SparkPlan],
        plan: SparkPlan): Seq[Schema] = {
      plan.children.map(c => Schema(c.output))
    }
  }

  val CONVENTION_DEF: PropertyDef[SparkPlan, Convention] = new PropertyDef[SparkPlan, Convention] {
    // TODO: Should the convention-transparent ops (e.g., aqe shuffle read) support
    //  convention-propagation. Probably need to refactor getChildrenPropertyRequirements.
    override def getProperty(plan: SparkPlan): Convention = plan match {
      case _: GroupLeafExec => throw new IllegalStateException()
      case ColumnarToRowExec(child) => Conventions.ROW_BASED
      case RowToColumnarExec(child) => Conventions.VANILLA_COLUMNAR
      case ColumnarTransitions.ColumnarToRowLike(child) => Conventions.ROW_BASED
      case ColumnarTransitions.RowToColumnarLike(child) => Conventions.GLUTEN_COLUMNAR
      case p if PlanUtil.outputNativeColumnarData(p) => Conventions.GLUTEN_COLUMNAR
      case p if PlanUtil.isVanillaColumnarOp(p) => Conventions.VANILLA_COLUMNAR
      case p if SparkShimLoader.getSparkShims.supportsRowBased(p) => Conventions.ROW_BASED
      case _ => throw new IllegalStateException()
    }

    override def getChildrenConstraints(
        constraint: Property[SparkPlan],
        plan: SparkPlan): Seq[Convention] = plan match {
      case ColumnarToRowExec(child) => Seq(Conventions.VANILLA_COLUMNAR)
      case ColumnarTransitions.ColumnarToRowLike(child) => Seq(Conventions.GLUTEN_COLUMNAR)
      case ColumnarTransitions.RowToColumnarLike(child) => Seq(Conventions.ROW_BASED)
      case _ =>
        val conv = getProperty(plan)
        plan.children.map(_ => conv)
    }
  }

  case class ConventionEnforcerRule(reqConv: Convention) extends CboRule[SparkPlan] {
    override def shift(node: SparkPlan): Iterable[SparkPlan] = {
      val conv = CONVENTION_DEF.getProperty(node)
      if (conv == reqConv) {
        return List.empty
      }
      (conv, reqConv) match {
        case (Conventions.VANILLA_COLUMNAR, Conventions.ROW_BASED) =>
          List(ColumnarToRowExec(node))
        case (Conventions.ROW_BASED, Conventions.VANILLA_COLUMNAR) =>
          List(RowToColumnarExec(node))
        case (Conventions.GLUTEN_COLUMNAR, Conventions.ROW_BASED) =>
          List(BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToRowExec(node))
        case (Conventions.ROW_BASED, Conventions.GLUTEN_COLUMNAR) =>
          val attempt = BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(node)
          if (attempt.doValidate().isValid) {
            List(attempt)
          } else {
            List.empty
          }
        case (Conventions.VANILLA_COLUMNAR, Conventions.GLUTEN_COLUMNAR) =>
          List(
            BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(
              ColumnarToRowExec(node)))
        case (Conventions.GLUTEN_COLUMNAR, Conventions.VANILLA_COLUMNAR) =>
          List(
            RowToColumnarExec(
              BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToRowExec(node)))
        case _ => List.empty
      }
    }

    override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
  }

  case class Schema(output: Seq[Attribute]) extends Property[SparkPlan] {
    override def satisfies(other: Property[SparkPlan]): Boolean = other match {
      case Schemas.ANY => true
      case Schema(otherOutput) => output == otherOutput
      case _ => throw new IllegalStateException()
    }

    override def definition(): PropertyDef[SparkPlan, _ <: Property[SparkPlan]] = {
      SCHEMA_DEF
    }
  }

  object Schemas {
    val ANY: Property[SparkPlan] = Schema(List())
  }

  sealed trait Convention extends Property[SparkPlan] {
    override def definition(): PropertyDef[SparkPlan, _ <: Property[SparkPlan]] = {
      CONVENTION_DEF
    }

    override def satisfies(other: Property[SparkPlan]): Boolean = other match {
      case Conventions.ANY => true
      case c: Convention => c == this
      case _ => throw new IllegalStateException()
    }
  }

  object Conventions {
    // FIXME: Velox and CH should have different conventions?
    case object ROW_BASED extends Convention
    case object VANILLA_COLUMNAR extends Convention
    case object GLUTEN_COLUMNAR extends Convention
    case object ANY extends Convention
  }
}

object GlutenPropertyModel {

  def apply(): PropertyModel[SparkPlan] = {
    PropertyModelImpl
  }

  private object PropertyModelImpl extends PropertyModel[SparkPlan] {
    override def propertyDefs: Seq[PropertyDef[SparkPlan, _ <: Property[SparkPlan]]] =
      Seq(SCHEMA_DEF, CONVENTION_DEF)

    override def newEnforcerRuleFactory(
        propertyDef: PropertyDef[SparkPlan, _ <: Property[SparkPlan]])
        : EnforcerRuleFactory[SparkPlan] = (reqProp: Property[SparkPlan]) => {
      propertyDef match {
        case SCHEMA_DEF =>
          Seq()
        case CONVENTION_DEF =>
          Seq(ConventionEnforcerRule(reqProp.asInstanceOf[Convention]))
      }
    }
  }
}
