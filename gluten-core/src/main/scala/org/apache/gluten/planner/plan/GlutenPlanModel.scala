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
package org.apache.gluten.planner.plan

import org.apache.gluten.cbo.PlanModel
import org.apache.gluten.cbo.property.PropertySet
import org.apache.gluten.planner.property.GlutenProperties
import org.apache.gluten.planner.property.GlutenProperties.Conventions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}

import java.util.Objects

object GlutenPlanModel {
  def apply(): PlanModel[SparkPlan] = {
    PlanModelImpl
  }

  case class GroupLeafExec(groupId: Int, propertySet: PropertySet[SparkPlan]) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] = throw new IllegalStateException()
    override def output: Seq[Attribute] = propertySet.get(GlutenProperties.SCHEMA_DEF).output
    override def supportsColumnar: Boolean =
      propertySet.get(GlutenProperties.CONVENTION_DEF) match {
        case Conventions.ROW_BASED => false
        case Conventions.VANILLA_COLUMNAR => true
        case Conventions.GLUTEN_COLUMNAR => true
        case Conventions.ANY => true
      }
  }

  private object PlanModelImpl extends PlanModel[SparkPlan] {
    override def childrenOf(node: SparkPlan): Seq[SparkPlan] = node.children

    override def withNewChildren(node: SparkPlan, children: Seq[SparkPlan]): SparkPlan = {
      node.withNewChildren(children)
    }

    override def hashCode(node: SparkPlan): Int = Objects.hashCode(node)

    override def equals(one: SparkPlan, other: SparkPlan): Boolean = Objects.equals(one, other)

    override def newGroupLeaf(groupId: Int, propSet: PropertySet[SparkPlan]): SparkPlan =
      GroupLeafExec(groupId, propSet)

    override def isGroupLeaf(node: SparkPlan): Boolean = node match {
      case _: GroupLeafExec => true
      case _ => false
    }

    override def getGroupId(node: SparkPlan): Int = node match {
      case gl: GroupLeafExec => gl.groupId
      case _ => throw new IllegalStateException()
    }
  }
}
