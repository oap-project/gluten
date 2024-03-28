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

import org.apache.gluten.cbo.{CboExplain, Optimization}
import org.apache.gluten.cbo.rule.CboRule
import org.apache.gluten.planner.cost.GlutenCostModel
import org.apache.gluten.planner.plan.GlutenPlanModel
import org.apache.gluten.planner.property.GlutenPropertyModel
import org.apache.gluten.planner.rule.GlutenRules

import org.apache.spark.sql.execution.SparkPlan

object GlutenOptimization {
  private object GlutenExplain extends CboExplain[SparkPlan] {
    override def describeNode(node: SparkPlan): String = node.nodeName
  }

  def apply(): Optimization[SparkPlan] = {
    Optimization[SparkPlan](
      GlutenCostModel(),
      GlutenPlanModel(),
      GlutenPropertyModel(),
      GlutenExplain,
      CboRule.Factory.reuse(GlutenRules()))
  }

  def apply(rules: Seq[CboRule[SparkPlan]]): Optimization[SparkPlan] = {
    Optimization[SparkPlan](
      GlutenCostModel(),
      GlutenPlanModel(),
      GlutenPropertyModel(),
      GlutenExplain,
      CboRule.Factory.reuse(rules))
  }
}
