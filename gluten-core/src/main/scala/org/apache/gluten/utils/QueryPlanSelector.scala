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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object PhysicalPlanSelector extends QueryPlanSelector[SparkPlan] {
  override protected def validate(plan: SparkPlan): Boolean = {
    BackendsApiManager.getValidatorApiInstance.doSparkPlanValidate(plan)
  }
}

object LogicalPlanSelector extends QueryPlanSelector[LogicalPlan] {
  override protected def validate(plan: LogicalPlan): Boolean = true
}

/** Select to decide whether a Spark plan can be accepted by Gluten for further execution. */
abstract class QueryPlanSelector[T <: QueryPlan[_]] extends Logging {

  private[this] def stackTrace(max: Int = 5): String = {
    val trim: Int = 6
    new Throwable().getStackTrace().slice(trim, trim + max).mkString("\n")
  }

  private def isGlutenEnabledForCurrentThread(session: SparkSession): Boolean = {
    val enabled =
      session.sparkContext.getLocalProperty(QueryPlanSelector.GLUTEN_ENABLE_FOR_THREAD_KEY)
    if (enabled != null) {
      enabled.toBoolean
    } else {
      true
    }
  }

  protected def validate(plan: T): Boolean

  private[this] def shouldUseGluten(session: SparkSession, plan: T): Boolean = {
    val glutenEnabled = session.conf
      .get(GlutenConfig.GLUTEN_ENABLE_KEY, GlutenConfig.GLUTEN_ENABLE_BY_DEFAULT.toString)
      .toBoolean && isGlutenEnabledForCurrentThread(session)
    if (log.isDebugEnabled) {
      logDebug(s"shouldUseGluten: $glutenEnabled")
      logDebug(
        s"=========================\n" +
          s"running shouldUseGluten from:\n${stackTrace()}\n" +
          s"plan:\n${plan.treeString}\n" +
          "=========================")
    }
    glutenEnabled & validate(plan)
  }

  def maybe(session: SparkSession, plan: T)(func: => T): T = {
    if (shouldUseGluten(session, plan)) func else plan
  }

  def maybeNil(session: SparkSession, plan: T)(func: => Seq[SparkPlan]): Seq[SparkPlan] = {
    if (shouldUseGluten(session, plan)) func else Nil
  }
}

object QueryPlanSelector {
  // control the usage of gluten at thread level
  val GLUTEN_ENABLE_FOR_THREAD_KEY = "gluten.enabledForCurrentThread"
}
