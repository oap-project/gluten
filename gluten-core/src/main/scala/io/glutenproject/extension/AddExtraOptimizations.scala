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
package io.glutenproject.extension

import io.glutenproject.extension.logical.LogicalPullOutPreProject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import scala.reflect.ClassTag

/**
 * Some rules need to be applied at the end of all Spark logical optimizers. This rule helps to add
 * logical optimizer to the experimental extra optimizations that will be applied in the end.
 */
case class AddExtraOptimizations(sparkSession: SparkSession) extends (LogicalPlan => Unit) {

  override def apply(plan: LogicalPlan): Unit = {

    def addIfNotExists[T <: Rule[LogicalPlan]](rule: T)(implicit tag: ClassTag[T]): Unit = {
      if (!sparkSession.experimental.extraOptimizations.exists(_.getClass == tag.runtimeClass)) {
        sparkSession.experimental.extraOptimizations ++= Seq(rule)
      }
    }

    addIfNotExists(LogicalPullOutPreProject(sparkSession))
  }
}
