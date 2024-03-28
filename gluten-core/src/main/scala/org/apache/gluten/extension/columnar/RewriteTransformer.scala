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
package org.apache.gluten.extension.columnar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import java.util.ServiceLoader

import scala.collection.JavaConverters._

trait RewriteTransformerRules {
  def rules: Seq[Rule[SparkPlan]]
}

case class RewriteTransformer(session: SparkSession) extends Rule[SparkPlan] {

  private val rules: Seq[Rule[SparkPlan]] = RewriteTransformer.loadRewritePlanRules

  override def apply(plan: SparkPlan): SparkPlan = {
    rules.foldLeft(plan) {
      case (plan, rule) =>
        rule(plan)
    }
  }

}

object RewriteTransformer {

  private def loadRewritePlanRules: Seq[Rule[SparkPlan]] = {
    val loader = Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(getClass.getClassLoader)
    val serviceLoader = ServiceLoader.load(classOf[RewriteTransformerRules], loader)

    serviceLoader.asScala.flatMap(_.rules).toSeq
  }
}
