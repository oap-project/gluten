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
package io.glutenproject.planner.metadata

import io.glutenproject.cbo.{Metadata, MetadataModel}
import io.glutenproject.planner.metadata.GlutenMetadata.Schema
import io.glutenproject.planner.plan.GlutenPlanModel.GroupLeafExec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan

object GlutenMetadataModel extends Logging {
  def apply(): MetadataModel[SparkPlan] = {
    MetadataModelImpl
  }

  private object MetadataModelImpl extends MetadataModel[SparkPlan] {
    override def metadataOf(node: SparkPlan): Metadata = node match {
      case g: GroupLeafExec => throw new UnsupportedOperationException()
      case other => GlutenMetadata(Schema(other.output))
    }

    override def dummy(): Metadata = GlutenMetadata(Schema(List()))
    override def verify(one: Metadata, other: Metadata): Unit = (one, other) match {
      case (left: GlutenMetadata, right: GlutenMetadata) if left.schema() != right.schema() =>
        // We apply loose restriction on schema. Since Gluten still have some customized
        // logics causing schema of an operator to change after being transformed.
        // For example: https://github.com/apache/incubator-gluten/pull/5171
        logWarning(s"Warning: Schema mismatch: one: ${left.schema()}, other: ${right.schema()}")
      case (left: GlutenMetadata, right: GlutenMetadata) if left == right =>
      case _ => throw new IllegalStateException(s"Metadata mismatch: one: $one, other $other")
    }
  }
}
