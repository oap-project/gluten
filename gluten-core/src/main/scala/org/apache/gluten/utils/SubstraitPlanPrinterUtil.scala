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

import org.apache.spark.internal.Logging

import com.google.protobuf.WrappersProto
import com.google.protobuf.util.JsonFormat
import io.substrait.proto.{NamedStruct, Plan}

object SubstraitPlanPrinterUtil extends Logging {

  /** Transform Substrait Plan to json format. */
  def substraitPlanToJson(substraintPlan: Plan): String = {
    val defaultRegistry = WrappersProto.getDescriptor.getMessageTypes
    val registry = com.google.protobuf.TypeRegistry
      .newBuilder()
      .add(substraintPlan.getDescriptorForType())
      .add(defaultRegistry)
      .build()
    JsonFormat.printer.usingTypeRegistry(registry).print(substraintPlan)
  }

  def substraitNamedStructToJson(substraintPlan: NamedStruct): String = {
    val defaultRegistry = WrappersProto.getDescriptor.getMessageTypes
    val registry = com.google.protobuf.TypeRegistry
      .newBuilder()
      .add(substraintPlan.getDescriptorForType())
      .add(defaultRegistry)
      .build()
    JsonFormat.printer.usingTypeRegistry(registry).print(substraintPlan)
  }

  /** Transform substrait plan json string to PlanNode */
  def jsonToSubstraitPlan(planJson: String): Plan = {
    try {
      val builder = Plan.newBuilder()
      val defaultRegistry = WrappersProto.getDescriptor.getMessageTypes
      val registry = com.google.protobuf.TypeRegistry
        .newBuilder()
        .add(builder.getDescriptorForType)
        .add(defaultRegistry)
        .build()
      JsonFormat.parser().usingTypeRegistry(registry).merge(planJson, builder)
      builder.build()
    } catch {
      case e: Exception =>
        logError("transform json string to substrait plan node error: ", e)
        null
    }
  }
}
