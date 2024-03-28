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
package org.apache.gluten.substrait.ddlplan;

import org.apache.gluten.substrait.SubstraitContext;
import org.apache.gluten.substrait.plan.PlanNode;

import io.substrait.proto.DllPlan;
import io.substrait.proto.InsertPlan;

import java.io.Serializable;

public class InsertPlanNode implements DllPlanNode, Serializable {

  private final PlanNode inputNode;

  private final SubstraitContext context;

  public InsertPlanNode(SubstraitContext context, PlanNode inputNode) {
    this.inputNode = inputNode;
    this.context = context;
  }

  @Override
  public DllPlan toProtobuf() {
    InsertPlan.Builder insertBuilder = InsertPlan.newBuilder();
    insertBuilder.setInput(inputNode.toProtobuf());
    if (context.getInsertOutputNode() != null) {
      insertBuilder.setOutput(context.getInsertOutputNode().toProtobuf());
    }

    DllPlan.Builder dllBuilder = DllPlan.newBuilder();
    dllBuilder.setInsertPlan(insertBuilder.build());
    return dllBuilder.build();
  }
}
