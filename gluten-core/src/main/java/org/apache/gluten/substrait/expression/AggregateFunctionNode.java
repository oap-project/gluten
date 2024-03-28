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
package org.apache.gluten.substrait.expression;

import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.AggregateFunction;
import io.substrait.proto.AggregationPhase;
import io.substrait.proto.FunctionArgument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AggregateFunctionNode implements Serializable {
  private final Long functionId;
  private final List<ExpressionNode> expressionNodes = new ArrayList<>();
  private final String phase;
  private final TypeNode outputTypeNode;

  AggregateFunctionNode(
      Long functionId,
      List<ExpressionNode> expressionNodes,
      String phase,
      TypeNode outputTypeNode) {
    this.functionId = functionId;
    this.expressionNodes.addAll(expressionNodes);
    this.phase = phase;
    this.outputTypeNode = outputTypeNode;
  }

  public AggregateFunction toProtobuf() {
    AggregateFunction.Builder aggBuilder = AggregateFunction.newBuilder();
    aggBuilder.setFunctionReference(functionId.intValue());

    if (phase == null) {
      aggBuilder.setPhase(AggregationPhase.AGGREGATION_PHASE_UNSPECIFIED);
    } else {
      switch (phase) {
        case "PARTIAL":
          aggBuilder.setPhase(AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE);
          break;
        case "PARTIAL_MERGE":
          aggBuilder.setPhase(AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE);
          break;
        case "COMPLETE":
          aggBuilder.setPhase(AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT);
          break;
        case "FINAL":
          aggBuilder.setPhase(AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT);
          break;
        default:
          aggBuilder.setPhase(AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT);
      }
    }
    for (ExpressionNode expressionNode : expressionNodes) {
      FunctionArgument.Builder functionArgument = FunctionArgument.newBuilder();
      functionArgument.setValue(expressionNode.toProtobuf());
      aggBuilder.addArguments(functionArgument.build());
    }
    aggBuilder.setOutputType(outputTypeNode.toProtobuf());

    return aggBuilder.build();
  }
}
