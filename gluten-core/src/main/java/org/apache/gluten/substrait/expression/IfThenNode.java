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

import io.substrait.proto.Expression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IfThenNode implements ExpressionNode, Serializable {

  private final List<ExpressionNode> ifNodes = new ArrayList<>();
  private final List<ExpressionNode> thenNodes = new ArrayList<>();

  private final ExpressionNode elseValue;

  public IfThenNode(
      List<ExpressionNode> ifNodes, List<ExpressionNode> thenNodes, ExpressionNode elseValue) {
    this.ifNodes.addAll(ifNodes);
    this.thenNodes.addAll(thenNodes);
    this.elseValue = elseValue;
  }

  @Override
  public Expression toProtobuf() {
    if (ifNodes.size() != thenNodes.size()) {
      throw new RuntimeException("The length of if nodes and then nodes is different.");
    }
    Expression.IfThen.Builder ifThenBuilder = Expression.IfThen.newBuilder();

    int ifNodesLen = ifNodes.size();
    for (int i = 0; i < ifNodesLen; i++) {
      Expression.IfThen.IfClause.Builder ifClauseBuilder = Expression.IfThen.IfClause.newBuilder();
      ifClauseBuilder.setIf(ifNodes.get(i).toProtobuf());
      ifClauseBuilder.setThen(thenNodes.get(i).toProtobuf());

      ifThenBuilder.addIfs(ifClauseBuilder.build());
    }

    if (elseValue != null) {
      ifThenBuilder.setElse(elseValue.toProtobuf());
    }

    Expression.Builder builder = Expression.newBuilder();
    builder.setIfThen(ifThenBuilder.build());
    return builder.build();
  }
}
