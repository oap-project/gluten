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

import org.apache.gluten.substrait.type.I32TypeNode;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Expression.Literal.Builder;

public class IntLiteralNode extends LiteralNodeWithValue<Integer> {
  public IntLiteralNode(Integer value) {
    super(value, new I32TypeNode(true));
  }

  public IntLiteralNode(Integer value, TypeNode typeNode) {
    super(value, typeNode);
  }

  @Override
  protected void updateLiteralBuilder(Builder literalBuilder, Integer value) {
    literalBuilder.setI32(value);
  }
}
