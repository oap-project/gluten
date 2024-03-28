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
package org.apache.gluten.substrait.derivation;

import io.substrait.proto.DerivationExpression;
import io.substrait.proto.Type;

import java.io.Serializable;

public class DerivationFP64TypeNode implements DerivationExpressionNode, Serializable {
  private final Boolean nullable;

  DerivationFP64TypeNode(Boolean nullable) {
    this.nullable = nullable;
  }

  @Override
  public DerivationExpression toProtobuf() {
    Type.FP64.Builder doubleBuilder = Type.FP64.newBuilder();
    if (nullable) {
      doubleBuilder.setNullability(Type.Nullability.NULLABILITY_NULLABLE);
    } else {
      doubleBuilder.setNullability(Type.Nullability.NULLABILITY_REQUIRED);
    }

    DerivationExpression.Builder builder = DerivationExpression.newBuilder();
    builder.setFp64(doubleBuilder.build());
    return builder.build();
  }
}
