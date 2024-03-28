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
package org.apache.gluten.substrait.type;

import io.substrait.proto.Type;

import java.io.Serializable;

public class DecimalTypeNode implements TypeNode, Serializable {
  public final Boolean nullable;
  public final int precision;
  public final int scale;

  public DecimalTypeNode(Boolean nullable, int precision, int scale) {
    this.nullable = nullable;
    this.precision = precision;
    this.scale = scale;
  }

  @Override
  public Type toProtobuf() {
    Type.Decimal.Builder decimalBuilder = Type.Decimal.newBuilder();
    decimalBuilder.setPrecision(precision);
    decimalBuilder.setScale(scale);
    if (nullable) {
      decimalBuilder.setNullability(Type.Nullability.NULLABILITY_NULLABLE);
    } else {
      decimalBuilder.setNullability(Type.Nullability.NULLABILITY_REQUIRED);
    }

    Type.Builder builder = Type.newBuilder();
    builder.setDecimal(decimalBuilder.build());
    return builder.build();
  }
}
