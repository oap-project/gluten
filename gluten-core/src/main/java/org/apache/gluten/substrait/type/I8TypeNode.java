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

public class I8TypeNode implements TypeNode, Serializable {
  private final Boolean nullable;

  public I8TypeNode(Boolean nullable) {
    this.nullable = nullable;
  }

  @Override
  public Type toProtobuf() {
    Type.I8.Builder i8Builder = Type.I8.newBuilder();
    if (nullable) {
      i8Builder.setNullability(Type.Nullability.NULLABILITY_NULLABLE);
    } else {
      i8Builder.setNullability(Type.Nullability.NULLABILITY_REQUIRED);
    }

    Type.Builder builder = Type.newBuilder();
    builder.setI8(i8Builder.build());
    return builder.build();
  }
}
