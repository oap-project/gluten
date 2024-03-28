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
import java.util.ArrayList;
import java.util.List;

public class StructNode implements TypeNode, Serializable {
  private final Boolean nullable;
  private final List<TypeNode> types = new ArrayList<>();
  private final List<String> names = new ArrayList<>();

  public StructNode(Boolean nullable, List<TypeNode> types, List<String> names) {
    this.nullable = nullable;
    this.types.addAll(types);
    this.names.addAll(names);
  }

  public StructNode(Boolean nullable, List<TypeNode> types) {
    this.nullable = nullable;
    this.types.addAll(types);
  }

  public List<TypeNode> getFieldTypes() {
    return types;
  }

  @Override
  public Type toProtobuf() {
    Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
    structBuilder.setNullability(
        nullable ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED);
    for (TypeNode typeNode : types) {
      structBuilder.addTypes(typeNode.toProtobuf());
    }
    for (String name : names) {
      structBuilder.addNames(name);
    }
    Type.Builder builder = Type.newBuilder();
    builder.setStruct(structBuilder.build());
    return builder.build();
  }
}
