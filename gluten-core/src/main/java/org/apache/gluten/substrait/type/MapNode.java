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

public class MapNode implements TypeNode, Serializable {
  private final Boolean nullable;
  private final TypeNode keyType;
  private final TypeNode valType;

  public MapNode(Boolean nullable, TypeNode keyType, TypeNode valType) {
    this.nullable = nullable;
    this.keyType = keyType;
    this.valType = valType;
  }

  // It's used in ExplodeTransformer to determine output datatype from children.
  public TypeNode getNestedType() {
    List<TypeNode> types = new ArrayList<>();
    types.add(keyType);
    types.add(valType);
    return TypeBuilder.makeStruct(false, types);
  }

  public TypeNode getKeyType() {
    return keyType;
  }

  public TypeNode getValueType() {
    return valType;
  }

  @Override
  public Type toProtobuf() {
    Type.Map.Builder mapBuilder = Type.Map.newBuilder();
    mapBuilder.setKey(keyType.toProtobuf());
    mapBuilder.setValue(valType.toProtobuf());
    mapBuilder.setNullability(
        nullable ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED);

    Type.Builder builder = Type.newBuilder();
    builder.setMap(mapBuilder.build());
    return builder.build();
  }
}
