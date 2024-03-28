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
package org.apache.gluten.substrait.rel;

import org.apache.gluten.substrait.expression.ExpressionNode;
import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;
import org.apache.gluten.substrait.type.ColumnTypeNode;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ReadRelNode implements RelNode, Serializable {
  private final List<TypeNode> types = new ArrayList<>();
  private final List<String> names = new ArrayList<>();
  private final List<ColumnTypeNode> columnTypeNodes = new ArrayList<>();
  private final ExpressionNode filterNode;
  private final AdvancedExtensionNode extensionNode;

  ReadRelNode(
      List<TypeNode> types,
      List<String> names,
      ExpressionNode filterNode,
      List<ColumnTypeNode> columnTypeNodes,
      AdvancedExtensionNode extensionNode) {
    this.types.addAll(types);
    this.names.addAll(names);
    this.filterNode = filterNode;
    this.columnTypeNodes.addAll(columnTypeNodes);
    this.extensionNode = extensionNode;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
    for (TypeNode typeNode : types) {
      structBuilder.addTypes(typeNode.toProtobuf());
    }

    NamedStruct.Builder nStructBuilder = NamedStruct.newBuilder();
    nStructBuilder.setStruct(structBuilder.build());
    for (String name : names) {
      nStructBuilder.addNames(name);
    }

    if (!columnTypeNodes.isEmpty()) {
      for (ColumnTypeNode columnTypeNode : columnTypeNodes) {
        nStructBuilder.addColumnTypes(columnTypeNode.toProtobuf());
      }
    }

    ReadRel.Builder readBuilder = ReadRel.newBuilder();
    readBuilder.setCommon(relCommonBuilder.build());
    readBuilder.setBaseSchema(nStructBuilder.build());

    if (filterNode != null) {
      readBuilder.setFilter(filterNode.toProtobuf());
    }

    if (extensionNode != null) {
      readBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }

    Rel.Builder builder = Rel.newBuilder();
    builder.setRead(readBuilder.build());
    return builder.build();
  }
}
