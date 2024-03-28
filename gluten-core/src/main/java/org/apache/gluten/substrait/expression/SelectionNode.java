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

public class SelectionNode implements ExpressionNode, Serializable {
  private final Integer fieldIndex;

  // The nested indices of child field. For case like a.b.c, the index of c is put at last.
  private final List<Integer> nestedChildIndices = new ArrayList<>();

  SelectionNode(Integer fieldIndex) {
    this.fieldIndex = fieldIndex;
  }

  SelectionNode(Integer fieldIndex, Integer childIndex) {
    this.fieldIndex = fieldIndex;
    this.nestedChildIndices.add(childIndex);
  }

  public SelectionNode addNestedChildIdx(Integer childIndex) {
    this.nestedChildIndices.add(childIndex);
    return this;
  }

  public Expression.ReferenceSegment createRef(
      Integer childIdx, Expression.ReferenceSegment childRef) {
    Expression.ReferenceSegment.StructField.Builder structBuilder =
        Expression.ReferenceSegment.StructField.newBuilder();
    structBuilder.setField(childIdx);
    if (childRef != null) {
      structBuilder.setChild(childRef);
    }

    Expression.ReferenceSegment.Builder refBuilder = Expression.ReferenceSegment.newBuilder();
    refBuilder.setStructField(structBuilder.build());
    return refBuilder.build();
  }

  @Override
  public Expression toProtobuf() {
    Expression.ReferenceSegment.StructField.Builder structBuilder =
        Expression.ReferenceSegment.StructField.newBuilder();
    structBuilder.setField(fieldIndex);

    // Handle the nested field indices.
    if (!nestedChildIndices.isEmpty()) {
      Expression.ReferenceSegment childRef = null;
      for (int i = nestedChildIndices.size() - 1; i >= 0; i--) {
        childRef = createRef(nestedChildIndices.get(i), childRef);
      }
      structBuilder.setChild(childRef);
    }

    Expression.ReferenceSegment.Builder refBuilder = Expression.ReferenceSegment.newBuilder();
    refBuilder.setStructField(structBuilder.build());

    Expression.FieldReference.Builder fieldBuilder = Expression.FieldReference.newBuilder();
    fieldBuilder.setDirectReference(refBuilder.build());

    Expression.Builder builder = Expression.newBuilder();
    builder.setSelection(fieldBuilder.build());
    return builder.build();
  }
}
