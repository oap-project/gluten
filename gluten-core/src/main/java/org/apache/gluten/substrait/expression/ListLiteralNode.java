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

import org.apache.gluten.substrait.type.ListNode;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Expression;
import io.substrait.proto.Expression.Literal.Builder;
import io.substrait.proto.Type;
import org.apache.spark.sql.catalyst.util.ArrayData;

public class ListLiteralNode extends LiteralNodeWithValue<ArrayData> {
  public ListLiteralNode(ArrayData array, TypeNode typeNode) {
    super(array, typeNode);
  }

  @Override
  protected void updateLiteralBuilder(Builder literalBuilder, ArrayData array) {
    Object[] elements = array.array();
    TypeNode elementType = ((ListNode) getTypeNode()).getNestedType();

    if (elements.length > 0) {
      Expression.Literal.List.Builder listBuilder = Expression.Literal.List.newBuilder();
      for (Object element : elements) {
        LiteralNode elementNode = ExpressionBuilder.makeLiteral(element, elementType);
        Expression.Literal elementExpr = elementNode.getLiteral();
        listBuilder.addValues(elementExpr);
      }
      literalBuilder.setList(listBuilder.build());
    } else {
      Type.List.Builder listTypeBuilder = Type.List.newBuilder();
      listTypeBuilder.setType(elementType.toProtobuf());
      literalBuilder.setEmptyList(listTypeBuilder.build());
    }
  }
}
