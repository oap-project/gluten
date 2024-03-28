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

import org.apache.gluten.substrait.type.MapNode;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Expression;
import io.substrait.proto.Expression.Literal.Builder;
import io.substrait.proto.Type;
import org.apache.spark.sql.catalyst.util.MapData;

public class MapLiteralNode extends LiteralNodeWithValue<MapData> {
  public MapLiteralNode(MapData map, TypeNode typeNode) {
    super(map, typeNode);
  }

  @Override
  protected void updateLiteralBuilder(Builder literalBuilder, MapData map) {
    Object[] keys = map.keyArray().array();
    Object[] values = map.valueArray().array();
    TypeNode mapType = ((MapNode) getTypeNode()).getKeyType();
    TypeNode valueType = ((MapNode) getTypeNode()).getValueType();

    if (keys.length > 0) {
      Expression.Literal.Map.Builder mapBuilder = Expression.Literal.Map.newBuilder();
      for (int i = 0; i < keys.length; ++i) {
        LiteralNode keyNode = ExpressionBuilder.makeLiteral(keys[i], mapType);
        LiteralNode valueNode = ExpressionBuilder.makeLiteral(values[i], valueType);

        Expression.Literal.Map.KeyValue.Builder kvBuilder =
            Expression.Literal.Map.KeyValue.newBuilder();
        kvBuilder.setKey(keyNode.getLiteral());
        kvBuilder.setValue(valueNode.getLiteral());
        mapBuilder.addKeyValues(kvBuilder.build());
      }

      literalBuilder.setMap(mapBuilder.build());
    } else {
      Type.Map.Builder mapTypeBuilder = Type.Map.newBuilder();
      mapTypeBuilder.setKey(mapType.toProtobuf());
      mapTypeBuilder.setValue(valueType.toProtobuf());
      literalBuilder.setEmptyMap(mapTypeBuilder.build());
    }
  }
}
