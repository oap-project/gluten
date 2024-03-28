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
package org.apache.gluten.substrait.extensions;

import io.substrait.proto.SimpleExtensionDeclaration;

import java.io.Serializable;

public class FunctionMappingNode implements Serializable {
  private final String name;
  private final Long functionId;

  public FunctionMappingNode(String name, Long functionId) {
    this.name = name;
    this.functionId = functionId;
  }

  public SimpleExtensionDeclaration toProtobuf() {
    SimpleExtensionDeclaration.ExtensionFunction.Builder funcBuilder =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder();
    funcBuilder.setFunctionAnchor(functionId.intValue());
    funcBuilder.setName(name);

    SimpleExtensionDeclaration.Builder declaration = SimpleExtensionDeclaration.newBuilder();
    declaration.setExtensionFunction(funcBuilder.build());
    return declaration.build();
  }
}
