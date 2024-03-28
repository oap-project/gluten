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
package org.apache.gluten.vectorized;

import java.io.Serializable;

public class ColumnarBatchSerializeResult implements Serializable {
  public static final ColumnarBatchSerializeResult EMPTY =
      new ColumnarBatchSerializeResult(0, new byte[0]);

  private long numRows;

  private byte[] serialized;

  public ColumnarBatchSerializeResult(long numRows, byte[] serialized) {
    this.numRows = numRows;
    this.serialized = serialized;
  }

  public long getNumRows() {
    return numRows;
  }

  public byte[] getSerialized() {
    return serialized;
  }
}
