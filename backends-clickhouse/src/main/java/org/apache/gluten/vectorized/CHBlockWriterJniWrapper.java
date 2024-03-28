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

import org.apache.spark.sql.vectorized.ColumnarBatch;

public class CHBlockWriterJniWrapper {
  private long instance = 0;

  private native long nativeCreateInstance();

  private native void nativeWrite(long instance, long block);

  private native int nativeResultSize(long instance);

  private native void nativeCollect(long instance, byte[] data);

  private native void nativeClose(long instance);

  public void write(ColumnarBatch columnarBatch) {
    if (instance == 0) {
      instance = nativeCreateInstance();
    }
    nativeWrite(instance, CHNativeBlock.fromColumnarBatch(columnarBatch).blockAddress());
  }

  public byte[] collectAsByteArray() {
    if (instance == 0L) {
      return new byte[0];
    }
    byte[] result = new byte[nativeResultSize(instance)];
    nativeCollect(instance, result);
    nativeClose(instance);
    instance = 0;
    return result;
  }
}
