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

import org.apache.gluten.metrics.IMetrics;

import org.apache.spark.sql.execution.utils.CHExecUtil;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class BatchIterator extends GeneralOutIterator {
  private final long handle;

  public BatchIterator(long handle) {
    super();
    this.handle = handle;
  }

  @Override
  public String getId() {
    // Using native handle as identifier
    return String.valueOf(handle);
  }

  private native boolean nativeHasNext(long nativeHandle);

  private native byte[] nativeNext(long nativeHandle);

  private native long nativeCHNext(long nativeHandle);

  private native void nativeClose(long nativeHandle);

  private native IMetrics nativeFetchMetrics(long nativeHandle);

  @Override
  public boolean hasNextInternal() throws IOException {
    return nativeHasNext(handle);
  }

  @Override
  public ColumnarBatch nextInternal() throws IOException {
    long block = nativeCHNext(handle);
    CHNativeBlock nativeBlock = new CHNativeBlock(block);
    int cols = nativeBlock.numColumns();
    ColumnVector[] columnVectors = new ColumnVector[cols];
    for (int i = 0; i < cols; i++) {
      columnVectors[i] =
          new CHColumnVector(
              CHExecUtil.inferSparkDataType(nativeBlock.getTypeByPosition(i)), block, i);
    }
    return new ColumnarBatch(columnVectors, nativeBlock.numRows());
  }

  @Override
  public IMetrics getMetricsInternal() throws IOException, ClassNotFoundException {
    return nativeFetchMetrics(handle);
  }

  @Override
  public void closeInternal() {
    nativeClose(handle);
  }
}
