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
package org.apache.gluten.columnarbatch;

import org.apache.gluten.exec.Runtime;
import org.apache.gluten.exec.RuntimeAware;
import org.apache.gluten.exec.Runtimes;

public class ColumnarBatchJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private ColumnarBatchJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static ColumnarBatchJniWrapper create() {
    return new ColumnarBatchJniWrapper(Runtimes.contextInstance());
  }

  public static ColumnarBatchJniWrapper forRuntime(Runtime runtime) {
    return new ColumnarBatchJniWrapper(runtime);
  }

  public native long createWithArrowArray(long cSchema, long cArray);

  public native long getForEmptySchema(int numRows);

  public native String getType(long batchHandle);

  public native long numColumns(long batchHandle);

  public native long numRows(long batchHandle);

  public native long numBytes(long batchHandle);

  public native long compose(long[] batches);

  public native void exportToArrow(long batch, long cSchema, long cArray);

  public native long select(
      long nativeMemoryManagerHandle, // why a mm is needed here?
      long batch,
      int[] columnIndices);

  public native void close(long batch);

  @Override
  public long handle() {
    return runtime.getHandle();
  }
}
