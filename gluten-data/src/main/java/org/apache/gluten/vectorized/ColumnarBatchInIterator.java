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

import org.apache.gluten.columnarbatch.ColumnarBatchJniWrapper;
import org.apache.gluten.columnarbatch.ColumnarBatches;
import org.apache.gluten.memory.arrowalloc.ArrowBufferAllocators;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;

public class ColumnarBatchInIterator extends GeneralInIterator {
  public ColumnarBatchInIterator(Iterator<ColumnarBatch> delegated) {
    super(delegated);
  }

  public long next() {
    final ColumnarBatch next = nextColumnarBatch();
    if (next.numCols() == 0) {
      // the operation will find a zero column batch from a task-local pool
      return ColumnarBatchJniWrapper.create().getForEmptySchema(next.numRows());
    }
    final ColumnarBatch offloaded =
        ColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), next);
    return ColumnarBatches.getNativeHandle(offloaded);
  }
}
