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
package org.apache.gluten.execution;

import org.apache.gluten.vectorized.CHColumnVector;
import org.apache.gluten.vectorized.GeneralInIterator;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;

public class ColumnarNativeIterator extends GeneralInIterator implements Iterator<byte[]> {

  public ColumnarNativeIterator(Iterator<ColumnarBatch> delegated) {
    super(delegated);
  }

  private transient ColumnarBatch nextBatch = null;

  private static byte[] longtoBytes(long data) {
    return new byte[] {
      (byte) ((data >> 56) & 0xff),
      (byte) ((data >> 48) & 0xff),
      (byte) ((data >> 40) & 0xff),
      (byte) ((data >> 32) & 0xff),
      (byte) ((data >> 24) & 0xff),
      (byte) ((data >> 16) & 0xff),
      (byte) ((data >> 8) & 0xff),
      (byte) ((data >> 0) & 0xff),
    };
  }

  @Override
  public boolean hasNext() {
    while (delegated.hasNext()) {
      nextBatch = delegated.next();
      if (nextBatch.numRows() > 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public ColumnarBatch nextColumnarBatch() {
    return nextBatch;
  }

  @Override
  public byte[] next() {
    ColumnarBatch nextBatch = nextColumnarBatch();
    if (nextBatch.numRows() > 0) {
      CHColumnVector col = (CHColumnVector) nextBatch.column(0);
      return longtoBytes(col.getBlockAddress());
    } else {
      throw new IllegalStateException();
    }
  }
}
