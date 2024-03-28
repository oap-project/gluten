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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.concurrent.atomic.AtomicLong;

public class IndicatorVector extends ColumnVector {
  private final Runtime runtime;
  private final long handle;
  private final AtomicLong refCnt = new AtomicLong(1L);

  protected IndicatorVector(Runtime runtime, long handle) {
    super(DataTypes.NullType);
    this.runtime = runtime;
    this.handle = handle;
  }

  public Runtime runtime() {
    return runtime;
  }

  public String getType() {
    return ColumnarBatchJniWrapper.forRuntime(runtime).getType(handle);
  }

  public long getNumColumns() {
    return ColumnarBatchJniWrapper.forRuntime(runtime).numColumns(handle);
  }

  public long getNumRows() {
    return ColumnarBatchJniWrapper.forRuntime(runtime).numRows(handle);
  }

  public long refCnt() {
    return refCnt.get();
  }

  public void retain() {
    refCnt.getAndIncrement();
  }

  @Override
  public void close() {
    if (refCnt.get() == 0) {
      // TODO use stronger restriction (IllegalStateException probably)
      return;
    }
    if (refCnt.decrementAndGet() == 0) {
      ColumnarBatchJniWrapper.forRuntime(runtime).close(handle);
    }
  }

  public boolean isClosed() {
    return refCnt.get() == 0;
  }

  @Override
  public boolean hasNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int numNulls() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException();
  }

  public long handle() {
    return handle;
  }
}
