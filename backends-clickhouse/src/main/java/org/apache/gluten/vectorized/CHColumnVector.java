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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class CHColumnVector extends ColumnVector {
  private final int columnPosition;
  private long blockAddress;

  public CHColumnVector(DataType type, long blockAddress, int columnPosition) {
    super(type);
    this.blockAddress = blockAddress;
    this.columnPosition = columnPosition;
  }

  public long getBlockAddress() {
    return blockAddress;
  }

  @Override
  public void close() {
    // blockAddress = 0;
  }

  private native boolean nativeHasNull(long blockAddress, int columnPosition);

  @Override
  public boolean hasNull() {
    return nativeHasNull(blockAddress, columnPosition);
  }

  private native int nativeNumNulls(long blockAddress, int columnPosition);

  @Override
  public int numNulls() {
    return nativeNumNulls(blockAddress, columnPosition);
  }

  private native boolean nativeIsNullAt(int rowId, long blockAddress, int columnPosition);

  @Override
  public boolean isNullAt(int rowId) {
    return nativeIsNullAt(rowId, blockAddress, columnPosition);
  }

  private native boolean nativeGetBoolean(int rowId, long blockAddress, int columnPosition);

  @Override
  public boolean getBoolean(int rowId) {
    return nativeGetBoolean(rowId, blockAddress, columnPosition);
  }

  private native byte nativeGetByte(int rowId, long blockAddress, int columnPosition);

  @Override
  public byte getByte(int rowId) {
    return nativeGetByte(rowId, blockAddress, columnPosition);
  }

  private native short nativeGetShort(int rowId, long blockAddress, int columnPosition);

  @Override
  public short getShort(int rowId) {
    return nativeGetShort(rowId, blockAddress, columnPosition);
  }

  private native int nativeGetInt(int rowId, long blockAddress, int columnPosition);

  @Override
  public int getInt(int rowId) {
    return nativeGetInt(rowId, blockAddress, columnPosition);
  }

  private native long nativeGetLong(int rowId, long blockAddress, int columnPosition);

  @Override
  public long getLong(int rowId) {
    return nativeGetLong(rowId, blockAddress, columnPosition);
  }

  private native float nativeGetFloat(int rowId, long blockAddress, int columnPosition);

  @Override
  public float getFloat(int rowId) {
    return nativeGetFloat(rowId, blockAddress, columnPosition);
  }

  private native double nativeGetDouble(int rowId, long blockAddress, int columnPosition);

  @Override
  public double getDouble(int rowId) {
    return nativeGetDouble(rowId, blockAddress, columnPosition);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return null;
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return null;
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return null;
  }

  private native String nativeGetString(int rowId, long blockAddress, int columnPosition);

  @Override
  public UTF8String getUTF8String(int rowId) {
    return UTF8String.fromString(nativeGetString(rowId, blockAddress, columnPosition));
  }

  @Override
  public byte[] getBinary(int rowId) {
    return new byte[0];
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return null;
  }
}
