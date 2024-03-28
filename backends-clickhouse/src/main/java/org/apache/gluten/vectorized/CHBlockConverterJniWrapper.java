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

import org.apache.gluten.execution.SparkRowIterator;
import org.apache.gluten.row.SparkRowInfo;

public class CHBlockConverterJniWrapper {

  private CHBlockConverterJniWrapper() {
    // utility class
  }

  // for ch columnar -> spark row
  public static native SparkRowInfo convertColumnarToRow(long blockAddress, int[] masks);

  // for ch columnar -> spark row
  public static native void freeMemory(long address, long size);

  // for spark row -> ch columnar
  public static native long convertSparkRowsToCHColumn(
      SparkRowIterator iter, String[] names, byte[][] types);

  // for spark row -> ch columnar
  public static native void freeBlock(long blockAddress);
}
