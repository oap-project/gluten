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

package io.glutenproject.vectorized;

import java.io.IOException;

public class ShuffleDecompressionJniWrapper {

  public ShuffleDecompressionJniWrapper() throws IOException {
    JniWorkspace.getDefault().libLoader().loadEssentials();
  }

  /**
   * Make for multiple decompression with the same schema
   *
   * @param cSchema {@link org.apache.arrow.c.ArrowSchema} address
   * @return native schema holder id
   * @throws RuntimeException
   */
  public native long make(long cSchema) throws RuntimeException;

  public native boolean decompress(
      long schemaHolderId,
      String compressionCodec,
      int numRows,
      long[] bufAddrs,
      long[] bufSizes,
      long[] bufMask,
      long cSchema,
      long cArray)
      throws RuntimeException;

  /**
   * Release resources associated with designated schema holder instance.
   *
   * @param schemaHolderId of the schema holder instance.
   */
  public native void close(long schemaHolderId);
}
