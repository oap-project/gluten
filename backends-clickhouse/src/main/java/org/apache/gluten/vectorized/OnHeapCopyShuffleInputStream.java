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

import org.apache.gluten.exception.GlutenException;

import io.netty.util.internal.PlatformDependent;

import java.io.InputStream;

public class OnHeapCopyShuffleInputStream implements ShuffleInputStream {

  private InputStream in;
  private final boolean isCompressed;
  private long bytesRead = 0L;

  private byte[] buffer;

  public OnHeapCopyShuffleInputStream(InputStream in, boolean isCompressed) {
    this.in = in;
    this.isCompressed = isCompressed;
  }

  @Override
  public long read(long destAddress, long maxReadSize) {
    return GlutenException.wrap(
        () -> {
          int maxReadSize32 = Math.toIntExact(maxReadSize);
          if (buffer == null || maxReadSize32 > buffer.length) {
            this.buffer = new byte[maxReadSize32];
          }
          // The code conducts copy as long as 'in' wraps off-heap data,
          // which is about to be moved to heap
          int read = in.read(buffer, 0, maxReadSize32);
          if (read == -1 || read == 0) {
            return 0;
          }
          // The code conducts copy, from heap to off-heap
          // memCopyFromHeap(buffer, destAddress, read);
          PlatformDependent.copyMemory(buffer, 0, destAddress, read);
          bytesRead += read;
          return read;
        });
  }

  @Override
  public long pos() {
    return bytesRead;
  }

  @Override
  public boolean isCompressed() {
    return this.isCompressed;
  }

  @Override
  public void close() {
    GlutenException.wrap(
        () -> {
          in.close();
          in = null;
          return null;
        });
  }
}
