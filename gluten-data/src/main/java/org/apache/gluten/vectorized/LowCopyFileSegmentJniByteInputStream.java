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
import org.apache.spark.network.util.LimitedInputStream;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * This implementation is targeted to optimize against Spark's {@link
 * org.apache.spark.network.buffer.FileSegmentManagedBuffer} to make sure shuffle data is shared
 * over JNI without unnecessary copy.
 */
public class LowCopyFileSegmentJniByteInputStream implements JniByteInputStream {
  private static final Field FIELD_FilterInputStream_in;
  private static final Field FIELD_LimitedInputStream_left;

  static {
    try {
      FIELD_FilterInputStream_in = FilterInputStream.class.getDeclaredField("in");
      FIELD_FilterInputStream_in.setAccessible(true);
      FIELD_LimitedInputStream_left = LimitedInputStream.class.getDeclaredField("left");
      FIELD_LimitedInputStream_left.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new GlutenException(e);
    }
  }

  private final InputStream in;
  private final FileChannel channel;

  private long bytesRead = 0L;
  private long left;

  public LowCopyFileSegmentJniByteInputStream(InputStream in) {
    this.in = in; // to prevent underlying netty buffer from being collected by GC
    final InputStream unwrapped = JniByteInputStreams.unwrapSparkInputStream(in);
    final LimitedInputStream lin = (LimitedInputStream) unwrapped;
    try {
      left = ((long) FIELD_LimitedInputStream_left.get(lin));
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
    final FileInputStream fin;
    try {
      fin = (FileInputStream) FIELD_FilterInputStream_in.get(lin);
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
    channel = fin.getChannel();
  }

  public static boolean isSupported(InputStream in) {
    if (!(in instanceof LimitedInputStream)) {
      return false;
    }
    final LimitedInputStream lin = (LimitedInputStream) in;
    final InputStream wrapped;
    try {
      wrapped = (InputStream) FIELD_FilterInputStream_in.get(lin);
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
    if (!(wrapped instanceof FileInputStream)) {
      return false;
    }
    return true;
  }

  @Override
  public long read(long destAddress, long maxSize) {
    long bytesToRead = Math.min(left, maxSize);
    int bytesToRead32 = Math.toIntExact(bytesToRead);
    if (bytesToRead32 == 0) {
      return 0;
    }
    ByteBuffer direct = PlatformDependent.directBuffer(destAddress, bytesToRead32);
    try {
      int bytes = channel.read(direct);
      if (bytes == -1) {
        return 0;
      }
      bytesRead += bytes;
      left -= bytes;
      return bytes;
    } catch (IOException e) {
      throw new GlutenException(e);
    }
  }

  @Override
  public long tell() {
    return bytesRead;
  }

  @Override
  public void close() {
    try {
      channel.close();
      in.close();
    } catch (IOException e) {
      throw new GlutenException(e);
    }
  }
}
