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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * This implementation is targeted to optimize against Spark's {@link
 * org.apache.spark.network.buffer.NettyManagedBuffer} to make sure shuffle data is shared over JNI
 * without unnecessary copy.
 */
public class LowCopyNettyJniByteInputStream implements JniByteInputStream {

  private static final Field FIELD_ByteBufInputStream_buffer;

  static {
    try {
      FIELD_ByteBufInputStream_buffer = ByteBufInputStream.class.getDeclaredField("buffer");
      FIELD_ByteBufInputStream_buffer.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private final InputStream in;

  private ByteBuf byteBuf;

  private int readBytesCount = 0;

  public LowCopyNettyJniByteInputStream(InputStream in) {
    this.in = in; // to prevent underlying netty buffer from being collected by GC
    final InputStream unwrapped = JniByteInputStreams.unwrapSparkInputStream(in);
    try {
      this.byteBuf = (ByteBuf) FIELD_ByteBufInputStream_buffer.get(unwrapped);
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
  }

  @Override
  public long read(long destAddress, long maxSize) {
    long bytesToRead = Math.min(maxSize, this.byteBuf.readableBytes());
    int bytesToRead32 = Math.toIntExact(bytesToRead);
    if (bytesToRead32 == 0) {
      return 0;
    }
    ByteBuffer direct = PlatformDependent.directBuffer(destAddress, bytesToRead32);
    // read data directly from ByteBuf to native address
    this.byteBuf.readBytes(direct);
    readBytesCount += direct.position();
    return direct.position();
  }

  public static boolean isSupported(InputStream in) {
    if (!(in instanceof ByteBufInputStream)) {
      return false;
    }
    ByteBufInputStream bbin = (ByteBufInputStream) in;
    try {
      final ByteBuf byteBuf = (ByteBuf) FIELD_ByteBufInputStream_buffer.get(bbin);
      if (!byteBuf.isDirect()) {
        return false;
      }
      return true;
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
  }

  @Override
  public long tell() {
    return readBytesCount;
  }

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException e) {
      throw new GlutenException(e);
    }
  }
}
