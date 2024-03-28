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
package org.apache.spark.storage;

import org.apache.gluten.exception.GlutenException;
import org.apache.gluten.vectorized.LowCopyFileSegmentShuffleInputStream;
import org.apache.gluten.vectorized.LowCopyNettyShuffleInputStream;
import org.apache.gluten.vectorized.OnHeapCopyShuffleInputStream;
import org.apache.gluten.vectorized.ShuffleInputStream;

import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.ning.compress.lzf.LZFInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import org.apache.spark.network.util.LimitedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import java.io.*;
import java.lang.reflect.Field;
import java.util.zip.CheckedInputStream;

public final class CHShuffleReadStreamFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CHShuffleReadStreamFactory.class);

  public static final Field FIELD_FilterInputStream_in;
  public static final Field FIELD_ByteBufInputStream_buffer;
  public static final Field FIELD_LimitedInputStream_left;

  public static final Field FIELD_SnappyInputStream_in;
  public static final Field FIELD_LZ4BlockInputStream_in;
  public static final Field FIELD_BufferedInputStream_in;
  public static final Field FIELD_ZstdInputStreamNoFinalizer_in;
  public static final Field FIELD_LZFInputStream_in;

  static {
    try {
      FIELD_FilterInputStream_in = FilterInputStream.class.getDeclaredField("in");
      FIELD_FilterInputStream_in.setAccessible(true);
      FIELD_ByteBufInputStream_buffer = ByteBufInputStream.class.getDeclaredField("buffer");
      FIELD_ByteBufInputStream_buffer.setAccessible(true);
      FIELD_LimitedInputStream_left = LimitedInputStream.class.getDeclaredField("left");
      FIELD_LimitedInputStream_left.setAccessible(true);

      FIELD_SnappyInputStream_in = SnappyInputStream.class.getDeclaredField("in");
      FIELD_SnappyInputStream_in.setAccessible(true);
      FIELD_LZ4BlockInputStream_in =
          LZ4BlockInputStream.class.getSuperclass().getDeclaredField("in");
      FIELD_LZ4BlockInputStream_in.setAccessible(true);
      FIELD_BufferedInputStream_in =
          BufferedInputStream.class.getSuperclass().getDeclaredField("in");
      FIELD_BufferedInputStream_in.setAccessible(true);
      FIELD_ZstdInputStreamNoFinalizer_in =
          ZstdInputStreamNoFinalizer.class.getSuperclass().getDeclaredField("in");
      FIELD_ZstdInputStreamNoFinalizer_in.setAccessible(true);
      FIELD_LZFInputStream_in = LZFInputStream.class.getDeclaredField("_inputStream");
      FIELD_LZFInputStream_in.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new GlutenException(e);
    }
  }

  private CHShuffleReadStreamFactory() {}

  public static ShuffleInputStream create(byte[] allBatches, boolean compressed) {
    return new OnHeapCopyShuffleInputStream(new ByteArrayInputStream(allBatches), compressed);
  }

  public static ShuffleInputStream create(
      InputStream in, boolean forceCompress, boolean isCustomizedShuffleCodec) {
    final InputStream unwrapped = unwrapInputStream(in, forceCompress, isCustomizedShuffleCodec);
    if (unwrapped != null) {
      return createCompressedShuffleInputStream(in, unwrapped);
    }
    return new OnHeapCopyShuffleInputStream(in, false);
  }

  private static InputStream unwrapInputStream(
      InputStream in, boolean forceCompress, boolean isCustomizedShuffleCodec) {
    if (forceCompress) {
      return unwrapSparkInputStream(in);
    } else if (isCustomizedShuffleCodec) {
      return unwrapSparkWithCompressedInputStream(in);
    }
    return null;
  }

  private static ShuffleInputStream createCompressedShuffleInputStream(
      InputStream in, InputStream unwrapped) {
    LimitedInputStream limitedInputStream = isReadFromFileSegment(unwrapped);
    if (limitedInputStream != null) {
      return new LowCopyFileSegmentShuffleInputStream(in, limitedInputStream, true);
    }
    // Unwrap ByteBufInputStream and get the ByteBuf
    ByteBuf byteBuf = isReadFromNettySupported(unwrapped);
    if (byteBuf != null) {
      return new LowCopyNettyShuffleInputStream(in, byteBuf, true);
    }
    // Unwrap failed, use on heap copy method
    return new OnHeapCopyShuffleInputStream(in, true);
  }

  /** Unwrap BufferReleasingInputStream and CheckedInputStream */
  public static InputStream unwrapSparkInputStream(InputStream in) {
    InputStream unwrapped = in;
    if (unwrapped instanceof BufferReleasingInputStream) {
      final BufferReleasingInputStream brin = (BufferReleasingInputStream) unwrapped;
      unwrapped = brin.delegate();
    }
    if (unwrapped instanceof CheckedInputStream) {
      final CheckedInputStream cin = (CheckedInputStream) unwrapped;
      try {
        unwrapped = ((InputStream) FIELD_FilterInputStream_in.get(cin));
      } catch (IllegalAccessException e) {
        LOG.error("Can not get the field 'in' from CheckedInputStream: ", e);
        return in;
      }
    }
    return unwrapped;
  }

  /** Unwrap BufferReleasingInputStream, CheckedInputStream and CompressionInputStream */
  public static InputStream unwrapSparkWithCompressedInputStream(InputStream in) {
    InputStream unwrapped = in;
    if (unwrapped instanceof BufferReleasingInputStream) {
      final BufferReleasingInputStream brin = (BufferReleasingInputStream) unwrapped;
      unwrapped = brin.delegate();
    }
    InputStream unwrappedCompression = unwrapCompressionInputStream(unwrapped);
    if (unwrappedCompression != null) {
      if (unwrappedCompression instanceof CheckedInputStream) {
        final CheckedInputStream cin = (CheckedInputStream) unwrappedCompression;
        try {
          return ((InputStream) FIELD_FilterInputStream_in.get(cin));
        } catch (IllegalAccessException e) {
          LOG.error("Can not get the field 'in' from CheckedInputStream: ", e);
          return null;
        }
      }
    } else {
      return null;
    }
    return unwrapped;
  }

  /** Check whether support reading from file segment (local read) */
  public static LimitedInputStream isReadFromFileSegment(InputStream in) {
    if (!(in instanceof LimitedInputStream)) {
      return null;
    }
    LimitedInputStream lin = (LimitedInputStream) in;
    InputStream wrapped;
    try {
      wrapped = (InputStream) FIELD_FilterInputStream_in.get(lin);
    } catch (IllegalAccessException e) {
      LOG.error("Can not get the fields from LimitedInputStream: ", e);
      return null;
    }
    if (!(wrapped instanceof FileInputStream)) {
      return null;
    }
    return lin;
  }

  /** Check whether support reading from netty (remote read) */
  public static ByteBuf isReadFromNettySupported(InputStream in) {
    if (!(in instanceof ByteBufInputStream)) {
      return null;
    }
    ByteBufInputStream bbin = (ByteBufInputStream) in;
    try {
      ByteBuf byteBuf = (ByteBuf) FIELD_ByteBufInputStream_buffer.get(bbin);
      if (!byteBuf.isDirect()) {
        return null;
      }
      return byteBuf;
    } catch (IllegalAccessException e) {
      LOG.error("Can not get the field 'buffer' from ByteBufInputStream: ", e);
      return null;
    }
  }

  /** Unwrap Spark compression input stream. */
  public static InputStream unwrapCompressionInputStream(InputStream is) {
    InputStream unwrapped = is;
    try {
      if (unwrapped instanceof BufferedInputStream) {
        final InputStream cis = (InputStream) FIELD_BufferedInputStream_in.get(unwrapped);
        if (cis instanceof ZstdInputStreamNoFinalizer) {
          unwrapped = (InputStream) FIELD_ZstdInputStreamNoFinalizer_in.get(cis);
        }
      } else if (unwrapped instanceof SnappyInputStream) {
        // unsupported
        return null;
      } else if (unwrapped instanceof LZ4BlockInputStream) {
        unwrapped = (InputStream) FIELD_LZ4BlockInputStream_in.get(unwrapped);
      } else if (unwrapped instanceof LZFInputStream) {
        unwrapped = (InputStream) FIELD_LZFInputStream_in.get(unwrapped);
      }
    } catch (IllegalAccessException e) {
      LOG.error("Can not get the field 'in' from compression input stream: ", e);
      return null;
    }
    return unwrapped;
  }
}
