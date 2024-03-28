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

import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import com.ning.compress.lzf.LZFOutputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;

public final class CHShuffleWriteStreamFactory {

  private CHShuffleWriteStreamFactory() {}

  private static final Logger LOG = LoggerFactory.getLogger(CHShuffleWriteStreamFactory.class);

  public static final Field FIELD_SnappyOutputStream_out;
  public static final Field FIELD_LZ4BlockOutputStream_out;
  public static final Field FIELD_BufferedOutputStream_out;
  public static final Field FIELD_ZstdOutputStreamNoFinalizer_out;
  public static final Field FIELD_LZFOutputStream_out;

  static {
    try {
      FIELD_SnappyOutputStream_out = SnappyOutputStream.class.getDeclaredField("out");
      FIELD_SnappyOutputStream_out.setAccessible(true);
      FIELD_LZ4BlockOutputStream_out =
          LZ4BlockOutputStream.class.getSuperclass().getDeclaredField("out");
      FIELD_LZ4BlockOutputStream_out.setAccessible(true);
      FIELD_BufferedOutputStream_out =
          BufferedOutputStream.class.getSuperclass().getDeclaredField("out");
      FIELD_BufferedOutputStream_out.setAccessible(true);
      FIELD_ZstdOutputStreamNoFinalizer_out =
          ZstdOutputStreamNoFinalizer.class.getSuperclass().getDeclaredField("out");
      FIELD_ZstdOutputStreamNoFinalizer_out.setAccessible(true);
      FIELD_LZFOutputStream_out = LZFOutputStream.class.getSuperclass().getDeclaredField("out");
      FIELD_LZFOutputStream_out.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new GlutenException(e);
    }
  }

  /** Unwrap Spark compression output stream. */
  public static OutputStream unwrapSparkCompressionOutputStream(
      OutputStream os, boolean isCustomizedShuffleCodec) {
    if (!isCustomizedShuffleCodec) return os;
    OutputStream out = null;
    try {
      if (os instanceof BufferedOutputStream) {
        final OutputStream cos = (OutputStream) FIELD_BufferedOutputStream_out.get(os);
        if (cos instanceof ZstdOutputStreamNoFinalizer) {
          out = (OutputStream) FIELD_ZstdOutputStreamNoFinalizer_out.get(cos);
        }
      } else if (os instanceof SnappyOutputStream) {
        // unsupported
        return null;
      } else if (os instanceof LZ4BlockOutputStream) {
        out = (OutputStream) FIELD_LZ4BlockOutputStream_out.get(os);
      } else if (os instanceof LZFOutputStream) {
        out = (OutputStream) FIELD_LZFOutputStream_out.get(os);
      } else if (os instanceof ByteArrayOutputStream) {
        out = os;
      }
    } catch (IllegalAccessException e) {
      LOG.error("Can not get the field 'out' from compression output stream: ", e);
      return null;
    }
    return out;
  }
}
