package io.glutenproject.vectorized;

import org.apache.arrow.vector.compression.CompressionUtil;

import javax.ws.rs.NotSupportedException;

public enum CompressType {

  // now type value is same with Arrow
  NO_COMPRESSION((byte)-1, "uncompressed"),
  LZ4_FRAME((byte)0, "lz4"),

  ZSTD((byte)1, "zstd"); // not use now

  private final byte type;
  // Arrow compress type in C++
  private String alias;

  CompressType(byte type, String alias) {
    this.type = type;
    this.alias = alias;
  }

  public byte getType() {
    return this.type;
  }

  public String getAlias() {
    return alias;
  }

  public static CompressType fromCompressionType(byte type) {
    CompressType[] var1 = values();
    int var2 = var1.length;

    for(int var3 = 0; var3 < var2; ++var3) {
      CompressType codecType = var1[var3];
      if (codecType.type == type) {
        return codecType;
      }
    }

    return NO_COMPRESSION;
  }

  public CompressionUtil.CodecType toArrowCompressType(CompressType type) {
    switch (type) {
      case NO_COMPRESSION:
        return CompressionUtil.CodecType.NO_COMPRESSION;
      case LZ4_FRAME:
        return CompressionUtil.CodecType.LZ4_FRAME;
      default:
        throw new NotSupportedException("Not supported this compress type" + type.name());
    }
  }
}
