/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.cdc.craft;

import io.tidb.bigdata.cdc.Misc;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

public class Codec {

  private final byte[] buffer;
  private int bufferPos;
  private int bufferLimit;

  public Codec(byte[] buffer) {
    this(buffer, 0, buffer.length);
  }

  public Codec(byte[] buffer, int offset, int length) {
    this.buffer = buffer;
    this.bufferPos = offset;
    this.bufferLimit = bufferPos + length;
  }

  /*
   * copied from com.google.protobuf.CodedInputStream (protobuf)
   */
  private static long decodeZigZag64(final long i64) {
    return (i64 >>> 1) ^ -(i64 & 1);
  }

  public Codec sliceTailing(int length) {
    return new Codec(buffer, bufferLimit - length, length);
  }

  public Codec truncateTailing(int length) {
    Codec tail = sliceTailing(length);
    this.bufferLimit -= length;
    return tail;
  }

  public Codec sliceHeading(int length) {
    return new Codec(buffer, bufferPos, length);
  }

  public Codec truncateHeading(int length) {
    Codec head = sliceHeading(length);
    this.bufferPos += length;
    return head;
  }

  public Codec clone() {
    return new Codec(buffer, bufferPos, bufferLimit - bufferPos);
  }

  public int available() {
    return bufferLimit - bufferPos;
  }

  // Primitive type decoders
  public int decodeUvarintLength() {
    return checkedCast(decodeUvarint());
  }

  public long decodeVarint() {
    return decodeZigZag64(readRawVarint64());
  }

  private int checkedCast(long from) {
    if (from > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Number " + from + " exceeds max limit of " + Integer.MAX_VALUE);
    }
    return (int) from;
  }

  public int decodeUvarintReversedLength() {
    return checkedCast(decodeUvarintReversed());
  }

  public long decodeUvarintReversed() {
    // Implementation notes:
    //
    // Optimized for one-byte values, expected to be common.
    // The particular code below was selected from various candidates
    // empirically, by winning VarintBenchmark.
    //
    // Sign extension of (signed) Java bytes is usually a nuisance, but
    // we exploit it here to more easily obtain the sign of bytes read.
    // Instead of cleaning up the sign extension bits by masking eagerly,
    // we delay until we find the final (positive) byte, when we clear all
    // accumulated bits with one xor.  We depend on javac to constant fold.
    fastpath:
    {
      int tempPos = bufferLimit - 1;

      if (bufferPos == tempPos) {
        break fastpath;
      }

      final byte[] buffer = this.buffer;
      long x;
      int y;
      if ((y = buffer[tempPos--]) >= 0) {
        bufferLimit = tempPos + 1;
        return y;
      } else if (tempPos - bufferPos < 9) {
        break fastpath;
      } else if ((y ^= (buffer[tempPos--] << 7)) < 0) {
        x = y ^ (~0 << 7);
      } else if ((y ^= (buffer[tempPos--] << 14)) >= 0) {
        x = y ^ ((~0 << 7) ^ (~0 << 14));
      } else if ((y ^= (buffer[tempPos--] << 21)) < 0) {
        x = y ^ ((~0 << 7) ^ (~0 << 14) ^ (~0 << 21));
      } else if ((x = y ^ ((long) buffer[tempPos--] << 28)) >= 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
      } else if ((x ^= ((long) buffer[tempPos--] << 35)) < 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35);
      } else if ((x ^= ((long) buffer[tempPos--] << 42)) >= 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42);
      } else if ((x ^= ((long) buffer[tempPos--] << 49)) < 0L) {
        x ^=
            (~0L << 7)
                ^ (~0L << 14)
                ^ (~0L << 21)
                ^ (~0L << 28)
                ^ (~0L << 35)
                ^ (~0L << 42)
                ^ (~0L << 49);
      } else {
        x ^= ((long) buffer[tempPos--] << 56);
        x ^=
            (~0L << 7)
                ^ (~0L << 14)
                ^ (~0L << 21)
                ^ (~0L << 28)
                ^ (~0L << 35)
                ^ (~0L << 42)
                ^ (~0L << 49)
                ^ (~0L << 56);
        if (x < 0L) {
          if (buffer[tempPos--] < 0L) {
            break fastpath; // Will throw malformedVarint()
          }
        }
      }
      bufferLimit = tempPos + 1;
      return x;
    }
    return decodeUvarint64ReversedSlowPath();
  }

  private long decodeUvarint64ReversedSlowPath() {
    long result = 0;
    int tempPos = bufferLimit - 1;
    for (int shift = 0; shift < 64; shift += 7) {
      if (tempPos <= bufferPos) {
        throw new IllegalStateException(
            "Buffer underflow: expecting 1 byte when there is nothing to read");
      }
      final byte b = buffer[tempPos--];
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        bufferLimit = tempPos + 1;
        return result;
      }
    }
    throw new IllegalStateException("Malformated varint");
  }

  public long decodeUvarint() {
    return readRawVarint64();
  }

  public short decodeUint8() {
    return readRawByte();
  }

  public double decodeFloat64() {
    return Double.longBitsToDouble(readRawLittleEndian64());
  }

  public byte[] decodeBytes() {
    final int length = decodeUvarintLength();
    checkAvailableBytes(length);
    final byte[] bytes = new byte[length];
    System.arraycopy(buffer, bufferPos, bytes, 0, length);
    bufferPos += length;
    return bytes;
  }

  public String decodeString() {
    return new String(decodeBytes(), StandardCharsets.UTF_8);
  }

  // Chunk decoders
  public String[] decodeStringChunk(int size) {
    final byte[][] bytesChunk = decodeBytesChunk(size);
    final String[] strings = new String[size];
    for (int idx = 0; idx < size; idx++) {
      strings[idx] = new String(bytesChunk[idx], StandardCharsets.UTF_8);
    }
    return strings;
  }

  public String[] decodeNullableStringChunk(int size) {
    final byte[][] bytesChunk = decodeNullableBytesChunk(size);
    final String[] strings = new String[size];
    for (int idx = 0; idx < size; idx++) {
      if (bytesChunk[idx] != null) {
        strings[idx] = new String(bytesChunk[idx], StandardCharsets.UTF_8);
      }
    }
    return strings;
  }

  private byte[][] doDecodeBytesChunk(int size, Supplier<Integer> lengthDecoder) {
    int[] lengthArray = new int[size];
    for (int idx = 0; idx < size; ++idx) {
      lengthArray[idx] = lengthDecoder.get();
    }

    byte[][] bytes = new byte[size][];
    for (int idx = 0; idx < size; ++idx) {
      int length = lengthArray[idx];
      if (length != -1) {
        bytes[idx] = new byte[length];
        System.arraycopy(buffer, bufferPos, bytes[idx], 0, length);
        bufferPos += length;
      }
    }

    return bytes;
  }

  public byte[][] decodeBytesChunk(int size) {
    return doDecodeBytesChunk(size, () -> {
      return Misc.uncheckedRun(this::decodeUvarint).intValue();
    });
  }

  public byte[][] decodeNullableBytesChunk(int size) {
    return doDecodeBytesChunk(size, () -> {
      return Misc.uncheckedRun(this::decodeVarint).intValue();
    });
  }

  public long[] decodeVarintChunk(int size) {
    long[] result = new long[size];
    for (int idx = 0; idx < size; ++idx) {
      result[idx] = decodeVarint();
    }
    return result;
  }

  public long[] decodeUvarintChunk(int size) {
    long[] result = new long[size];
    for (int idx = 0; idx < size; ++idx) {
      result[idx] = decodeUvarint();
    }
    return result;
  }

  public long[] decodeDeltaVarintChunk(int size) {
    long[] result = new long[size];
    result[0] = decodeVarint();
    for (int idx = 1; idx < size; ++idx) {
      result[idx] = decodeVarint();
      result[idx] = result[idx - 1] + result[idx];
    }
    return result;
  }

  public long[] decodeDeltaUvarintChunk(int size) {
    long[] result = new long[size];
    result[0] = decodeUvarint();
    for (int idx = 1; idx < size; ++idx) {
      result[idx] = decodeUvarint();
      result[idx] = result[idx - 1] + result[idx];
    }
    return result;
  }

  private int checkAvailableBytes(final int expecting) {
    final int available = bufferLimit - bufferPos;
    if (available < expecting) {
      throw new IllegalStateException("Buffer underflow: expecting " + expecting
          + " byte(s) when only " + available + " byte(s) are available");
    }
    return bufferPos;
  }

  private byte readRawByte() {
    final int expecting = 1;
    final int pos = checkAvailableBytes(expecting);
    bufferPos += expecting;
    return buffer[pos];
  }

  /*
   * copied from com.google.protobuf.CodedInputStream (protobuf)
   */
  private long readRawLittleEndian64() {
    final int expecting = 8;
    final int pos = checkAvailableBytes(expecting);
    bufferPos += expecting;
    return (((buffer[pos] & 0xffL))
        | ((buffer[pos + 1] & 0xffL) << 8)
        | ((buffer[pos + 2] & 0xffL) << 16)
        | ((buffer[pos + 3] & 0xffL) << 24)
        | ((buffer[pos + 4] & 0xffL) << 32)
        | ((buffer[pos + 5] & 0xffL) << 40)
        | ((buffer[pos + 6] & 0xffL) << 48)
        | ((buffer[pos + 7] & 0xffL) << 56));
  }

  /*
   * copied from com.google.protobuf.CodedInputStream (protobuf)
   */
  public long readRawVarint64() {
    // Implementation notes:
    //
    // Optimized for one-byte values, expected to be common.
    // The particular code below was selected from various candidates
    // empirically, by winning VarintBenchmark.
    //
    // Sign extension of (signed) Java bytes is usually a nuisance, but
    // we exploit it here to more easily obtain the sign of bytes read.
    // Instead of cleaning up the sign extension bits by masking eagerly,
    // we delay until we find the final (positive) byte, when we clear all
    // accumulated bits with one xor.  We depend on javac to constant fold.
    fastpath:
    {
      int tempPos = bufferPos;

      if (bufferLimit == tempPos) {
        break fastpath;
      }

      final byte[] buffer = this.buffer;
      long x;
      int y;
      if ((y = buffer[tempPos++]) >= 0) {
        bufferPos = tempPos;
        return y;
      } else if (bufferLimit - tempPos < 9) {
        break fastpath;
      } else if ((y ^= (buffer[tempPos++] << 7)) < 0) {
        x = y ^ (~0 << 7);
      } else if ((y ^= (buffer[tempPos++] << 14)) >= 0) {
        x = y ^ ((~0 << 7) ^ (~0 << 14));
      } else if ((y ^= (buffer[tempPos++] << 21)) < 0) {
        x = y ^ ((~0 << 7) ^ (~0 << 14) ^ (~0 << 21));
      } else if ((x = y ^ ((long) buffer[tempPos++] << 28)) >= 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
      } else if ((x ^= ((long) buffer[tempPos++] << 35)) < 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35);
      } else if ((x ^= ((long) buffer[tempPos++] << 42)) >= 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42);
      } else if ((x ^= ((long) buffer[tempPos++] << 49)) < 0L) {
        x ^=
            (~0L << 7)
                ^ (~0L << 14)
                ^ (~0L << 21)
                ^ (~0L << 28)
                ^ (~0L << 35)
                ^ (~0L << 42)
                ^ (~0L << 49);
      } else {
        x ^= ((long) buffer[tempPos++] << 56);
        x ^=
            (~0L << 7)
                ^ (~0L << 14)
                ^ (~0L << 21)
                ^ (~0L << 28)
                ^ (~0L << 35)
                ^ (~0L << 42)
                ^ (~0L << 49)
                ^ (~0L << 56);
        if (x < 0L) {
          if (buffer[tempPos++] < 0L) {
            break fastpath; // Will throw malformedVarint()
          }
        }
      }
      bufferPos = tempPos;
      return x;
    }
    return readRawVarint64SlowPath();
  }

  /*
   * copied from com.google.protobuf.CodedInputStream (protobuf)
   */
  private long readRawVarint64SlowPath() {
    long result = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      final byte b = readRawByte();
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new IllegalStateException("Malformated varint");
  }
}