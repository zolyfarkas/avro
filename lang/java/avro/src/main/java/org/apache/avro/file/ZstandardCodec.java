/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.util.NonCopyingByteArrayOutputStream;

import com.github.luben.zstd.Zstd;

public class ZstandardCodec extends Codec {
  public final static int DEFAULT_COMPRESSION = 3;
  public final static boolean DEFAULT_USE_BUFFERPOOL = false;
  private static final int DEFAULT_BUFFER_SIZE = 8192;

  static class Option extends CodecFactory {
    private final int compressionLevel;
    private final boolean useChecksum;
    private final boolean useBufferPool;

    Option(int compressionLevel, boolean useChecksum, boolean useBufferPool) {
      this.compressionLevel = compressionLevel;
      this.useChecksum = useChecksum;
      this.useBufferPool = useBufferPool;
    }

    @Override
    protected Codec createInstance() {
      return new ZstandardCodec(compressionLevel, useChecksum, useBufferPool);
    }
  }

  private final int compressionLevel;
  private final boolean useChecksum;
  private final boolean useBufferPool;

  /**
   * Create a ZstandardCodec instance with the given compressionLevel, checksum,
   * and bufferPool option
   **/
  public ZstandardCodec(int compressionLevel, boolean useChecksum, boolean useBufferPool) {
    this.compressionLevel = compressionLevel;
    this.useChecksum = useChecksum;
    this.useBufferPool = useBufferPool;
  }

  @Override
  public String getName() {
    return DataFileConstants.ZSTANDARD_CODEC;
  }

  @Override
  public ByteBuffer compress(ByteBuffer data) throws IOException {
    // Presize the output to the zstd frame bound so the buffer never grows
    // (and copies) while the frame is written. The buffer is per-call: the
    // fork's later shared-instance buffer (4d78e3d88) caused the aliasing
    // its b21bbf94e had to fix, and is deliberately not reproduced.
    int bound = (int) Math.min(Zstd.compressBound(data.remaining()), Integer.MAX_VALUE);
    NonCopyingByteArrayOutputStream baos = new NonCopyingByteArrayOutputStream(Math.max(bound, 1));
    try (OutputStream outputStream = ZstandardLoader.output(baos, compressionLevel, useChecksum, useBufferPool)) {
      outputStream.write(data.array(), computeOffset(data), data.remaining());
    }
    return baos.asByteBuffer();
  }

  @Override
  public ByteBuffer decompress(ByteBuffer compressedData) throws IOException {
    NonCopyingByteArrayOutputStream baos = NonCopyingByteArrayOutputStream
        .capacityLimitedOutputStream(initialDecompressCapacity(compressedData));
    InputStream bytesIn = new ByteArrayInputStream(compressedData.array(), computeOffset(compressedData),
        compressedData.remaining());
    try (InputStream ios = ZstandardLoader.input(bytesIn, useBufferPool)) {
      baos.readFrom(ios);
    }
    return baos.asByteBuffer();
  }

  /**
   * Initial capacity for the decompression buffer: the frame's content size
   * when the frame header carries one, so the common case needs no growth
   * copies at all. When the content size is unknown, fall back to twice the
   * compressed size (the fork's final-form heuristic, from its b21bbf94e),
   * replacing the fixed 8 KiB start that forced repeated growth copies on
   * larger payloads. The capacity-limited factory caps the result at the
   * decompression limit.
   */
  private static int initialDecompressCapacity(ByteBuffer compressedData) {
    long contentSize = Zstd.getFrameContentSize(compressedData.array(), computeOffset(compressedData),
        compressedData.remaining());
    if (contentSize > 0) {
      return (int) Math.min(contentSize, Integer.MAX_VALUE);
    }
    long guess = 2L * compressedData.remaining();
    return (int) Math.max(Math.min(guess, Integer.MAX_VALUE), DEFAULT_BUFFER_SIZE);
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj) || (obj != null && obj.getClass() == this.getClass());
  }

  @Override
  public String toString() {
    return getName() + "[" + compressionLevel + "]";
  }
}
