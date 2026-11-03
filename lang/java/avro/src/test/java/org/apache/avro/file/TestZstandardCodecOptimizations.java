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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.SystemLimitException;
import org.apache.avro.util.NonCopyingByteArrayOutputStream;
import org.junit.Test;

/**
 * Tests for the ported Zstandard codec optimizations: direct-buffer reads in
 * decompression, output presizing, and — critically — that a shared codec
 * instance stays byte-exact under concurrency (the fork's shared output
 * buffer, later fixed by its b21bbf94e, must not come back).
 */
public class TestZstandardCodecOptimizations {

  private static byte[] bytes(ByteBuffer buf) {
    ByteBuffer dup = buf.duplicate();
    byte[] out = new byte[dup.remaining()];
    dup.get(out);
    return out;
  }

  private static byte[] repetitive(int size) {
    byte[] data = new byte[size];
    byte[] pattern = "the quick brown fox jumps over the lazy dog ".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    for (int i = 0; i < size; i++) {
      data[i] = pattern[i % pattern.length];
    }
    return data;
  }

  private static byte[] random(int size, long seed) {
    byte[] data = new byte[size];
    new Random(seed).nextBytes(data);
    return data;
  }

  @Test
  public void testReadFromDrainsStreamDirectly() throws IOException {
    byte[] data = random(100_000, 42L);
    NonCopyingByteArrayOutputStream out = new NonCopyingByteArrayOutputStream(16);
    long read = out.readFrom(new ByteArrayInputStream(data));
    assertEquals(data.length, read);
    assertArrayEquals(data, bytes(out.asByteBuffer()));
  }

  @Test
  public void testReadFromEnforcesLimitOnlyOnOverflow() throws IOException {
    byte[] exact = random(1024, 7L);
    NonCopyingByteArrayOutputStream out = NonCopyingByteArrayOutputStream.capacityLimitedOutputStream(16, 1024);
    // a stream ending exactly at the limit must not throw
    assertEquals(1024, out.readFrom(new ByteArrayInputStream(exact)));
    assertArrayEquals(exact, bytes(out.asByteBuffer()));

    NonCopyingByteArrayOutputStream over = NonCopyingByteArrayOutputStream.capacityLimitedOutputStream(16, 1024);
    try {
      over.readFrom(new ByteArrayInputStream(random(2048, 8L)));
      fail("expected SystemLimitException when data exceeds the limit");
    } catch (SystemLimitException expected) {
      // expected
    }
  }

  @Test
  public void testRoundTrips() throws IOException {
    ZstandardCodec codec = new ZstandardCodec(3, false, false);
    byte[][] payloads = { new byte[0], new byte[] { 1 }, "hello zstd".getBytes(java.nio.charset.StandardCharsets.UTF_8),
        random(64 * 1024, 1L), repetitive(1024 * 1024), random(1024 * 1024, 2L), repetitive(12345) };
    for (byte[] payload : payloads) {
      ByteBuffer compressed = codec.compress(ByteBuffer.wrap(payload));
      assertArrayEquals("round trip failed for payload of " + payload.length + " bytes", payload,
          bytes(codec.decompress(compressed)));
    }
  }

  @Test
  public void testConcurrentSharedCodecInstanceByteExact() throws Exception {
    final ZstandardCodec codec = new ZstandardCodec(3, false, false);
    final int threads = 8;
    final int iterations = 100;
    final byte[][] payloads = new byte[threads][];
    for (int i = 0; i < threads; i++) {
      payloads[i] = (i % 2 == 0) ? repetitive(50_000 + i * 7_919) : random(50_000 + i * 7_919, 100L + i);
    }
    // one shared compressed buffer, decompressed concurrently by all threads
    final ByteBuffer sharedCompressed = codec.compress(ByteBuffer.wrap(payloads[0]));

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Void>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int thread = t;
        futures.add(pool.submit((Callable<Void>) () -> {
          for (int i = 0; i < iterations; i++) {
            ByteBuffer compressed = codec.compress(ByteBuffer.wrap(payloads[thread]));
            assertArrayEquals("corrupted round trip, thread " + thread + " iteration " + i, payloads[thread],
                bytes(codec.decompress(compressed)));
            assertArrayEquals("corrupted shared decompress, thread " + thread + " iteration " + i, payloads[0],
                bytes(codec.decompress(sharedCompressed)));
          }
          return null;
        }));
      }
      for (Future<Void> f : futures) {
        f.get();
      }
    } finally {
      pool.shutdownNow();
    }
    assertTrue(true);
  }
}
