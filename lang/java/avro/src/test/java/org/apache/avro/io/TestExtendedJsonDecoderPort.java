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
package org.apache.avro.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 * Ported/adapted from the fork's ExtendedJsonDecoderTest. The fork test's
 * round trips through ExtendedJsonEncoder and its spf4j-generated
 * LogRecord/LogLevel classes are not ported (the encoder is out of scope and
 * the generated classes don't exist here); the decoding behaviors are pinned
 * with hand-written natural JSON instead. testDecoding is the fork's test
 * with its original resources and assertions.
 */
public class TestExtendedJsonDecoderPort {

  private static String resource(String name) throws IOException {
    try (InputStream in = TestExtendedJsonDecoderPort.class.getResourceAsStream("/" + name)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** The fork's testDecoding, with its original resources and assertions. */
  @Test
  public void testDecoding() throws IOException {
    String data = resource("testData.json");
    Schema writerSchema = new Schema.Parser().parse(resource("testDataWriterSchema.json"));
    Schema readerSchema = new Schema.Parser().parse(resource("testDataReaderSchema.json"));
    ByteArrayInputStream bis = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
    ExtendedJsonDecoder decoder = new ExtendedJsonDecoder(writerSchema, bis);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
    GenericRecord testData = reader.read(null, decoder);
    assertEquals(Long.valueOf(1L), ((Map<String, Long>) testData.get("someMap")).get("A"));
    assertEquals("caca", testData.get("someField").toString());
  }

  @Test
  public void testNaturalUnionDecoding() throws IOException {
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"u\",\"type\":[\"null\",\"long\"],\"default\":null}]}");
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    GenericRecord rec = reader.read(null,
        new ExtendedJsonDecoder(schema, new ByteArrayInputStream("{\"u\":5}".getBytes(StandardCharsets.UTF_8))));
    assertEquals(5L, rec.get("u"));
    GenericRecord recNull = reader.read(null,
        new ExtendedJsonDecoder(schema, new ByteArrayInputStream("{\"u\":null}".getBytes(StandardCharsets.UTF_8))));
    assertEquals(null, recNull.get("u"));
  }

  @Test
  public void testMissingFieldDefaultInjected() throws IOException {
    // same-schema read of data that omits a defaulted field: the decoder
    // injects the writer-schema default (the behavior validateDefault and
    // spf4j readers rely on)
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"long\"},"
        + "{\"name\":\"b\",\"type\":\"string\",\"default\":\"injected\"}]}");
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    GenericRecord rec = reader.read(null,
        new ExtendedJsonDecoder(schema, new ByteArrayInputStream("{\"a\":1}".getBytes(StandardCharsets.UTF_8))));
    assertEquals(1L, rec.get("a"));
    assertEquals("injected", rec.get("b").toString());
  }

  @Test
  public void testBooleanAndDoubleNaturalDecoding() throws IOException {
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"boolVal\",\"type\":\"boolean\"},"
        + "{\"name\":\"doubleVal\",\"type\":\"double\"},"
        + "{\"name\":\"mapVal\",\"type\":{\"type\":\"map\",\"values\":\"boolean\"}}]}");
    String json = "{\"boolVal\":true,\"doubleVal\":0.5,\"mapVal\":{\"a\":true,\"b\":false}}";
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    GenericRecord rec = reader.read(null,
        new ExtendedJsonDecoder(schema, new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))));
    assertEquals(Boolean.TRUE, rec.get("boolVal"));
    assertEquals(0.5, (Double) rec.get("doubleVal"), 0.0);
    assertEquals(Boolean.FALSE, ((Map<?, ?>) rec.get("mapVal")).get(new org.apache.avro.util.Utf8("b")));
  }

  @Test
  public void testReadBigDecimal() throws IOException {
    Schema schema = Schema.create(Schema.Type.LONG);
    ExtendedJsonDecoder decoder = new ExtendedJsonDecoder(schema,
        new ByteArrayInputStream("123.45".getBytes(StandardCharsets.UTF_8)));
    JsonExtensionDecoder ext = decoder;
    assertEquals(new BigDecimal("123.45"), ext.readBigDecimal(schema));
    // no number next -> null (the ee5e5ef94 semantics)
    ExtendedJsonDecoder decoder2 = new ExtendedJsonDecoder(Schema.create(Schema.Type.STRING),
        new ByteArrayInputStream("\"abc\"".getBytes(StandardCharsets.UTF_8)));
    assertNull(((JsonExtensionDecoder) decoder2).readBigDecimal(Schema.create(Schema.Type.STRING)));
  }
}
