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
package org.apache.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.StringWriter;
import org.junit.Test;

/**
 * Pins the resolver seam contract: with no resolver registered, parse and
 * write behavior is upstream's (a {@code $ref} node is not special); with a
 * resolver registered, the reference-attribute hook fires on both sides.
 */
public class TestSchemaResolverSeams {

  private static final String WITH_NAMED_REFS = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
      + "{\"name\":\"first\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":[{\"name\":\"v\",\"type\":\"long\"}]}},"
      + "{\"name\":\"second\",\"type\":\"Inner\"}]}";

  @Test
  public void testNoResolverParsesNamedRefsAsUpstream() {
    Schema schema = new Schema.Parser().parse(WITH_NAMED_REFS);
    assertEquals(WITH_NAMED_REFS.replace(" ", ""), schema.toString());
    // and a re-parse of the rendering is stable
    assertEquals(schema, new Schema.Parser().parse(schema.toString()));
  }

  @Test
  public void testNoResolverRefNodeIsNotSpecial() {
    // without a resolver, {"$ref": ...} has no "type" and fails as upstream
    assertThrows(SchemaParseException.class, () -> new Schema.Parser().parse("{\"$ref\":\"anything\"}"));
  }

  @Test
  public void testNoResolverWriteIsUpstream() throws IOException {
    Schema schema = new Schema.Parser().parse(WITH_NAMED_REFS);
    StringWriter writer = new StringWriter();
    JsonGenerator gen = Schema.FACTORY.createGenerator(writer);
    schema.toJson(SchemaResolver.NONE, gen);
    gen.flush();
    assertEquals(schema.toString(), writer.toString());
  }

  @Test
  public void testCustomAttributeName() throws IOException {
    Schema recSchema = SchemaBuilder.record("TestRecord").prop("id", "testId").fields().nullableLong("number", 0L)
        .endRecord();
    Schema schema = SchemaBuilder.array().items(recSchema);
    SchemaResolver resolver = new SchemaResolver() {
      @Override
      public Schema resolveSchema(String id) {
        if ("testId".equals(id)) {
          return recSchema;
        }
        throw new IllegalArgumentException("Unknown schema with id " + id);
      }

      @Override
      public String getId(Schema s) {
        return s.getProp("id");
      }

      @Override
      public String getJsonAttrName() {
        return "ref";
      }
    };
    StringWriter writer = new StringWriter();
    JsonGenerator gen = Schema.FACTORY.createGenerator(writer);
    schema.toJson(new AvroNamesRefResolver(resolver), gen);
    gen.flush();
    assertEquals("{\"type\":\"array\",\"items\":{\"ref\":\"testId\"}}", writer.toString());
    Schema result = new Schema.Parser().setSchemaResolver(resolver).parse(writer.toString());
    assertEquals(schema, result);
  }

  @Test
  public void testCustomReadWriteHooksFire() throws IOException {
    // A resolver that ignores getId/$ref entirely and uses its own wire
    // form through the customRead/customWrite hooks (the d5c248b9f shape).
    Schema recSchema = SchemaBuilder.record("TestRecord").fields().nullableLong("number", 0L).endRecord();
    Schema schema = SchemaBuilder.array().items(recSchema);
    SchemaResolver resolver = new SchemaResolver() {
      @Override
      public Schema resolveSchema(String id) {
        throw new UnsupportedOperationException("custom hooks only");
      }

      @Override
      public String getId(Schema s) {
        return null;
      }

      @Override
      public boolean customWrite(Schema s, com.fasterxml.jackson.core.JsonGenerator gen) throws IOException {
        if (s.equals(recSchema)) {
          gen.writeStartObject();
          gen.writeStringField("$id", "theRecord");
          gen.writeEndObject();
          return true;
        }
        return false;
      }

      @Override
      public Schema customRead(java.util.function.Function<String, com.fasterxml.jackson.databind.JsonNode> object) {
        com.fasterxml.jackson.databind.JsonNode id = object.apply("$id");
        if (id != null && "theRecord".equals(id.asText())) {
          return recSchema;
        }
        return null;
      }
    };
    StringWriter writer = new StringWriter();
    JsonGenerator gen = Schema.FACTORY.createGenerator(writer);
    schema.toJson(resolver, gen);
    gen.flush();
    assertEquals("{\"type\":\"array\",\"items\":{\"$id\":\"theRecord\"}}", writer.toString());
    Schema result = new Schema.Parser().setSchemaResolver(resolver).parse(writer.toString());
    assertEquals(schema, result);
  }

  @Test
  public void testTopLevelRefResolves() {
    Schema recSchema = SchemaBuilder.record("TestRecord").fields().nullableLong("number", 0L).endRecord();
    SchemaResolver resolver = new SchemaResolver() {
      @Override
      public Schema resolveSchema(String id) {
        if ("testId".equals(id)) {
          return recSchema;
        }
        throw new IllegalArgumentException("Unknown schema with id " + id);
      }

      @Override
      public String getId(Schema s) {
        return null;
      }
    };
    Schema result = new Schema.Parser().setSchemaResolver(resolver).parse("{\"$ref\":\"testId\"}");
    assertEquals(recSchema, result);
  }
}
