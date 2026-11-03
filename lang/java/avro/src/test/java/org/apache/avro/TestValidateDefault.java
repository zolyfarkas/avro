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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Pins the fork's decode-based default validation (ee5e5ef94): defaults for
 * logical-typed fields are validated by actually decoding them with
 * ExtendedJsonDecoder, and structural checks follow the fork's rules (which
 * differ from upstream's stricter/looser boolean check in the places these
 * tests pin).
 */
public class TestValidateDefault {

  private static Schema parseField(String fieldJson) {
    return new Schema.Parser()
        .parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":[" + fieldJson + "]}");
  }

  @Test
  public void testDecimalNaturalDefaultValidates() {
    // a decimal default in natural JSON form (a number): upstream's
    // structural check rejects it for a bytes schema; the fork's decode
    // accepts it
    parseField("{\"name\":\"d\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
        + "\"precision\":9,\"scale\":2},\"default\":1.23}");
  }

  @Test
  public void testDecimalObjectDefaultRejected() {
    AvroTypeException e = assertThrows(AvroTypeException.class,
        () -> parseField("{\"name\":\"d\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":9,\"scale\":2},\"default\":{}}"));
    assertTrue(e.getMessage(), e.getMessage().contains("Invalid default for field d"));
  }

  @Test
  public void testUuidDefaultValidates() {
    // logical-typed defaults go through the decode path; a valid uuid
    // default parses
    parseField("{\"name\":\"u\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"},"
        + "\"default\":\"123e4567-e89b-12d3-a456-426614174000\"}");
  }

  @Test
  public void testFractionalIntDefaultRejected() {
    // CHANGED EXPECTATION vs the fork: the fork's structural rule for INT
    // is isNumber() only and accepts 1.5; upstream's integral-and-in-range
    // check is kept (upstream's TestSchema pins it), so this is rejected
    assertThrows(AvroTypeException.class, () -> parseField("{\"name\":\"i\",\"type\":\"int\",\"default\":1.5}"));
    parseField("{\"name\":\"i\",\"type\":\"int\",\"default\":5}");
  }

  @Test
  public void testUnionDefaultMustMatchFirstBranch() {
    // fork validates a union default against the first branch only;
    // upstream's any-branch check accepted this
    assertThrows(AvroTypeException.class,
        () -> parseField("{\"name\":\"u\",\"type\":[\"string\",\"int\"],\"default\":5}"));
    parseField("{\"name\":\"u\",\"type\":[\"string\",\"int\"],\"default\":\"x\"}");
  }

  @Test
  public void testIsValidDefaultWidenedCatch() throws Exception {
    // A logical-type conversion that fails with a plain RuntimeException
    // (not an AvroRuntimeException): with the fork's 1059a43e1 widening,
    // isValidDefault reports the default invalid instead of propagating.
    LogicalType lt = new LogicalType("widening-test-lt") {
    };
    Schema schema = Schema.create(Schema.Type.STRING);
    lt.addToSchema(schema);
    org.apache.avro.Conversion<String> conversion = new org.apache.avro.Conversion<String>() {
      @Override
      public Class<String> getConvertedType() {
        return String.class;
      }

      @Override
      public String getLogicalTypeName() {
        return "widening-test-lt";
      }

      @Override
      public String fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        throw new IllegalStateException("conversion boom");
      }

      @Override
      public CharSequence toCharSequence(String value, Schema schema, LogicalType type) {
        return value;
      }
    };
    org.apache.avro.generic.GenericData.get().addLogicalTypeConversion(conversion);
    assertFalse(schema.isValidDefault(Schema.MAPPER.readTree("\"x\"")));
  }

  @Test
  public void testIsValidDefaultBasics() throws Exception {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    assertTrue(stringSchema.isValidDefault(Schema.MAPPER.readTree("\"x\"")));
    assertFalse(stringSchema.isValidDefault(Schema.MAPPER.readTree("5")));
    Schema intSchema = Schema.create(Schema.Type.INT);
    assertTrue(intSchema.isValidDefault(Schema.MAPPER.readTree("5")));
    assertFalse(intSchema.isValidDefault(Schema.MAPPER.readTree("\"x\"")));
  }
}
