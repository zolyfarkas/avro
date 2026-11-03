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

import java.util.Arrays;
import org.junit.Test;

/**
 * Adapted from the fork's TestEnumStringSymbols: pins the schema-level
 * string-symbol contract (getEnumStringSymbol / getEnumStringSymbols driven
 * by the {@code stringSymbols} property). The fork test's GenericData
 * EnumSymbol assertions are not ported: they exercise the fork's separate
 * EnumSymbol data model, which is not part of the enum-codegen commit
 * (a49c5c2b3) being ported here.
 */
public class TestEnumStringSymbolsPort {

  private static Schema enumSchema() {
    return new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"myrecord\",\"namespace\":\"org.example\","
        + "\"fields\":[{\"name\":\"f0\",\"type\":{\"type\":\"enum\",\"name\":\"MyEnum\","
        + "\"symbols\":[\"UNKNOWN\",\"A\",\"B\",\"C\"],\"default\":\"UNKNOWN\","
        + "\"stringSymbols\":{\"A\":\"A+\"}}}]}").getField("f0").schema();
  }

  @Test
  public void testStringSymbols() {
    Schema enumSchema = enumSchema();
    assertEquals("A+", enumSchema.getEnumStringSymbol("A"));
    // unmapped symbols map to themselves
    assertEquals("B", enumSchema.getEnumStringSymbol("B"));
    assertEquals("UNKNOWN", enumSchema.getEnumStringSymbol("UNKNOWN"));
    assertEquals(Arrays.asList("UNKNOWN", "A+", "B", "C"), enumSchema.getEnumStringSymbols());
  }

  @Test
  public void testNoStringSymbolsPropIsIdentity() {
    Schema enumSchema = new Schema.Parser()
        .parse("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"X\",\"Y\"]}");
    assertEquals("X", enumSchema.getEnumStringSymbol("X"));
    assertEquals(Arrays.asList("X", "Y"), enumSchema.getEnumStringSymbols());
  }

  @Test
  public void testNonEnumThrows() {
    Schema notEnum = Schema.create(Schema.Type.STRING);
    assertThrows(AvroRuntimeException.class, () -> notEnum.getEnumStringSymbol("A"));
    assertThrows(AvroRuntimeException.class, notEnum::getEnumStringSymbols);
  }
}
