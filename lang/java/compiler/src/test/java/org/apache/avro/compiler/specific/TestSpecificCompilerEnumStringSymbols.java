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
package org.apache.avro.compiler.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Pins the ported enum codegen contract (a49c5c2b3): generated enums carry
 * their string symbol as toString(), expose the Avro symbol via getSymbol(),
 * compile, and round-trip through the specific data layer.
 */
public class TestSpecificCompilerEnumStringSymbols {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private ClassLoader compile(String schemaJson) throws Exception {
    Schema schema = new Schema.Parser().parse(schemaJson);
    SpecificCompiler compiler = new SpecificCompiler(schema);
    compiler.setStringType(GenericData.StringType.String);
    Collection<SpecificCompiler.OutputFile> outputs = compiler.compile();
    File dst = tempFolder.newFolder();
    TestSpecificCompiler.assertCompilesWithJavaCompiler(dst, outputs);
    return new URLClassLoader(new URL[] { dst.toURI().toURL() }, getClass().getClassLoader());
  }

  @Test
  public void testGeneratedEnumCarriesStringSymbols() throws Exception {
    ClassLoader cl = compile("{\"type\":\"enum\",\"name\":\"MyEnum\",\"namespace\":\"test\","
        + "\"symbols\":[\"UNKNOWN\",\"A\",\"B\"],\"default\":\"UNKNOWN\",\"stringSymbols\":{\"A\":\"A+\"}}");
    Class<?> enumClass = Class.forName("test.MyEnum", true, cl);
    Object a = Enum.valueOf((Class<Enum>) enumClass, "A");
    // GenericEnumSymbol contract: toString() is the (string) symbol
    assertEquals("A+", a.toString());
    Method getSymbol = enumClass.getMethod("getSymbol");
    assertEquals("A", getSymbol.invoke(a));
    Object unknown = Enum.valueOf((Class<Enum>) enumClass, "UNKNOWN");
    assertEquals("UNKNOWN", unknown.toString());
    // the embedded schema still carries the mapping
    Schema schema = (Schema) enumClass.getField("SCHEMA$").get(null);
    assertEquals("A+", schema.getEnumStringSymbol("A"));
  }

  @Test
  public void testGeneratedEnumRoundTripsThroughSpecificData() throws Exception {
    ClassLoader cl = compile("{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"test\",\"fields\":["
        + "{\"name\":\"f\",\"type\":{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"X\",\"Y\"],"
        + "\"default\":\"X\"}}]}");
    Class<?> recordClass = Class.forName("test.R", true, cl);
    Class<?> enumClass = Class.forName("test.E", true, cl);
    Object enumY = Enum.valueOf((Class<Enum>) enumClass, "Y");
    Object record = recordClass.getDeclaredConstructor().newInstance();
    recordClass.getMethod("setF", enumClass).invoke(record, enumY);

    Schema schema = (Schema) recordClass.getField("SCHEMA$").get(null);
    org.apache.avro.specific.SpecificData data = new org.apache.avro.specific.SpecificData(cl);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SpecificDatumWriter<Object> writer = new SpecificDatumWriter<>(schema, data);
    org.apache.avro.io.Encoder enc = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(record, enc);
    enc.flush();

    SpecificDatumReader<Object> reader = new SpecificDatumReader<>(schema, schema, data);
    Object read = reader.read(null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));
    Object f = read.getClass().getMethod("getF").invoke(read);
    assertEquals("Y", f.toString());
    assertTrue(f == Enum.valueOf((Class<Enum>) enumClass, "Y"));
  }

  @Test
  public void testEscapeJava() {
    assertEquals("plain", SpecificCompiler.escapeJava("plain"));
    assertEquals("a\\\"b", SpecificCompiler.escapeJava("a\"b"));
    assertEquals("a\\\\b", SpecificCompiler.escapeJava("a\\b"));
    assertEquals("a\\nb", SpecificCompiler.escapeJava("a\nb"));
    assertEquals("\\u00e9", SpecificCompiler.escapeJava("é"));
  }
}
