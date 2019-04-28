/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro;

import com.google.common.io.Resources;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.junit.Assert;
import static org.junit.Assert.assertNotSame;
import org.junit.Test;

public class TestSchema {
  @Test
  public void testSplitSchemaBuild() {
    Schema s = SchemaBuilder
       .record("HandshakeRequest")
       .namespace("org.apache.avro.ipc").fields()
         .name("clientProtocol").type().optional().stringType()
         .name("meta").type().optional().map().values().bytesType()
         .endRecord();

    String schemaString = s.toString();
    final int mid = schemaString.length() / 2;

    Schema parsedStringSchema = new org.apache.avro.Schema.Parser().parse(s.toString());
    Schema parsedArrayOfStringSchema =
      new org.apache.avro.Schema.Parser().parse
      (schemaString.substring(0, mid), schemaString.substring(mid));
    assertNotNull(parsedStringSchema);
    assertNotNull(parsedArrayOfStringSchema);
    assertEquals(parsedStringSchema.toString(), parsedArrayOfStringSchema.toString());
  }

  @Test
  public void testDuplicateRecordFieldName() {
    final Schema schema = Schema.createRecord("RecordName", null, null, false);
    final List<Field> fields = new ArrayList<Field>();
    fields.add(new Field("field_name", Schema.create(Type.NULL), null, null));
    fields.add(new Field("field_name", Schema.create(Type.INT), null, null));
    try {
      schema.setFields(fields);
      fail("Should not be able to create a record with duplicate field name.");
    } catch (AvroRuntimeException are) {
      assertTrue(are.getMessage().contains("Duplicate field field_name in record RecordName"));
    }
  }

  @Test
  public void testCreateUnionVarargs() {
    List<Schema> types = new ArrayList<Schema>();
    types.add(Schema.create(Type.NULL));
    types.add(Schema.create(Type.LONG));
    Schema expected = Schema.createUnion(types);

    Schema schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
    assertEquals(expected, schema);
  }

  @Test
  public void testEmptyRecordSchema() {
    Schema schema = Schema.createRecord("foobar", null, null, false);
    String schemaString = schema.toString();
    assertNotNull(schemaString);
  }

  @Test
  public void testSchemaWithFields() {
    List<Field> fields = new ArrayList<Field>();
    fields.add(new Field("field_name1", Schema.create(Type.NULL), null, null));
    fields.add(new Field("field_name2", Schema.create(Type.INT), null, null));
    Schema schema = Schema.createRecord("foobar", null, null, false, fields);
    String schemaString = schema.toString();
    assertNotNull(schemaString);
    assertEquals(2, schema.getFields().size());
  }

  @Test(expected = NullPointerException.class)
  public void testSchemaWithNullFields() {
    Schema.createRecord("foobar", null, null, false, null);
  }



    @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    Schema payload = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"KeyValue\",\"namespace\":\"org.apache.avro\","
            + "\"doc\":\"generic key value type\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"string\","
            + "\"avro.java.string\":\"String\"},\"doc\":\"generic key type\"},"
            + "{\"name\":\"value\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],"
            + "\"doc\":\"generic value type\"}]}");

    oos.writeObject(payload);
    oos.close();
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bis);
    Schema sp = (Schema) ois.readObject();
    Assert.assertEquals(payload, sp);
    ois.close();
  }

  @Test
  public void testSchemaAttr() throws IOException {
    Schema s = new Schema.Parser().parse(Resources.toString(Resources.getResource("TestSchema.avsc"),
            StandardCharsets.UTF_8));
    Assert.assertTrue(s.getProp("beta") != null);
  }

  @Test
  public void testAliasesSelfReferential() {
    String t1 = "{\"type\":\"record\",\"name\":\"a\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"record\",\"name\":\"C\",\"fields\":[{\"name\":\"c\",\"type\":{\"type\":\"array\",\"items\":[\"null\",\"C\"]}}]}}]}";
    String t2 = "{\"type\":\"record\",\"name\":\"x\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"record\",\"name\":\"C\",\"fields\":[{\"name\":\"d\",\"type\":{\"type\":\"array\",\"items\":[\"null\",\"C\"]},\"aliases\":[\"c\"]}]}}],\"aliases\":[\"a\"]}";
    Schema s1 = new Schema.Parser().parse(t1);
    Schema s2 = new Schema.Parser().parse(t2);

    assertEquals(s1.getAliases(), Collections.emptySet());
    assertEquals(s2.getAliases(), Collections.singleton("a"));

    Schema s3 = Schema.applyAliases(s1, s2);
    assertNotSame(s2, s3);
    assertEquals(s2, s3);
  }



}
