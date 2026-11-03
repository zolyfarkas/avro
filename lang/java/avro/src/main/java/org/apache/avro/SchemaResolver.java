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

/**
 * Resolves schemas by an external id, and provides the id for a schema, so
 * schemas can be referenced by id instead of by name or by full definition.
 *
 * <p>
 * When a resolver is registered on a {@link ParseContext} (or through
 * {@link Schema.Parser#setSchemaResolver(SchemaResolver)}), the schema parser
 * consults it for object nodes carrying the resolver's reference attribute
 * (see {@link #getJsonAttrName()}): such a node is replaced by
 * {@link #resolveSchema(String)} applied to the attribute value. When a schema
 * is written with {@link Schema#toJson(SchemaResolver, com.fasterxml.jackson.core.JsonGenerator)},
 * a named schema whose {@link #getId(Schema)} is non-null is written as a
 * reference object instead of its definition.
 * </p>
 *
 * <p>
 * With no resolver registered, parsing and writing are exactly upstream's.
 * (Not to be confused with {@code org.apache.avro.util.SchemaResolver}, an
 * unrelated internal helper for resolving parsed name references.)
 * </p>
 */
public interface SchemaResolver {

  /**
   * Resolve the schema with the given id.
   *
   * @param id the id of the schema to resolve
   * @return the schema for {@code id}; never {@code null}
   */
  Schema resolveSchema(String id);

  /**
   * The id of the given schema, or {@code null} if this resolver has no id for
   * it (in which case the schema is written out in full).
   */
  String getId(Schema schema);

  /**
   * The JSON attribute that carries a schema reference in schema documents.
   * Defaults to {@code "$ref"}.
   */
  default String getJsonAttrName() {
    return "$ref";
  }

  /** A resolver that resolves nothing and assigns no ids. */
  SchemaResolver NONE = new SchemaResolver() {
    @Override
    public Schema resolveSchema(final String id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getId(final Schema schema) {
      return null;
    }
  };
}
