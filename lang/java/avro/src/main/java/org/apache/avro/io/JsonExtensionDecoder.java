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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.avro.Schema;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * An extension interface that allows decoding decimal and other JSON types in
 * their natural (non-Avro-JSON-encoding) form. Implemented by
 * {@link ExtendedJsonDecoder}.
 *
 * <p>
 * Note: upstream's {@code GenericDatumReader} does not consult this
 * interface; it exists for readers that dispatch on it (as the fork's reader
 * and spf4j's lib-ext do). Ported from the fork's interface of the same name;
 * the fork's {@code javax.annotation.Nullable} markers are expressed in the
 * javadoc instead, as upstream carries no annotation library.
 * </p>
 */
public interface JsonExtensionDecoder {

  <T> T readValue(final Schema schema, final Class<T> clasz) throws IOException;

  JsonNode readValueAsTree(final Schema schema) throws IOException;

  JsonParser bufferValue(final Schema schema) throws IOException;

  /** @return the next value as a BigInteger, or null if no number is next */
  BigInteger readBigInteger(final Schema schema) throws IOException;

  /** @return the next value as a BigDecimal, or null if no number is next */
  BigDecimal readBigDecimal(final Schema schema) throws IOException;

}
