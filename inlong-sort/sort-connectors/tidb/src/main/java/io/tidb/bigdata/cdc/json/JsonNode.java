/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.cdc.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/*
 * Interface for json node
 */
public interface JsonNode {

  boolean has(String field);

  Optional<JsonNode> get(String field);

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  default JsonNode mustGet(final String field) {
    return get(field).get();
  }

  byte[] binaryValue();

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  default byte[] mustGetBinary(final String field) {
    return getBinary(field).get();
  }

  default Optional<byte[]> getBinary(final String field) {
    return get(field).map(JsonNode::binaryValue);
  }

  default byte[] getBinary(final String field, final byte[] dft) {
    return getBinary(field).orElse(dft);
  }

  String textValue();

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  default String mustGetText(final String field) {
    return getText(field).get();
  }

  default Optional<String> getText(final String field) {
    return get(field).map(JsonNode::textValue);
  }

  default String getText(final String field, final String dft) {
    return getText(field).orElse(dft);
  }

  Number numberValue();

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  default Number mustGetNumber(final String field) {
    return getNumber(field).get();
  }

  default Optional<Number> getNumber(final String field) {
    return get(field).map(JsonNode::numberValue);
  }

  default Number getNumber(final String field, final Number dft) {
    return getNumber(field).orElse(dft);
  }

  int intValue();

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  default int mustGetInt(final String field) {
    return getInt(field).get();
  }

  default Optional<Integer> getInt(final String field) {
    return get(field).map(JsonNode::intValue);
  }

  default int getInt(final String field, final int dft) {
    return getInt(field).orElse(dft);
  }

  BigInteger bigIntegerValue();

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  default BigInteger mustGetBigInteger(final String field) {
    return getBigInteger(field).get();
  }

  default Optional<BigInteger> getBigInteger(final String field) {
    return get(field).map(JsonNode::bigIntegerValue);
  }

  default BigInteger getBigInteger(final String field, final BigInteger dft) {
    return getBigInteger(field).orElse(dft);
  }

  long longValue();

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  default long mustGetLong(final String field) {
    return getLong(field).get();
  }

  default Optional<Long> getLong(final String field) {
    return get(field).map(JsonNode::longValue);
  }

  default long getLong(final String field, final long dft) {
    return getLong(field).orElse(dft);
  }

  boolean booleanValue();

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  default boolean mustGetBoolean(final String field) {
    return getBoolean(field).get();
  }

  default Optional<Boolean> getBoolean(final String field) {
    return get(field).map(JsonNode::booleanValue);
  }

  default boolean getBoolean(final String field, final boolean dft) {
    return getBoolean(field).orElse(dft);
  }

  BigDecimal bigDecimalValue();

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  default BigDecimal mustGetBigDecimal(final String field) {
    return getBigDecimal(field).get();
  }

  default Optional<BigDecimal> getBigDecimal(final String field) {
    return get(field).map(JsonNode::bigDecimalValue);
  }

  default BigDecimal getBigDecimal(final String field, final BigDecimal dft) {
    return getBigDecimal(field).orElse(dft);
  }

  Type getType();

  Iterator<Map.Entry<String, JsonNode>> fields();

  enum Type {
    NOT_SUPPORTED,
    BOOLEAN,
    NULL,
    NUMBER,
    OBJECT,
    ARRAY,
    STRING,
    MISSING,
  }
}
