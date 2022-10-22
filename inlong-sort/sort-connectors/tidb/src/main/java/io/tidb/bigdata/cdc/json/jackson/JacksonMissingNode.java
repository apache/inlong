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

package io.tidb.bigdata.cdc.json.jackson;

import io.tidb.bigdata.cdc.json.JsonNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;

public class JacksonMissingNode implements JsonNode {

  private static final JacksonMissingNode INSTANCE = new JacksonMissingNode();

  private JacksonMissingNode() {
  }

  public static JacksonMissingNode getInstance() {
    return INSTANCE;
  }

  @Override
  public boolean has(final String field) {
    return false;
  }

  @Override
  public Optional<JsonNode> get(final String field) {
    return Optional.empty();
  }

  @Override
  public byte[] binaryValue() {
    return new byte[0];
  }

  @Override
  public String textValue() {
    return null;
  }

  @Override
  public Number numberValue() {
    return null;
  }

  @Override
  public int intValue() {
    return 0;
  }

  @Override
  public BigInteger bigIntegerValue() {
    return null;
  }

  @Override
  public long longValue() {
    return 0;
  }

  @Override
  public boolean booleanValue() {
    return false;
  }

  @Override
  public BigDecimal bigDecimalValue() {
    return null;
  }

  @Override
  public Type getType() {
    return Type.MISSING;
  }

  @Override
  public Iterator<Entry<String, JsonNode>> fields() {
    return null;
  }
}
