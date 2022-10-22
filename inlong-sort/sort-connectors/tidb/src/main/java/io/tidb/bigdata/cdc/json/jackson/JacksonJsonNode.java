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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

class JacksonJsonNode implements JsonNode {

  private final JacksonContext context;
  private final Object node;

  JacksonJsonNode(final JacksonContext context, final Object node) {
    this.context = context;
    this.node = node;
  }

  @Override
  public boolean has(final String field) {
    return context.has(node, field);
  }

  @Override
  public Optional<JsonNode> get(final String field) {
    return Optional.ofNullable(context.get(node, field))
        .map(node -> new JacksonJsonNode(context, node));
  }

  @Override
  public byte[] binaryValue() {
    return context.binaryValue(node);
  }

  @Override
  public String textValue() {
    return context.textValue(node);
  }

  @Override
  public Number numberValue() {
    return context.numberValue(node);
  }

  @Override
  public int intValue() {
    return context.intValue(node);
  }

  @Override
  public BigInteger bigIntegerValue() {
    return context.bigIntegerValue(node);
  }

  @Override
  public long longValue() {
    return context.longValue(node);
  }

  @Override
  public boolean booleanValue() {
    return context.booleanValue(node);
  }

  @Override
  public BigDecimal bigDecimalValue() {
    return context.bigDecimalValue(node);
  }

  @Override
  public Type getType() {
    return context.getNodeType(node);
  }

  @Override
  public Iterator<Entry<String, JsonNode>> fields() {
    final Iterator<Map.Entry<String, Object>> iterator = context.fields(node);
    return new Iterator<Map.Entry<String, JsonNode>>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Map.Entry<String, JsonNode> next() {
        final Map.Entry<String, Object> entry = iterator.next();
        return new Map.Entry<String, JsonNode>() {
          private JacksonJsonNode value;

          @Override
          public String getKey() {
            return entry.getKey();
          }

          @Override
          public JsonNode getValue() {
            if (value == null) {
              value = new JacksonJsonNode(context, entry.getValue());
            }
            return value;
          }

          @Override
          public JsonNode setValue(JsonNode value) {
            throw new IllegalStateException("Updating JsonNode is prohibited!");
          }
        };
      }
    };
  }
}
