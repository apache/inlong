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

import io.tidb.bigdata.cdc.DDLValue;
import io.tidb.bigdata.cdc.Key;
import io.tidb.bigdata.cdc.ResolvedValue;
import io.tidb.bigdata.cdc.RowChangedValue;
import io.tidb.bigdata.cdc.RowColumn;
import io.tidb.bigdata.cdc.RowDeletedValue;
import io.tidb.bigdata.cdc.RowInsertedValue;
import io.tidb.bigdata.cdc.RowUpdatedValue;
import io.tidb.bigdata.cdc.Value;
import io.tidb.bigdata.cdc.ValueDecoder;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * TiCDC open protocol json format event value decoder, parse bits into event value instances.
 */
public class JsonValueDecoder implements ValueDecoder {

  private static final ThreadLocal<ArrayList<RowColumn>> tlsRowColumnBuffer =
      ThreadLocal.withInitial(ArrayList::new);

  private static final String DDL_QUERY_TOKEN = "q";
  private static final String TYPE_TOKEN = "t";
  private static final String COLUMN_FLAG_TOKEN = "f";
  private static final String COLUMN_WHERE_HANDLE_TOKEN = "h";
  private static final String COLUMN_VALUE_TOKEN = "v";
  private static final String UPDATE_NEW_VALUE_TOKEN = "u";
  private static final String UPDATE_OLD_VALUE_TOKEN = "p";
  private static final String UPDATE_DELETE_VALUE_TOKEN = "d";
  private final JsonDecoder decoder;

  public JsonValueDecoder(final DataInputStream input, final JsonParser parser) {
    // we know there is only one json parser at this time
    decoder = new JsonDecoder(input, parser);
  }

  public JsonValueDecoder(final byte[] input, final JsonParser parser) {
    this(new DataInputStream(new ByteArrayInputStream(input)), parser);
  }

  private static DDLValue parseDDLValue(final JsonNode node) {
    return new DDLValue(node.mustGetText(DDL_QUERY_TOKEN), node.mustGetInt(TYPE_TOKEN));
  }

  private static Object checkAndConvertFromString(int type, long flags, String value) {
    if (RowColumn.isUnsigned(flags)) {
      switch (RowColumn.getType(type)) {
        case TINYTEXT:
          // FALLTHROUGH
        case MEDIUMTEXT:
          // FALLTHROUGH
        case LONGTEXT:
          // FALLTHROUGH
        case TEXT:
          return Base64.getDecoder().decode(value);
        default:
          break;
      }
    }
    return value;
  }

  private static RowColumn[] parseColumns(final JsonNode row) {
    final ArrayList<RowColumn> buffer = tlsRowColumnBuffer.get();
    for (final Iterator<Map.Entry<String, JsonNode>> it = row.fields(); it.hasNext(); ) {
      final Map.Entry<String, JsonNode> entry = it.next();
      final JsonNode node = entry.getValue();
      final JsonNode valueNode = node.mustGet(COLUMN_VALUE_TOKEN);
      final Object value;
      final int type = node.mustGetInt(TYPE_TOKEN);
      final long flags = node.getLong(COLUMN_FLAG_TOKEN, 0);

      switch (valueNode.getType()) {
        case BOOLEAN:
          value = valueNode.booleanValue();
          break;
        case NUMBER:
          value = valueNode.numberValue();
          break;
        case STRING:
          value = checkAndConvertFromString(type, flags, valueNode.textValue());
          break;
        case NULL:
          value = null;
          break;
        default:
          throw new RuntimeException("Invalid column value type: " + valueNode.getType());
      }
      buffer.add(new RowColumn(
          entry.getKey(),
          value,
          node.getBoolean(COLUMN_WHERE_HANDLE_TOKEN, false),
          type, flags));
    }
    final RowColumn[] columns = buffer.toArray(new RowColumn[0]);
    buffer.clear();
    return columns;
  }

  private static RowChangedValue parseRowChangedDeleteValue(final JsonNode object) {
    return new RowDeletedValue(parseColumns(object));
  }

  private static RowChangedValue parseRowChangedUpdateValue(final Optional<JsonNode> oldValue,
      final JsonNode newValue) {
    return oldValue
        .map(o -> (RowChangedValue) (new RowUpdatedValue(parseColumns(o), parseColumns(newValue))))
        .orElseGet(() -> new RowInsertedValue(parseColumns(newValue)));
  }

  private static RowChangedValue parseRowChangedValue(final JsonNode node) {
    if (node.has(UPDATE_NEW_VALUE_TOKEN)) {
      return parseRowChangedUpdateValue(node.get(UPDATE_OLD_VALUE_TOKEN),
          node.mustGet(UPDATE_NEW_VALUE_TOKEN));
    } else if (node.has(UPDATE_DELETE_VALUE_TOKEN)) {
      return parseRowChangedDeleteValue(node.mustGet(UPDATE_DELETE_VALUE_TOKEN));
    } else {
      throw new RuntimeException("Can not parse Value:" + node);
    }
  }

  @Override
  public boolean hasNext() {
    return decoder.hasNext();
  }

  @Override
  public Value next() {
    final JsonNode node = decoder.next();
    if (node.getType() == JsonNode.Type.MISSING) {
      return ResolvedValue.getInstance();
    } else if (node.has(DDL_QUERY_TOKEN)) {
      return parseDDLValue(node);
    } else {
      return parseRowChangedValue(node);
    }
  }

  public Value next(final Key key) {
    final JsonNode node = decoder.next();
    switch (key.getType()) {
      case ROW_CHANGED:
        return parseRowChangedValue(node);
      case DDL:
        return parseDDLValue(node);
      case RESOLVED:
        return ResolvedValue.getInstance();
      default:
        throw new RuntimeException("Invalid key type");
    }
  }
}
