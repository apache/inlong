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

package io.tidb.bigdata.cdc.craft;

import io.tidb.bigdata.cdc.DDLValue;
import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.Key;
import io.tidb.bigdata.cdc.ResolvedValue;
import io.tidb.bigdata.cdc.RowChangedValue;
import io.tidb.bigdata.cdc.RowColumn;
import io.tidb.bigdata.cdc.RowDeletedValue;
import io.tidb.bigdata.cdc.RowInsertedValue;
import io.tidb.bigdata.cdc.RowUpdatedValue;
import io.tidb.bigdata.cdc.Value;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

public class CraftParserState implements Iterator<Event> {

  private static final byte COLUMN_GROUP_TYPE_OLD = 0x2;
  private static final byte COLUMN_GROUP_TYPE_NEW = 0x1;

  private final Codec codec;
  private final Key[] keys;
  private final int[][] sizeTables;
  private final int[] valueSizeTable;
  private final Event[] events;
  private int index;
  private final CraftTermDictionary termDictionary;

  CraftParserState(Codec codec, Key[] keys, int[][] sizeTables,
      CraftTermDictionary termDictionary) {
    this(codec, keys, sizeTables, new Event[keys.length], termDictionary);
  }

  private CraftParserState(Codec codec, Key[] keys, int[][] sizeTables,
      Event[] events, CraftTermDictionary termDictionary) {
    this.codec = codec;
    this.keys = keys;
    this.index = 0;
    this.events = events;
    this.sizeTables = sizeTables;
    this.valueSizeTable = sizeTables[CraftParser.VALUE_SIZE_TABLE_INDEX];
    this.termDictionary = termDictionary;
  }

  @Override
  public CraftParserState clone() {
    return new CraftParserState(codec.clone(), keys, sizeTables,
        Arrays.copyOf(events, events.length), termDictionary);
  }

  @Override
  public boolean hasNext() {
    return index < keys.length;
  }

  @Override
  public Event next() {
    Event event = events[index];
    if (event != null) {
      return event;
    }
    event = events[index] = new Event(keys[index], this.decodeValue());
    index++;
    return event;
  }

  private DDLValue decodeDDL(Codec codec) {
    long type = codec.decodeUvarint();
    String query = codec.decodeString();
    return new DDLValue(query, (int) type);
  }

  private Object decodeTiDBType(long type, long flags, byte[] value) {
    if (value == null) {
      return null;
    }
    switch (RowColumn.getType((int) type)) {
      case DATE:
        // FALLTHROUGH
      case DATETIME:
        // FALLTHROUGH
      case NEWDATE:
        // FALLTHROUGH
      case TIMESTAMP:
        // FALLTHROUGH
      case TIME:
        // FALLTHROUGH
      case DECIMAL:
        // FALLTHROUGH
      case JSON:
        // value type for these mysql types is string
        return new String(value, StandardCharsets.UTF_8);
      case CHAR:
        // FALLTHROUGH
      case BINARY:
        // FALLTHROUGH
      case VARBINARY:
        // FALLTHROUGH
      case VARCHAR:
        // FALLTHROUGH
      case TINYTEXT:
        // FALLTHROUGH
      case TINYBLOB:
        // FALLTHROUGH
      case MEDIUMTEXT:
        // FALLTHROUGH
      case MEDIUMBLOB:
        // FALLTHROUGH
      case LONGTEXT:
        // FALLTHROUGH
      case LONGBLOB:
        // FALLTHROUGH
      case TEXT:
        // FALLTHROUGH
      case BLOB:
        // raw value type for these mysql types is []byte
        return value;
      case ENUM:
        // FALLTHROUGH
      case SET:
        // FALLTHROUGH
      case BIT:
        // value type for these mysql types is uint64
        return new Codec(value).decodeUvarint();
      case FLOAT:
      case DOUBLE:
        // value type for these mysql types is float64
        return new Codec(value).decodeFloat64();
      case TINYINT:
      case BOOL:
      case SMALLINT:
      case INT:
      case BIGINT:
      case MEDIUMINT:
        // value types for these mysql types are int64 or uint64 depends on flags
        Codec codec = new Codec(value);
        if (RowColumn.isUnsigned(flags)) {
          return codec.decodeUvarint();
        } else {
          return codec.decodeVarint();
        }
      case YEAR:
        return new Codec(value).decodeVarint();
      case NULL:
        // FALLTHROUGH
      case GEOMETRY:
        // FALLTHROUGH
      default:
        return null;
    }
  }

  private RowColumn[] decodeColumnGroup(Codec codec) {
    int numOfColumns = (int) codec.decodeUvarint();
    String[] names = termDictionary.decodeChunk(codec, numOfColumns);
    long[] types = codec.decodeUvarintChunk(numOfColumns);
    long[] flags = codec.decodeUvarintChunk(numOfColumns);
    byte[][] values = codec.decodeNullableBytesChunk(numOfColumns);

    RowColumn[] columns = new RowColumn[numOfColumns];
    for (int idx = 0; idx < numOfColumns; idx++) {
      columns[idx] = new RowColumn(names[idx],
          decodeTiDBType(types[idx], flags[idx], values[idx]),
          false, (int) types[idx], flags[idx]);
    }

    return columns;
  }

  private RowChangedValue decodeRowChanged(Codec codec) {
    RowColumn[] oldColumns = null;
    RowColumn[] newColumns = null;
    int[] sizeTable = sizeTables[CraftParser.COLUMN_GROUP_SIZE_TABLE_START_INDEX + index];
    int columnGroupIndex = 0;
    while (codec.available() > 0) {
      final int size = sizeTable[columnGroupIndex++];
      final Codec columnGroupCodec = codec.truncateHeading(size);
      byte type = (byte) columnGroupCodec.decodeUint8();
      RowColumn[] columns = decodeColumnGroup(columnGroupCodec);
      switch (type) {
        case COLUMN_GROUP_TYPE_OLD:
          oldColumns = columns;
          break;
        case COLUMN_GROUP_TYPE_NEW:
          newColumns = columns;
          break;
        default:
          throw new IllegalStateException("Unknown column group type: " + type);
      }
    }
    if (oldColumns != null) {
      if (newColumns == null) {
        return new RowDeletedValue(oldColumns);
      } else {
        return new RowUpdatedValue(oldColumns, newColumns);
      }
    } else {
      return new RowInsertedValue(newColumns);
    }
  }

  private Value decodeValue() {
    int size = valueSizeTable[index];
    Key.Type type = keys[index].getType();
    switch (keys[index].getType()) {
      case DDL:
        return decodeDDL(codec.truncateHeading(size));
      case RESOLVED:
        return ResolvedValue.getInstance();
      case ROW_CHANGED:
        return decodeRowChanged(codec.truncateHeading(size));
      default:
        throw new IllegalStateException("Unknown value type: " + type);
    }
  }
}