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

package io.tidb.bigdata.cdc;

import io.tidb.bigdata.cdc.json.jackson.JacksonObjectNode;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/*
 * TiCDC row changed event value
 */
public abstract class RowChangedValue implements Value {

  private final RowColumn[] oldValue;
  private final RowColumn[] newValue;

  protected RowChangedValue(final RowColumn[] oldValue, final RowColumn[] newValue) {
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  protected static JacksonObjectNode toJson(RowColumn[] columns, JacksonObjectNode object) {
    for (RowColumn column : columns) {
      JacksonObjectNode node = object.putObject(column.getName());
      node.put("f", column.getFlags());
      node.put("h", column.isWhereHandle());
      node.put("t", column.getType().code());
      switch (column.getType()) {
        case BOOL:
          node.put("v", column.asBoolean());
          break;
        case TINYINT:
          node.put("v", column.asTinyInt());
          break;
        case SMALLINT:
          node.put("v", column.asSmallInt());
          break;
        case INT:
          node.put("v", column.asInt());
          break;
        case BIGINT:
        case YEAR:
          node.put("v", column.asBigInt());
          break;
        case MEDIUMINT:
          node.put("v", column.asMediumInt());
          break;
        case FLOAT:
          node.put("v", column.asFloat());
          break;
        case DOUBLE:
          node.put("v", column.asDouble());
          break;
        case NULL:
        case GEOMETRY:
          node.putNull("v");
          break;
        case TIMESTAMP:
          node.put("v", column.asText());
          break;
        case DATE:
          node.put("v", column.asText());
          break;
        case TIME:
          node.put("v", column.asText());
          break;
        case DATETIME:
          node.put("v", column.asText());
          break;
        case NEWDATE:
          node.put("v", column.asText());
          break;
        case BIT:
          node.put("v", column.asBit());
          break;
        case DECIMAL:
          node.put("v", column.asDecimal());
          break;
        case ENUM:
          node.put("v", column.asEnum());
          break;
        case SET:
          node.put("v", column.asSet());
          break;
        case JSON:
          node.put("v", column.asJson());
          break;
        case VARCHAR:
          node.put("v", column.asVarchar());
          break;
        case TINYTEXT:
          node.put("v", column.asTinyText());
          break;
        case MEDIUMTEXT:
          node.put("v", column.asMediumText());
          break;
        case LONGTEXT:
          node.put("v", column.asLongText());
          break;
        case TEXT:
          node.put("v", column.asText());
          break;
        case CHAR:
          node.put("v", column.asChar());
          break;
        case TINYBLOB:
          node.put("v", column.asTinyBlob());
          break;
        case MEDIUMBLOB:
          node.put("v", column.asMediumBlob());
          break;
        case LONGBLOB:
          node.put("v", column.asLongBlob());
          break;
        case BLOB:
          node.put("v", column.asBlob());
          break;
        case VARBINARY:
          node.put("v", column.asVarbinary());
          break;
        case BINARY:
          node.put("v", column.asBinary());
          break;
        default:
          throw new IllegalArgumentException("Unknown column type: " + column.getType());
      }
    }
    return object;
  }

  public abstract Type getType();

  public RowColumn[] getOldValue() {
    return oldValue;
  }

  public RowColumn[] getNewValue() {
    return newValue;
  }

  public abstract Optional<RowDeletedValue> asDeleted();

  public abstract Optional<RowUpdatedValue> asUpdated();

  public abstract Optional<RowInsertedValue> asInserted();

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RowChangedValue)) {
      return false;
    }

    final RowChangedValue other = (RowChangedValue) o;
    return Objects.equals(getType(), other.getType())
        && Arrays.deepEquals(getNewValue(), other.getNewValue())
        && Arrays.deepEquals(getOldValue(), other.getOldValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), Arrays.hashCode(getNewValue()), Arrays.hashCode(getOldValue()));
  }

  @Override
  public Optional<RowChangedValue> asRowChanged() {
    return Optional.of(this);
  }

  @Override
  public Optional<ResolvedValue> asResolved() {
    return Optional.empty();
  }

  @Override
  public Optional<DDLValue> asDDL() {
    return Optional.empty();
  }

  public enum Type {
    INSERT,
    UPDATE,
    DELETE
  }
}
