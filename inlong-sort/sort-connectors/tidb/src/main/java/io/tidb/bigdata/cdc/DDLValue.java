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

import io.tidb.bigdata.cdc.json.jackson.JacksonFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/*
 * Value type for DDL event
 */
public final class DDLValue implements Value {

  private final String query;
  private final Type type;

  public DDLValue(final String query, final int type) {
    this.query = query;
    this.type = Type.findByCode(type);
  }

  public String getQuery() {
    return query;
  }

  public Type getType() {
    return type;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof DDLValue)) {
      return false;
    }
    final DDLValue other = (DDLValue) o;
    return Objects.equals(query, other.query) && Objects.equals(type, other.type);
  }

  @Override
  public Optional<RowChangedValue> asRowChanged() {
    return Optional.empty();
  }

  @Override
  public Optional<ResolvedValue> asResolved() {
    return Optional.empty();
  }

  @Override
  public Optional<DDLValue> asDDL() {
    return Optional.of(this);
  }

  @Override
  public String toJson(JacksonFactory factory) {
    return factory.toJson(
        factory.createObject().put("q", query).put("t", type.code()));
  }

  public enum Type {
    CREATE_SCHEMA(1),
    DROP_SCHEMA(2),
    CREATE_TABLE(3),
    DROP_TABLE(4),
    ADD_COLUMN(5),
    DROP_COLUMN(6),
    ADD_INDEX(7),
    DROP_INDEX(8),
    ADD_FOREIGN_KEY(9),
    DROP_FOREIGN_KEY(10),
    TRUNCATE_TABLE(11),
    MODIFY_COLUMN(12),
    REBASE_AUTO_ID(13),
    RENAME_TABLE(14),
    SET_DEFAULT_VALUE(15),
    SHARD_ROWID(16),
    MODIFY_TABLE_COMMENT(17),
    RENAME_INDEX(18),
    ADD_TABLE_PARTITION(19),
    DROP_TABLE_PARTITION(20),
    CREATE_VIEW(21),
    MODIFY_TABLE_CHARSET_AND_COLLATE(22),
    TRUNCATE_TABLE_PARTITION(23),
    DROP_VIEW(24),
    RECOVER_TABLE(25),
    MODIFY_SCHEMA_CHARSET_AND_COLLATE(26),
    LOCK_TABLE(27),
    UNLOCK_TABLE(28),
    REPAIR_TABLE(29),
    SET_TIFLASH_REPLICA(30),
    UPDATE_TIFLASH_REPLICA_STATUS(31),
    ADD_PRIMARY_KEY(32),
    DROP_PRIMARY_KEY(33),
    CREATE_SEQUENCE(34),
    ALTER_SEQUENCE(35),
    DROP_SEQUENCE(36);

    private static final Map<Integer, Type> byId = new HashMap<>();

    static {
      for (final Type t : Type.values()) {
        if (byId.containsKey(t.code())) {
          // we use the first definition for those types sharing the same code
          continue;
        }
        byId.put(t.code(), t);
      }
    }

    private final int code;

    Type(final int code) {
      this.code = code;
    }

    private static Type findByCode(final int code) {
      final Type type = byId.get(code);
      if (type == null) {
        throw new IllegalArgumentException("Unknown ddl type code: " + code);
      }
      return type;
    }

    public int code() {
      return code;
    }
  }
}
