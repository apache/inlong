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
import java.util.Objects;

/*
 * TiCDC event type
 */
public final class Event {

  static final JacksonFactory defaultJacksonFactory = JacksonFactory.create();
  private final Key key;
  private final Value value;

  public Event(final Key key, final Value value) {
    this.key = key;
    this.value = value;
  }

  public Key getKey() {
    return key;
  }

  public Value getValue() {
    return value;
  }

  public Key.Type getType() {
    return key.getType();
  }

  public long getTimestamp() {
    return key.getTimestamp();
  }

  public long getTs() {
    return key.getTs();
  }

  public String getSchema() {
    return key.getSchema();
  }

  public String getTable() {
    return key.getTable();
  }

  public long getRowId() {
    return key.getRowId();
  }

  public long getPartition() {
    return key.getPartition();
  }

  public RowChangedValue asRowChanged() {
    return (RowChangedValue) value;
  }

  public DDLValue asDDL() {
    return (DDLValue) value;
  }

  public ResolvedValue asResolved() {
    return (ResolvedValue) value;
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Event)) {
      return false;
    }
    final Event o = (Event) other;
    return Objects.equals(key, o.key) && Objects.equals(value, o.value);
  }
}
