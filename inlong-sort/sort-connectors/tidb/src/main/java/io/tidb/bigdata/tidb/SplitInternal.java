/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Objects;
import org.tikv.common.meta.TiTimestamp;

public final class SplitInternal implements Serializable {

  private final TableHandleInternal table;
  private final String startKey;
  private final String endKey;
  private final TiTimestamp timestamp;

  public SplitInternal(
      TableHandleInternal table,
      String startKey,
      String endKey,
      TiTimestamp timestamp) {
    this.table = requireNonNull(table, "table is null");
    this.startKey = requireNonNull(startKey, "startKey is null");
    this.endKey = requireNonNull(endKey, "endKey is null");
    this.timestamp = requireNonNull(timestamp, "timestamp is null");
  }

  public SplitInternal(
      TableHandleInternal table,
      Base64KeyRange range,
      TiTimestamp timestamp) {
    this(table, range.getStartKey(), range.getEndKey(), timestamp);
  }

  public TableHandleInternal getTable() {
    return table;
  }

  public String getStartKey() {
    return startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  public TiTimestamp getTimestamp() {
    return timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, startKey, endKey);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    SplitInternal other = (SplitInternal) obj;
    return Objects.equals(this.table, other.table)
        && Objects.equals(this.startKey, other.startKey)
        && Objects.equals(this.endKey, other.endKey)
        && Objects.equals(this.timestamp, other.timestamp);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("startKey", startKey)
        .add("endKey", endKey)
        .add("timestamp", timestamp)
        .toString();
  }
}