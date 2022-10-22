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

import com.google.common.base.Joiner;
import java.io.Serializable;
import java.util.Objects;

public class TableHandleInternal implements Serializable {

  private final String connectorId;
  private final String schemaName;
  private final String tableName;

  public TableHandleInternal(
      String connectorId,
      String schemaName,
      String tableName) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.schemaName = requireNonNull(schemaName, "schemaName is null");
    this.tableName = requireNonNull(tableName, "tableName is null");
  }

  public String getSchemaTableName() {
    return Joiner.on(".").join(getSchemaName(), getTableName());
  }

  public String getConnectorId() {
    return connectorId;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectorId, schemaName, tableName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    TableHandleInternal other = (TableHandleInternal) obj;
    return Objects.equals(this.connectorId, other.connectorId)
        && Objects.equals(this.schemaName, other.schemaName)
        && Objects.equals(this.tableName, other.tableName);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("connectorId", connectorId)
        .add("schema", schemaName)
        .add("table", tableName)
        .toString();
  }
}