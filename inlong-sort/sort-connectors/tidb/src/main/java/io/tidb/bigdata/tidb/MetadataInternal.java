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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class MetadataInternal {

  private final String connectorId;
  private final ClientSession session;

  public MetadataInternal(String connectorId, ClientSession session) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.session = requireNonNull(session, "session is null");
  }

  public List<String> listSchemaNames() {
    return session.getSchemaNames();
  }

  public Optional<TableHandleInternal> getTableHandle(String schemaName, String tableName) {
    return session.getTable(schemaName, tableName)
        .map(t -> new TableHandleInternal(connectorId, schemaName, tableName));
  }

  public Map<String, List<String>> listTables(Optional<String> schemaName) {
    return session.listTables(schemaName);
  }

  public Optional<List<ColumnHandleInternal>> getColumnHandles(TableHandleInternal tableHandle) {
    return session.getTableColumns(tableHandle);
  }

  public Optional<List<ColumnHandleInternal>> getColumnHandles(String schemaName,
      String tableName) {
    return session.getTableColumns(schemaName, tableName);
  }

  public boolean tableExists(String databaseName, String tableName) {
    return session.tableExists(databaseName, tableName);
  }

  public void createTable(String databaseName, String tableName, List<String> columnNames,
      List<String> columnTypes, List<String> primaryKeyColumns, List<String> uniqueKeyColumns,
      boolean ignoreExisting) {
    session.createTable(databaseName, tableName, columnNames, columnTypes, primaryKeyColumns,
        uniqueKeyColumns, ignoreExisting);
  }

  public void dropTable(String schemaName, String tableName, boolean ignoreIfNotExists) {
    session.dropTable(schemaName, tableName, ignoreIfNotExists);
  }

  public boolean databaseExists(String databaseName) {
    return session.databaseExists(databaseName);
  }

  public void createDatabase(String databaseName, boolean ignoreIfExists) {
    session.createDatabase(databaseName, ignoreIfExists);
  }

  public void dropDatabase(String schemaName, boolean ignoreIfNotExists) {
    session.dropDatabase(schemaName, ignoreIfNotExists);
  }

  public void renameTable(String oldDatabaseName, String newDatabaseName, String oldTableName,
      String newTableName) {
    session.renameTable(oldDatabaseName, newDatabaseName, oldTableName, newTableName);
  }

  public void addColumn(String databaseName, String tableName, String columnName,
      String columnType) {
    session.addColumn(databaseName, tableName, columnName, columnType);
  }

  public void renameColumn(String databaseName, String tableName, String oldName, String newName,
      String newType) {
    session.renameColumn(databaseName, tableName, oldName, newName, newType);
  }

  public void dropColumn(String databaseName, String tableName, String columnName) {
    session.dropColumn(databaseName, tableName, columnName);
  }

  public Connection getJdbcConnection() throws SQLException {
    return session.getJdbcConnection();
  }

  public List<String> getPrimaryKeyColumns(String databaseName, String tableName) {
    return session.getPrimaryKeyColumns(databaseName, tableName);
  }

  public List<String> getUniqueKeyColumns(String databaseName, String tableName) {
    return session.getUniqueKeyColumns(databaseName, tableName);
  }

  public String getConnectorId() {
    return connectorId;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("connectorId", connectorId)
        .add("session", session)
        .toString();
  }
}
