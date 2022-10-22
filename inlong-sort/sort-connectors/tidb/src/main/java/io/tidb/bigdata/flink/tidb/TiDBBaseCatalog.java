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

package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.DATABASE_NAME;
import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TiDBBaseCatalog extends AbstractCatalog {

  static final Logger LOG = LoggerFactory.getLogger(TiDBBaseCatalog.class);

  public static final String TIDB_CATALOG_LOAD_MODE = "tidb.catalog.load-mode";
  public static final String TIDB_CATALOG_LOAD_MODE_DEFAULT = CatalogLoadMode.EAGER.name();

  public static final String DEFAULT_DATABASE = "default";

  public static final String DEFAULT_NAME = "tidb";

  private final Map<String, String> properties;

  private final CatalogLoadMode catalogLoadMode;

  private Optional<ClientSession> clientSession = Optional.empty();

  public TiDBBaseCatalog(String name, String defaultDatabase, Map<String, String> properties) {
    super(name, defaultDatabase);
    this.properties = Preconditions.checkNotNull(properties);
    this.catalogLoadMode = CatalogLoadMode.fromString(
        properties.getOrDefault(TIDB_CATALOG_LOAD_MODE, TIDB_CATALOG_LOAD_MODE_DEFAULT));
  }

  public TiDBBaseCatalog(String name, Map<String, String> properties) {
    this(name, DEFAULT_DATABASE, properties);
  }

  public TiDBBaseCatalog(Map<String, String> properties) {
    this(DEFAULT_NAME, DEFAULT_DATABASE, properties);
  }

  private void initClientSession() {
    if (!clientSession.isPresent()) {
      try {
        LOG.info("Init client session");
        clientSession = Optional.of(ClientSession.create(new ClientConfig(properties)));
      } catch (Exception e) {
        throw new CatalogException("Can not init client session", e);
      }
    }
  }

  @Override
  public synchronized void open() throws CatalogException {
    if (catalogLoadMode == CatalogLoadMode.EAGER) {
      initClientSession();
    } else {
      LOG.info("We do nothing because tidb catalog use lazy open");
    }
  }

  @Override
  public synchronized void close() throws CatalogException {
    clientSession.ifPresent(session -> {
      try {
        session.close();
      } catch (Exception e) {
        LOG.warn("Can not close clientSession", e);
      }
      clientSession = Optional.empty();
    });
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    return getClientSession().getSchemaNames();
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }
    return new CatalogDatabaseImpl(ImmutableMap.of(), "");
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    return getClientSession().databaseExists(databaseName);
  }

  @Override
  public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  public void createDatabase(String databaseName, boolean ignoreIfExists) {
    getClientSession().createDatabase(databaseName, ignoreIfExists);
  }

  @Override
  public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    getClientSession().dropDatabase(databaseName, ignoreIfNotExists);
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    Preconditions.checkNotNull(databaseName);
    return getClientSession().getTableNames(databaseName);
  }

  @Override
  public List<String> listViews(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    Preconditions.checkNotNull(tablePath);
    return getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
  }

  public CatalogBaseTable getTable(String databaseName, String tableName)
      throws TableNotExistException, CatalogException {
    Map<String, String> properties = new HashMap<>(this.properties);
    properties.put(DATABASE_NAME.key(), databaseName);
    properties.put(TABLE_NAME.key(), tableName);
    return new CatalogTableImpl(getTableSchema(databaseName, tableName), properties, "");
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    Preconditions.checkNotNull(tablePath);
    return tableExists(tablePath.getDatabaseName(), tablePath.getObjectName());
  }

  public boolean tableExists(String databaseName, String tableName) throws CatalogException {
    return getClientSession().tableExists(databaseName, tableName);
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    Preconditions.checkNotNull(tablePath);
    dropTable(tablePath.getDatabaseName(), tablePath.getObjectName(), ignoreIfNotExists);
  }

  public void dropTable(String databaseName, String tableName, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    getClientSession().dropTable(databaseName, tableName, ignoreIfNotExists);
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath,
      List<Expression> filters)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
      CatalogPartition partition, boolean ignoreIfExists)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
      PartitionAlreadyExistsException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
      boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listFunctions(String dbName)
      throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath)
      throws FunctionNotExistException, CatalogException {
    throw new FunctionNotExistException(getName(), functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createFunction(ObjectPath functionPath, CatalogFunction function,
      boolean ignoreIfExists)
      throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction,
      boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return null;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return null;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
    return null;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
    return null;
  }

  @Override
  public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
      boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableColumnStatistics(ObjectPath tablePath,
      CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException, TablePartitionedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionColumnStatistics(ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  public TableSchema getTableSchema(String databaseName, String tableName) {
    return getClientSession()
        .getTableMust(databaseName, tableName)
        .getColumns()
        .stream()
        .reduce(TableSchema.builder(), (builder, c) -> builder.field(c.getName(),
            TypeUtils.getFlinkType(c.getType())), (builder1, builder2) -> null).build();
  }

  @Override
  public abstract Optional<Factory> getFactory();

  public void sqlUpdate(String... sqls) {
    getClientSession().sqlUpdate(sqls);
  }

  private ClientSession getClientSession() {
    if (clientSession.isPresent()) {
      return clientSession.get();
    }
    if (catalogLoadMode == CatalogLoadMode.EAGER) {
      throw new IllegalStateException("TiDB catalog is not opened or has been closed ");
    }
    initClientSession();
    return clientSession.get();
  }

  public enum CatalogLoadMode {
    LAZY, EAGER;

    public static CatalogLoadMode fromString(String s) {
      return Arrays.stream(values()).filter(mode -> mode.name().equalsIgnoreCase(s)).findFirst()
          .orElseThrow(() -> new IllegalArgumentException(
              "Catalog load mode must be one of: " + Arrays.toString(values())));
    }
  }
}
