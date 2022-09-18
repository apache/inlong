/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wisecoders.dbschema.influxdb;

import com.wisecoders.dbschema.influxdb.resultSet.ArrayResultSet;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.Organization;
import com.influxdb.query.FluxColumn;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import java.sql.*;
import java.util.List;

/**
 * Copyright Wise Coders GmbH https://wisecoders.com
 * Driver is used in the DbSchema Database Designer https://dbschema.com
 * Free to be used by everyone.
 * Code modifications allowed only to GitHub repository https://github.com/wise-coders/influxdb-jdbc-driver
 */

public class InfluxMetaData implements DatabaseMetaData {

    private final InfluxConnection influxConnection;

    public InfluxMetaData(InfluxConnection connection) {
        this.influxConnection = connection;
    }

    @Override
    public ResultSet getSchemas() {
        ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[]{"TABLE_SCHEMA", "TABLE_CAT"});
        for (FluxTable fluxTable : influxConnection.client.getQueryApi().query("buckets()")) {
            for (FluxRecord fluxRecord : fluxTable.getRecords()) {
                result.addRow(new String[]{String.valueOf(fluxRecord.getValueByKey("name")), null});
            }
        }
        return result;
    }


    @Override
    public ResultSet getCatalogs() {
        ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[]{"TABLE_SCHEMA", "TABLE_CAT"});
        for (Organization organization : influxConnection.client.getOrganizationsApi().findOrganizations()) {
            result.addRow(new String[]{String.valueOf(organization.getName())});
        }
        return result;
    }


    /**
     * @see java.sql.DatabaseMetaData#getTables(java.lang.String, java.lang.String, java.lang.String,
     * java.lang.String[])
     */
    public ResultSet getTables(String catalogName, String schemaName, String tableNamePattern, String[] types) {
        final ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(
                new String[]{"TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                        "TYPE_SCHEMA", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"});

        result.setColumnNames(new String[]{"TABLE_CAT"});
        for (FluxTable fluxTable : influxConnection.client.getQueryApi().query(
                "import \"influxdata/influxdb/schema\"\n\n  schema.measurements(bucket: \"" + schemaName + "\")")) {
            for (FluxRecord fluxRecord : fluxTable.getRecords()) {
                result.addRow(createTableRow(catalogName, String.valueOf(fluxRecord.getValueByKey("_value")), null));
            }
        }
        return result;
    }

    private String[] createTableRow(String catalogName, String tableName, String comment) {
        String[] data = new String[10];
        data[0] = catalogName; // TABLE_CAT
        data[1] = ""; // TABLE_SCHEMA
        data[2] = tableName; // TABLE_NAME
        data[3] = "TABLE"; // TABLE_TYPE
        data[4] = comment; // REMARKS
        data[5] = ""; // TYPE_CAT
        data[6] = ""; // TYPE_SCHEM
        data[7] = ""; // TYPE_NAME
        data[8] = ""; // SELF_REFERENCING_COL_NAME
        data[9] = ""; // REF_GENERATION
        return data;
    }

    /**
     * @see java.sql.DatabaseMetaData#getColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getColumns(String catalogName, String schemaName, String tableName, String columnNamePattern) {

        final ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[]{"TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "OPTIONS"});

        String fluxQuery = "import \"influxdata/influxdb/schema\"\n" +
                "schema.measurementFieldKeys(bucket: \"" + schemaName + "\", measurement: \"" + tableName
                + "\", start: " + influxConnection.startDays + "d )";
        for (FluxTable columnNames : influxConnection.client.getQueryApi().query(fluxQuery)) {
            for (FluxRecord columnNamesRecord : columnNames.getRecords()) {
                String columnName = String.valueOf(columnNamesRecord.getValueByKey("_value"));
                if (!columnName.startsWith("_")) {
                    String columnDataType =
                            getColumnDataType(influxConnection.client.getQueryApi(), schemaName, tableName, columnName);
                    addColumn(catalogName, tableName, result, columnName, columnDataType);
                }
            }
        }
        String fluxQuery2 = "import \"influxdata/influxdb/schema\"\n" +
                "schema.measurementTagKeys(bucket: \"" + schemaName + "\", measurement: \"" + tableName + "\" )";
        for (FluxTable columnNames : influxConnection.client.getQueryApi().query(fluxQuery2)) {
            for (FluxRecord columnNamesRecord : columnNames.getRecords()) {
                String columnName = String.valueOf(columnNamesRecord.getValueByKey("_value"));
                if (!columnName.startsWith("_")) {
                    addColumn(catalogName, tableName, result, columnName, "string");
                }
            }
        }
        return result;
    }

    private void addColumn(String catalogName, String tableName, ArrayResultSet result, String columnName,
                           String columnDataType) {
        result.addRow(new String[]{
                catalogName, // "TABLE_CAT",
                null, // "TABLE_SCHEMA",
                tableName, // "TABLE_NAME", (i.e. Cassandra Collection Name)
                columnName, // "COLUMN_NAME",
                "4", // "DATA_TYPE",
                columnDataType,
                // "TYPE_NAME", -- I LET THIS INTENTIONALLY TO USE .toString() BECAUSE OF USER DEFINED TYPES.
                "800", // "COLUMN_SIZE",
                "0", // "BUFFER_LENGTH", (not used)
                "0", // "DECIMAL_DIGITS",
                "10", // "NUM_PREC_RADIX",
                "0", // "NULLABLE", // I RETREIVE HERE IF IS FROZEN ( MANDATORY ) OR NOT ( NULLABLE )
                "", // "REMARKS",
                "", // "COLUMN_DEF",
                "0", // "SQL_DATA_TYPE", (not used)
                "0", // "SQL_DATETIME_SUB", (not used)
                "800", // "CHAR_OCTET_LENGTH",
                "1", // "ORDINAL_POSITION",
                "NO", // "IS_NULLABLE",
                null, // "SCOPE_CATLOG", (not a REF type)
                null, // "SCOPE_SCHEMA", (not a REF type)
                null, // "SCOPE_TABLE", (not a REF type)
                null, // "SOURCE_DATA_TYPE", (not a DISTINCT or REF type)
                "NO", // "IS_AUTOINCREMENT" (can be auto-generated, but can also be specified)
                null // TABLE_OPTIONS
        });
    }


    @Override
    public ResultSet getPrimaryKeys(String catalogName, String schemaName, String tableName) {
        final ArrayResultSet result = new ArrayResultSet();
        String fluxQuery2 = "import \"influxdata/influxdb/schema\"\n" +
                "schema.measurementTagKeys(bucket: \"" + schemaName + "\", measurement: \"" + tableName + "\" )";
        int seq = 0;
        for (FluxTable columnNames : influxConnection.client.getQueryApi().query(fluxQuery2)) {
            for (FluxRecord columnNamesRecord : columnNames.getRecords()) {
                String columnName = String.valueOf(columnNamesRecord.getValueByKey("_value"));
                if (!columnName.startsWith("_")) {
                    result.addRow(new String[]{
                            catalogName, // "TABLE_CAT",
                            schemaName, // "TABLE_SCHEMA",
                            tableName, // "TABLE_NAME", (i.e. Cassandra Collection Name)
                            columnName, // "COLUMN_NAME",
                            "" + (++seq),
                            "PK_" + tableName
                    });
                }
            }
        }
        return result;
    }

    public static String getColumnDataType(QueryApi queryApi, String schemaName, String measurement,
                                           String columnName) {
        String fluxToGetDataType = "from(bucket: \"" + schemaName + "\") \n" +
                "|> range(start: -40d) \n" +
                "|> filter(fn: (r) => r._measurement == \"" + measurement + "\") \n" +
                "|> filter(fn: (r) => r._field == \"" + columnName + "\") \n" +
                "|> keep(columns: [\"_value\"]) \n" +
                "|> last() \n";

        final List<FluxTable> tableList = queryApi.query(fluxToGetDataType);
        if (tableList.size() > 0) {
            FluxTable columnTypeTable = tableList.get(0);

            for (FluxColumn columnType : columnTypeTable.getColumns()) {
                if (columnType.getLabel().equals("_value")) {
                    return columnType.getDataType();
                }
            }
        }
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[]{"TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION"});
        return result;
    }

    /*
    @Override
    public ResultSet getIndexInfo(String catalogName, String schemaName, String tableName, boolean unique, boolean
    approximate) {
        ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[]{"TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION"});

        String fluxQuery = "import \"influxdata/influxdb/schema\"\n\n  schema.measurementTagKeys(bucket: \"" +
        schemaName + "\", measurement: \"" + tableName + "\")";
        for (FluxTable fluxTable : influxConnection.client.getQueryApi().query( fluxQuery)) {
            for (FluxRecord fluxRecord : fluxTable.getRecords()) {
                String columnName = String.valueOf( fluxRecord.getValueByKey("_value"));
                if ( !columnName.startsWith("_") ) {
                    result.addRow(new String[]{catalogName, // "TABLE_CAT",
                            null, // "TABLE_SCHEMA",
                            fluxTable.toString(), // "TABLE_NAME", (measurement)
                            "YES", // "NON-UNIQUE",
                            columnName, // "INDEX QUALIFIER",
                            columnName, // "INDEX_NAME",
                            "0", // "TYPE",
                            "0", // "ORDINAL_POSITION"
                            columnName, // "COLUMN_NAME",
                            "A", // "ASC_OR_DESC",
                            "0", // "CARDINALITY",
                            "0", // "PAGES",
                            "" // "FILTER_CONDITION",
                    });
                }
            }
        }
        return result;
    }*/


    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    @Override
    public String getURL() throws SQLException {
        return null;
    }

    @Override
    public String getUserName() throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return null;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return null;
    }

    @Override
    public String getDriverName() throws SQLException {
        return null;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return null;
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return null;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return null;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return null;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        return null;
    }


    @Override
    public ResultSet getTableTypes() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
