/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.sort.iceberg.flink.actions;

import org.apache.iceberg.actions.Action;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.inlong.sort.iceberg.flink.CompactTableProperties.COMPACT_INTERVAL;
import static org.apache.inlong.sort.iceberg.flink.CompactTableProperties.COMPACT_INTERVAL_DEFAULT;
import static org.apache.inlong.sort.iceberg.flink.CompactTableProperties.COMPACT_RESOUCE_POOL;
import static org.apache.inlong.sort.iceberg.flink.CompactTableProperties.COMPACT_RESOUCE_POOL_DEFAULT;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.AUTH_SECRET_ID;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.AUTH_SECRET_KEY;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.REWRITE_DB_NAME;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.REWRITE_TABLE_NAME;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.URL_DATA_SOURCE;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.URL_DATA_SOURCE_DEFAULT;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.URL_DEFAULT_DATABASE;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.URL_DEFAULT_DATABASE_DEFAULT;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.URL_ENDPOINT;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.URL_ENDPOINT_DEFAULT;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.URL_REGION;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.URL_REGION_DEFAULT;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.URL_TASK_TYPE;
import static org.apache.inlong.sort.iceberg.flink.actions.SyncRewriteDataFilesActionOption.URL_TASK_TYPE_DEFAULT;

/**
 * Do rewrite action with dlc Spark SQL.
 */
public class SyncRewriteDataFilesAction implements
        Action<SyncRewriteDataFilesAction, RewriteResult> {

    private static final Logger LOG = LoggerFactory.getLogger(SyncRewriteDataFilesAction.class);
    private static final String DLC_JDBC_CLASS = "com.tencent.cloud.dlc.jdbc.DlcDriver";

    private Map<String, String> options;
    private AtomicInteger snapshotCounter;

    private SyncRewriteDataFilesAction() {
        this.options = new HashMap<>();
        this.snapshotCounter = new AtomicInteger();
    }

    @Override
    public SyncRewriteDataFilesAction option(String name, String value) {
        this.options.put(name, value);
        return this;
    }

    @Override
    public SyncRewriteDataFilesAction options(Map<String, String> options) {
        this.options.putAll(options);
        return this;
    }

    @Override
    public RewriteResult execute() {
        if (!shouldExecute()) {
            return new RewriteResult("Skip This compact.");
        }

        Connection connection = buildConnection();
        if (connection == null) {
            LOG.error("Can't get DLC JDBC Connection");
            return new RewriteResult("fail.");
        }

        String dbName = options.get(REWRITE_DB_NAME);
        String tableName = options.get(REWRITE_TABLE_NAME);
        Preconditions.checkNotNull(dbName);
        Preconditions.checkNotNull(tableName);
        String wholeTableName = String.format("%s.%s", dbName, tableName);
        String rewriteTableSql =
                String.format(
                        "CALL `DataLakeCatalog`.`system`.rewrite_data_files"
                                + "(`table` => '%s')",
                        wholeTableName);
        try {
            Statement statement = connection.createStatement();
            boolean firstIsResultSet = statement.execute(rewriteTableSql);
            if (firstIsResultSet) {
                ResultSet rs = statement.getResultSet();
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                while (rs.next()) {
                    StringBuilder lineResult = new StringBuilder();
                    for (int i = 1; i <= columnsNumber; i++) {
                        if (i > 1) {
                            lineResult.append(",  ");
                        }
                        lineResult.append(rsmd.getColumnName(i) + ":" + rs.getString(i));
                    }
                    LOG.info("[Result:]{}", lineResult);
                }
            } else {
                LOG.info("[Result:]there has no output.");
            }
            statement.close();
            connection.close();
        } catch (SQLException e) {
            LOG.warn("[Result:]Execute rewrite sql err.", e);
            return new RewriteResult("fail.");
        }
        return new RewriteResult("success.");
    }

    public static SyncRewriteDataFilesAction instance(SyncRewriteDataFilesActionOption option) {
        return new SyncRewriteDataFilesAction().options(option.getProperties());
    }

    private boolean shouldExecute() {
        return snapshotCounter.incrementAndGet()
                % PropertyUtil.propertyAsInt(options, COMPACT_INTERVAL, COMPACT_INTERVAL_DEFAULT) == 0;
    }

    private Connection buildConnection() {
        StringBuilder builder = new StringBuilder();
        builder.append("jdbc:dlc:" + options.getOrDefault(URL_ENDPOINT, URL_ENDPOINT_DEFAULT));
        builder.append("?task_type=" + options.getOrDefault(URL_TASK_TYPE, URL_TASK_TYPE_DEFAULT));
        builder.append("&database_name=" + options.getOrDefault(URL_DEFAULT_DATABASE, URL_DEFAULT_DATABASE_DEFAULT));
        builder.append("&datasource_connection_name="
                + options.getOrDefault(URL_DATA_SOURCE, URL_DATA_SOURCE_DEFAULT));
        builder.append("&region=" + options.getOrDefault(URL_REGION, URL_REGION_DEFAULT));
        builder.append("&data_engine_name="
                + options.getOrDefault(COMPACT_RESOUCE_POOL, COMPACT_RESOUCE_POOL_DEFAULT));
        Connection connection = null;
        try {
            Class.forName(DLC_JDBC_CLASS);
            connection = DriverManager.getConnection(
                    builder.toString(),
                    options.get(AUTH_SECRET_ID),
                    options.get(AUTH_SECRET_KEY));
            // get meta data
            DatabaseMetaData metaData = connection.getMetaData();
            LOG.info("DLC product = {}.", metaData.getDatabaseProductName());
            LOG.info("DLC jdbc version = {}, ", metaData.getDriverMajorVersion(), metaData.getDriverMinorVersion());
        } catch (SQLException e) {
            LOG.error("Create connection err.Please check configuration. Request URL: {}.", builder);
        } catch (ClassNotFoundException e) {
            LOG.error("DLC JDBC Driver class not found.Please check classpath({}).",
                    System.getProperty("java.class.path"), e);
        }
        return connection;
    }
}
