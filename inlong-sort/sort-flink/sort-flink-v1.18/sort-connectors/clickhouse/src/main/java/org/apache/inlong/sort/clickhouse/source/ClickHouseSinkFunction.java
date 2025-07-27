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

package org.apache.inlong.sort.clickhouse.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * ClickHouse sink function to write RowData into ClickHouse via JDBC.
 */
public class ClickHouseSinkFunction extends RichSinkFunction<RowData> {

    private static final int BATCH_SIZE = 500;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;
    private final RowType rowType;

    private transient Connection connection;
    private transient PreparedStatement statement;

    private int batchCount = 0;

    public ClickHouseSinkFunction(String jdbcUrl,
                                  String username,
                                  String password,
                                  String tableName,
                                  RowType rowType) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
        this.rowType = rowType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);

        String sql = buildInsertSQL(tableName, rowType);
        statement = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        if (rowData.getRowKind() != RowKind.INSERT && rowData.getRowKind() != RowKind.UPDATE_AFTER) {
            return;
        }

        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            LogicalType fieldType = rowType.getTypeAt(i);
            if (rowData.isNullAt(i)) {
                statement.setObject(i + 1, null);
                continue;
            }

            switch (fieldType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    statement.setString(i + 1, rowData.getString(i).toString());
                    break;
                case BOOLEAN:
                    statement.setBoolean(i + 1, rowData.getBoolean(i));
                    break;
                case TINYINT:
                    statement.setByte(i + 1, rowData.getByte(i));
                    break;
                case SMALLINT:
                    statement.setShort(i + 1, rowData.getShort(i));
                    break;
                case INTEGER:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                    statement.setInt(i + 1, rowData.getInt(i));
                    break;
                case BIGINT:
                case INTERVAL_DAY_TIME:
                    statement.setLong(i + 1, rowData.getLong(i));
                    break;
                case FLOAT:
                    statement.setFloat(i + 1, rowData.getFloat(i));
                    break;
                case DOUBLE:
                    statement.setDouble(i + 1, rowData.getDouble(i));
                    break;
                case DECIMAL:
                    int precision = ((DecimalType) fieldType).getPrecision();
                    int scale = ((DecimalType) fieldType).getScale();
                    DecimalData decimalData = rowData.getDecimal(i, precision, scale);
                    statement.setBigDecimal(i + 1, decimalData.toBigDecimal());
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    int tsPrecision = ((TimestampType) fieldType).getPrecision();
                    Timestamp timestamp = rowData.getTimestamp(i, tsPrecision).toTimestamp();
                    statement.setTimestamp(i + 1, timestamp);
                    break;
                case BINARY:
                case VARBINARY:
                    statement.setBytes(i + 1, rowData.getBinary(i));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + fieldType);
            }
        }

        statement.addBatch();
        batchCount++;

        if (batchCount >= BATCH_SIZE) {
            statement.executeBatch();
            connection.commit();
            batchCount = 0;
        }
    }

    public void flush() throws SQLException {
        if (statement != null) {
            statement.executeBatch();
            connection.commit();
        }
    }

    @Override
    public void close() throws Exception {
        if (batchCount > 0) {
            statement.executeBatch();
            connection.commit();
            batchCount = 0;
        }
        super.close();
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private String buildInsertSQL(String tableName, RowType rowType) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName).append(" (");

        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            sb.append(fieldNames.get(i));
            if (i < fieldNames.size() - 1) {
                sb.append(", ");
            }
        }

        sb.append(") VALUES (");
        for (int i = 0; i < fieldNames.size(); i++) {
            sb.append("?");
            if (i < fieldNames.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
