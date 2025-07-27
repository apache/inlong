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

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.sql.*;

public class ClickHouseRowDataInputFormat extends RichInputFormat<RowData, GenericInputSplit> {

    private final ReadableConfig config;
    private final RowType rowType;

    private transient Connection connection;
    private transient ResultSet resultSet;
    private transient PreparedStatement statement;

    public ClickHouseRowDataInputFormat(ReadableConfig config, RowType rowType) {
        this.config = config;
        this.rowType = rowType;
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        try {
            String jdbcUrl = config.get(ClickHouseOptions.URL);
            String user = config.get(ClickHouseOptions.USERNAME);
            String password = config.get(ClickHouseOptions.PASSWORD);
            String query = config.get(ClickHouseOptions.QUERY);

            connection = DriverManager.getConnection(jdbcUrl, user, password);
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            throw new IOException("Error connecting to ClickHouse", e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        try {
            return !resultSet.next();
        } catch (SQLException e) {
            throw new IOException("Error reading from ClickHouse", e);
        }
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        try {
            GenericRowData rowData = new GenericRowData(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                rowData.setField(i, resultSet.getObject(i + 1)); // 1-based indexing
            }
            return rowData;
        } catch (SQLException e) {
            throw new IOException("Error converting row", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (resultSet != null) resultSet.close();
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            throw new IOException("Error closing ClickHouse connection", e);
        }
    }

    @Override
    public GenericInputSplit[] createInputSplits(int minNumSplits) {
        // No parallel read for now
        return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] genericInputSplits) {
        return null;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return null;
    }
}
