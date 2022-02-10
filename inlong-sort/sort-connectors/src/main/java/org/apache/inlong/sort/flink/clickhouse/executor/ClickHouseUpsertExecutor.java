/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.flink.clickhouse.executor;

import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.flink.clickhouse.ClickHouseRowConverter;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.ClickHouseStatement;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClickHouseUpsertExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseUpsertExecutor.class);

    private final String insertSql;

    private final String updateSql;

    private final String deleteSql;

    private final FormatInfo[] formatInfos;

    private final int maxRetries;

    private final List<Row> insertBatch = new ArrayList<>();

    private final List<Row> deleteBatch = new ArrayList<>();

    private transient ClickHousePreparedStatement insertStmt;

    private transient ClickHousePreparedStatement updateStmt;

    private transient ClickHousePreparedStatement deleteStmt;

    public ClickHouseUpsertExecutor(
            String insertSql,
            String updateSql,
            String deleteSql,
            FormatInfo[] formatInfos,
            ClickHouseSinkInfo clickHouseSinkInfo) {
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.deleteSql = deleteSql;
        this.formatInfos = formatInfos;
        this.maxRetries = clickHouseSinkInfo.getWriteMaxRetryTimes();
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        insertStmt = (ClickHousePreparedStatement) connection.prepareStatement(insertSql);
        updateStmt = (ClickHousePreparedStatement) connection.prepareStatement(updateSql);
        deleteStmt = (ClickHousePreparedStatement) connection.prepareStatement(deleteSql);
    }

    @Override
    public synchronized void addBatch(Tuple2<Boolean, Row> record) {
        boolean insert = record.f0;
        if (insert) {
            insertBatch.add(record.f1);
        } else {
            deleteBatch.add(record.f1);
        }
    }

    @Override
    public synchronized void executeBatch() throws IOException {
        try {
            processBatch(insertStmt, insertBatch);
            processBatch(deleteStmt, deleteBatch);
        } catch (Exception exception) {
            throw new IOException("Flush data to clickhouse failed! " + exception);
        }
    }

    @Override
    public void closeStatement() throws SQLException {
        for (ClickHouseStatement stmt : Arrays.asList(insertStmt, updateStmt, deleteStmt)) {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    private void processBatch(ClickHousePreparedStatement stmt, List<Row> batch) throws Exception {
        if (!batch.isEmpty()) {
            for (Row row : batch) {
                ClickHouseRowConverter.setRow(stmt, formatInfos, row);
                stmt.addBatch();
            }

            for (int i = 1; i <= maxRetries; i++) {
                try {
                    stmt.executeBatch();
                    batch.clear();
                    break;
                } catch (SQLException e) {
                    LOG.error("ClickHouse executeBatch error, retry times = {}", i, e);
                    if (i >= maxRetries) {
                        throw new IOException(e);
                    }

                    try {
                        Thread.sleep((1000L * i));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", e);
                    }
                }
            }
        }
    }
}
