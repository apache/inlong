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
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseAppendExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAppendExecutor.class);

    private final String insertSql;

    private final FormatInfo[] formatInfos;

    private final int flushIntervalSecond;

    private final int maxRetries;

    private transient ClickHousePreparedStatement stmt;

    private transient List<Row> batch = new ArrayList<>();

    private transient ExecuteBatchService executeBatchService;

    public ClickHouseAppendExecutor(
            String insertSql,
            FormatInfo[] formatInfos,
            ClickHouseSinkInfo clickHouseSinkInfo) {
        this.insertSql = insertSql;
        this.formatInfos = formatInfos;
        this.flushIntervalSecond = clickHouseSinkInfo.getFlushInterval();
        this.maxRetries = clickHouseSinkInfo.getWriteMaxRetryTimes();
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        if (batch == null) {
            batch = new ArrayList<>();
        }
        stmt = (ClickHousePreparedStatement) connection.prepareStatement(insertSql);
        executeBatchService = new ExecuteBatchService();
        executeBatchService.startAsync();
    }

    @Override
    public synchronized void addBatch(Tuple2<Boolean, Row> record) {
        batch.add(record.f1);
    }

    @Override
    public synchronized void executeBatch() throws IOException {
        if (executeBatchService.isRunning()) {
            this.notifyAll();
        } else {
            throw new IOException("executor unexpectedly terminated", executeBatchService.failureCause());
        }
    }

    @Override
    public void closeStatement() throws SQLException {
        if (executeBatchService != null) {
            executeBatchService.stopAsync().awaitTerminated();
        }

        if (stmt != null) {
            stmt.close();
            stmt = null;
        }
    }

    private class ExecuteBatchService extends AbstractExecutionThreadService {

        private ExecuteBatchService() {
        }

        protected void run() throws Exception {
            while (isRunning()) {
                synchronized (ClickHouseAppendExecutor.this) {
                    ClickHouseAppendExecutor.this.wait(flushIntervalSecond * 1000L);
                    if (!batch.isEmpty()) {
                        for (Row row : batch) {
                            ClickHouseRowConverter.setRow(stmt, formatInfos, row);
                            stmt.addBatch();
                        }
                        attemptExecuteBatch();
                    }
                }
            }
        }

        private void attemptExecuteBatch() throws IOException {
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
