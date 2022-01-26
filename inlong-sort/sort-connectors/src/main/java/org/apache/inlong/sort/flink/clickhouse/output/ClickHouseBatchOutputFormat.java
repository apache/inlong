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

package org.apache.inlong.sort.flink.clickhouse.output;

import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.flink.clickhouse.ClickHouseConnectionProvider;
import org.apache.inlong.sort.flink.clickhouse.executor.ClickHouseExecutor;
import org.apache.inlong.sort.flink.clickhouse.executor.ClickHouseExecutorFactory;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.sql.SQLException;

public class ClickHouseBatchOutputFormat extends AbstractClickHouseOutputFormat {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchOutputFormat.class);

    private final ClickHouseConnectionProvider connectionProvider;

    private final String[] fieldNames;

    private final FormatInfo[] formatInfos;

    private final ClickHouseSinkInfo clickHouseSinkInfo;

    private boolean closed = false;

    private transient ClickHouseExecutor executor;

    private transient int batchCount = 0;

    public ClickHouseBatchOutputFormat(
            @Nonnull String[] fieldNames,
            @Nonnull FormatInfo[] formatInfos,
            @Nonnull ClickHouseSinkInfo clickHouseSinkInfo) {
        this.connectionProvider = new ClickHouseConnectionProvider(clickHouseSinkInfo);
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.formatInfos = Preconditions.checkNotNull(formatInfos);
        this.clickHouseSinkInfo = Preconditions.checkNotNull(clickHouseSinkInfo);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            ClickHouseConnection connection = connectionProvider.getConnection();
            executor = ClickHouseExecutorFactory.generateClickHouseExecutor(
                    clickHouseSinkInfo.getTableName(), fieldNames, formatInfos, clickHouseSinkInfo);
            executor.prepareStatement(connection);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeRecord(Tuple2<Boolean, Row> record) throws IOException {
        addBatch(record);
        batchCount++;
        if (batchCount >= clickHouseSinkInfo.getFlushRecordNumber()) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (batchCount > 0) {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.warn("Writing records to ClickHouse failed.", e);
                }
            }
            closeConnection();
        }
    }

    @Override
    public void flush() throws IOException {
        executor.executeBatch();
        batchCount = 0;
    }

    private void addBatch(Tuple2<Boolean, Row> record) {
        executor.addBatch(record);
    }

    private void closeConnection() {
        try {
            executor.closeStatement();
            connectionProvider.closeConnections();
        } catch (SQLException se) {
            LOG.warn("ClickHouse connection could not be closed: {}", se.getMessage());
        }
    }
}

