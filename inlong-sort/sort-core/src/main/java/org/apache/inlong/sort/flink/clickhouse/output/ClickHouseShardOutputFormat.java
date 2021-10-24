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
import org.apache.inlong.sort.flink.clickhouse.partitioner.ClickHousePartitioner;
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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseShardOutputFormat extends AbstractClickHouseOutputFormat {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardOutputFormat.class);

    private static final Pattern PATTERN = Pattern.compile("Distributed\\((?<cluster>[a-zA-Z_][0-9a-zA-Z_]*),"
            + "\\s*(?<database>[a-zA-Z_][0-9a-zA-Z_]*),\\s*(?<table>[a-zA-Z_][0-9a-zA-Z_]*)");

    private final ClickHouseConnectionProvider connectionProvider;

    private final String[] fieldNames;

    private final FormatInfo[] formatInfos;

    private final ClickHousePartitioner partitioner;

    private final ClickHouseSinkInfo clickHouseSinkInfo;

    private final List<ClickHouseExecutor> shardExecutors = new ArrayList<>();

    private boolean closed = false;

    private transient int[] batchCounts;

    public ClickHouseShardOutputFormat(
            @Nonnull String[] fieldNames,
            @Nonnull FormatInfo[] formatInfos,
            @Nonnull ClickHousePartitioner partitioner,
            @Nonnull ClickHouseSinkInfo clickHouseSinkInfo) {
        this.connectionProvider = new ClickHouseConnectionProvider(clickHouseSinkInfo);
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.formatInfos = Preconditions.checkNotNull(formatInfos);
        this.partitioner = Preconditions.checkNotNull(partitioner);
        this.clickHouseSinkInfo = Preconditions.checkNotNull(clickHouseSinkInfo);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            initializeExecutors();
        } catch (Exception e) {
           throw new IOException(e);
        }
    }

    @Override
    public void writeRecord(Tuple2<Boolean, Row> recordAndMode) throws IOException {
        boolean rowMode = recordAndMode.f0;
        if (rowMode) {
            writeRecordToOneExecutor(recordAndMode);
        } else {
            writeRecordToAllExecutors(recordAndMode);
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            flush();
            closeConnection();
        }
    }

    public void flush() throws IOException {
        for (int i = 0; i < shardExecutors.size(); i++) {
            flush(i);
        }
    }

    public void flush(int index) throws IOException {
        shardExecutors.get(index).executeBatch();
        batchCounts[index] = 0;
    }

    private void initializeExecutors() throws Exception {
        String engine = connectionProvider.queryTableEngine(
                clickHouseSinkInfo.getDatabaseName(), clickHouseSinkInfo.getTableName());
        Matcher matcher = PATTERN.matcher(engine);
        List<ClickHouseConnection> shardConnections;
        String remoteTable;

        if (matcher.find()) {
            String remoteCluster = matcher.group("cluster");
            String remoteDatabase = matcher.group("database");
            remoteTable = matcher.group("table");
            shardConnections = connectionProvider.getShardConnections(remoteCluster, remoteDatabase);
            batchCounts = new int[shardConnections.size()];
        } else {
            throw new IOException("table `" + clickHouseSinkInfo.getDatabaseName() + "`.`"
                    + clickHouseSinkInfo.getTableName() + "` is not a Distributed table");
        }

        for (ClickHouseConnection shardConnection : shardConnections) {
            ClickHouseExecutor clickHouseExecutor = ClickHouseExecutorFactory.generateClickHouseExecutor(
                    remoteTable, fieldNames, formatInfos, clickHouseSinkInfo);
            clickHouseExecutor.prepareStatement(shardConnection);
            this.shardExecutors.add(clickHouseExecutor);
        }
    }

    private void writeRecordToOneExecutor(Tuple2<Boolean, Row> record) throws IOException {
        int selected = partitioner.select(record, shardExecutors.size());
        shardExecutors.get(selected).addBatch(record);
        batchCounts[selected] = batchCounts[selected] + 1;
        if (batchCounts[selected] >= clickHouseSinkInfo.getFlushRecordNumber()) {
            flush(selected);
        }
    }

    private void writeRecordToAllExecutors(Tuple2<Boolean, Row> record) throws IOException {
        for (int i = 0; i < shardExecutors.size(); i++) {
            shardExecutors.get(i).addBatch(record);
            batchCounts[i] = batchCounts[i] + 1;
            if (batchCounts[i] >= clickHouseSinkInfo.getFlushRecordNumber()) {
                flush(i);
            }
        }
    }

    private void closeConnection() {
        try {
            for (ClickHouseExecutor shardExecutor : shardExecutors) {
                shardExecutor.closeStatement();
            }
            connectionProvider.closeConnections();
        } catch (SQLException se) {
            LOG.warn("ClickHouse connection could not be closed!", se);
        }
    }
}
