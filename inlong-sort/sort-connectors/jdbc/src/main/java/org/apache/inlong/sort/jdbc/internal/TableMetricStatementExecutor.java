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

package org.apache.inlong.sort.jdbc.internal;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.jdbc.statement.StatementFactory;
import org.apache.flink.table.data.RowData;
import org.apache.inlong.sort.base.dirty.DirtySinkHelper;
import org.apache.inlong.sort.base.dirty.DirtyType;
import org.apache.inlong.sort.base.metric.SinkMetricData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link JdbcBatchStatementExecutor} that simply adds the records into batches of {@link
 * java.sql.PreparedStatement} and doesn't buffer records in memory. Only used in Table/SQL API.
 */
public final class TableMetricStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(TableMetricStatementExecutor.class);
    private final StatementFactory stmtFactory;
    private final JdbcRowConverter converter;
    private final List<RowData> batch;
    private final DirtySinkHelper<Object> dirtySinkHelper;
    private final SinkMetricData sinkMetricData;
    private final AtomicInteger counter = new AtomicInteger();
    private transient FieldNamedPreparedStatement st;
    private boolean multipleSink;

    public TableMetricStatementExecutor(StatementFactory stmtFactory, JdbcRowConverter converter,
            DirtySinkHelper<Object> dirtySinkHelper, SinkMetricData sinkMetricData) {
        this.stmtFactory = checkNotNull(stmtFactory);
        this.converter = checkNotNull(converter);
        this.batch = new ArrayList<>();
        this.dirtySinkHelper = dirtySinkHelper;
        this.sinkMetricData = sinkMetricData;
    }

    /**
     * parses an SQL exception message, and returns dirty records
     *
     * @param e the exception
     * @param answer the position(s) of dirty record
     * @return as many dirty record positions as it can identify
     */
    public static void parseRecord(Exception e, List<Integer> answer) {
        final Pattern pattern = Pattern.compile("Batch entry (\\d+) ");
        Matcher matcher = pattern.matcher(e.getMessage());
        if (matcher.find()) {
            answer.add(Integer.parseInt(matcher.group(1)));
        }
        // if e is sql exciption, identify all dirty data, else identify only one dirty data.
        if (e instanceof SQLException) {
            LOG.error("SQL exception found");
            SQLException next = ((SQLException) e).getNextException();
            if (next != null) {
                parseRecord(next, answer);
            }
        }
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        st = stmtFactory.createStatement(connection);
    }

    public void setMultipleSink(boolean multipleSink) {
        this.multipleSink = multipleSink;
    }

    @Override
    public void addToBatch(RowData record) throws SQLException {
        batch.add(record);
        LOG.info("record is:{},st is:{}", record, st);
        converter.toExternal(record, st);
        st.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        try {
            st.executeBatch();
            addMetrics();
        } catch (SQLException e) {
            if (counter.incrementAndGet() == 3 || multipleSink) {
                LOG.info("record parse start");
                List<Integer> dirtyRecords = new ArrayList<>();
                parseRecord(e, dirtyRecords);
                handleDirty(dirtyRecords);
                counter.set(0);
            } else {
                throw new SQLException(e);
            }
        }
    }

    private void addMetrics() {
        Set<RowData> set = new HashSet<>();
        int rowCount = 0;
        long rowSize = 0;
        for (RowData record : batch) {
            if (!set.contains(record)) {
                set.add(record);
                rowCount++;
                rowSize += record.toString().getBytes(StandardCharsets.UTF_8).length;
            }
        }
        sinkMetricData.invoke(rowCount, rowSize);
        batch.clear();
    }

    private void handleDirty(List<Integer> dirtyRecords) throws SQLException {
        Long rowCount = Long.valueOf(dirtyRecords.get(0));
        Long rowSize = 0L;
        List<RowData> toClear = new ArrayList<>();
        // find statements
        for (int i = 0; i < dirtyRecords.get(0); i++) {
            try {
                toClear.add(batch.get(i));
                rowSize += batch.get(i).toString().getBytes(StandardCharsets.UTF_8).length;
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }
        sinkMetricData.invoke(rowCount, rowSize);
        for (int dirtyPosition : dirtyRecords) {
            LOG.error("record {} is dirty", dirtyPosition);
            LOG.error("print batch {}", batch.toArray());
            RowData dirtyRecord = batch.get(dirtyPosition);
            dirtySinkHelper.invoke(dirtyRecord, DirtyType.BATCH_LOAD_ERROR, new SQLException("jdbc dirty record"));
            sinkMetricData.invokeDirty(1, dirtyRecord.toString().getBytes(StandardCharsets.UTF_8).length);
            toClear.add(dirtyRecord);
        }
        // clear statement and retry clean data
        batch.removeAll(toClear);
        st.clearParameters();
        for (RowData record : batch) {
            converter.toExternal(record, st);
            st.addBatch();
        }
        executeBatch();
    }

    @Override
    public void closeStatements() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }
    }
}
