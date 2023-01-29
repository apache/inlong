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
import org.apache.inlong.sort.base.metric.sub.SinkTableMetricData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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
    private final SinkTableMetricData sinkMetricData;
    private final AtomicInteger counter = new AtomicInteger();
    private transient FieldNamedPreparedStatement st;
    private boolean multipleSink;
    private Function<RowData, RowData> valueTransform = null;
    // counters used for table level metric calculation for multiple sink
    public long[] metric = new long[4];

    public TableMetricStatementExecutor(StatementFactory stmtFactory, JdbcRowConverter converter,
            DirtySinkHelper<Object> dirtySinkHelper, SinkTableMetricData sinkMetricData) {
        this.stmtFactory = checkNotNull(stmtFactory);
        this.converter = checkNotNull(converter);
        this.batch = new CopyOnWriteArrayList<>();
        this.dirtySinkHelper = dirtySinkHelper;
        this.sinkMetricData = sinkMetricData;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        st = stmtFactory.createStatement(connection);
    }

    public void setMultipleSink(boolean multipleSink) {
        this.multipleSink = multipleSink;
    }

    public void setValueTransform(Function<RowData, RowData> valueTransform) {
        this.valueTransform = valueTransform;
    }

    @Override
    public void addToBatch(RowData record) {
        if (valueTransform != null) {
            record = valueTransform.apply(record); // copy or not
        }
        batch.add(record);
    }

    @Override
    public void executeBatch() throws SQLException {
        for (RowData record : batch) {
            long rowSize = record.toString().getBytes(StandardCharsets.UTF_8).length * 8L;
            try {
                converter.toExternal(record, st);
                st.addBatch();
                st.executeBatch();
                st.clearParameters();
                batch.remove(record);
                if (!multipleSink) {
                    sinkMetricData.invoke(1, rowSize);
                }
                metric[0]++;
                metric[1] += rowSize;
            } catch (Exception e) {
                if (multipleSink || counter.incrementAndGet() >= 3) {
                    LOG.info("record parse start {}", counter);
                    batch.remove(record);
                    dirtySinkHelper.invoke(record, DirtyType.BATCH_LOAD_ERROR, new SQLException("jdbc dirty record"));
                    LOG.info("invoke dirty");
                    if (!multipleSink) {
                        sinkMetricData.invokeDirty(1, rowSize);
                    }
                    metric[2]++;
                    metric[3] += rowSize;
                    st.clearParameters();
                    counter.set(0);
                } else {
                    throw new SQLException(e);
                }
            }
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }
    }
}
