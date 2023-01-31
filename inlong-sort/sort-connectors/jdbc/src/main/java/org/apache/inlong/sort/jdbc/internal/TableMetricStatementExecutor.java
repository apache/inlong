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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link JdbcBatchStatementExecutor} that simply adds the records into batches of {@link
 * java.sql.PreparedStatement} and doesn't buffer records in memory. Only used in Table/SQL API.
 */
public final class TableMetricStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

    private static final Pattern pattern = Pattern.compile("Batch entry (\\d+) ");
    private static final Logger LOG = LoggerFactory.getLogger(TableMetricStatementExecutor.class);
    private final StatementFactory stmtFactory;
    private final JdbcRowConverter converter;
    private List<RowData> batch;
    private final DirtySinkHelper<Object> dirtySinkHelper;
    private final SinkMetricData sinkMetricData;
    private final AtomicInteger counter = new AtomicInteger();
    private transient FieldNamedPreparedStatement st;
    private boolean multipleSink;
    private Function<RowData, RowData> valueTransform = null;
    // counters used for table level metric calculation for multiple sink
    public long[] metric = new long[4];

    public TableMetricStatementExecutor(StatementFactory stmtFactory, JdbcRowConverter converter,
            DirtySinkHelper<Object> dirtySinkHelper, SinkMetricData sinkMetricData) {
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
    public void addToBatch(RowData record) throws SQLException {
        if (valueTransform != null) {
            record = valueTransform.apply(record); // copy or not
        }
        batch.add(record);
        converter.toExternal(record, st);
        st.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        for (RowData record : batch) {
            LOG.debug("print batch:{}", record);
        }

        try {
            st.executeBatch();
            batch.clear();
        } catch (SQLException e) {
            //clear the prepared statement first to avoid exceptions
            st.clearParameters();
            LOG.debug("record parse start {}, exception cause {}", counter, e);

            try {
                List<Integer> errorPositions = parseError(e);
                for (int pos : errorPositions) {
                    LOG.debug("dirty data detected:{}", batch.get(pos));
                }
                //the data before the first sqlexception are already written, handle those and remove them.
                int writtenSize = errorPositions.get(0);
                //approximate since it may be inefficient to iterate over all writtenSize-1 elements.
                long writtenBytes =
                        (long) batch.get(0).toString().getBytes(StandardCharsets.UTF_8).length * writtenSize;
                if (!multipleSink) {
                    sinkMetricData.invoke(writtenSize, writtenBytes);
                    LOG.debug("print {} records invoke clean", writtenSize);
                } else {
                    metric[0] += writtenSize;
                    metric[1] += writtenBytes;
                }

                batch = batch.subList(writtenSize, batch.size());

                //for the unwritten data, remove the dirty ones
                for (int pos : errorPositions) {
                    pos -= writtenSize;
                    RowData record = batch.get(pos);
                    batch.remove(record);
                    dirtySinkHelper.invoke(record, DirtyType.BATCH_LOAD_ERROR, new SQLException("jdbc dirty record"));
                    if (!multipleSink) {
                        sinkMetricData.invokeDirty(1, record.toString().getBytes(StandardCharsets.UTF_8).length);
                        LOG.debug("print record:{} invoke dirty", record);
                    } else {
                        metric[2]++;
                        metric[3] += record.toString().getBytes(StandardCharsets.UTF_8).length;
                    }
                    LOG.debug("print record:{} invoke dirty", record);
                }

                //try to execute the supposedly clean batch, throw exception on failure
                for (RowData record : batch) {
                    addToBatch(record);
                }
                st.executeBatch();
                batch.clear();
            } catch (Exception ex) {
                //clear parameters to make sure the batch is always clean in the end.
                st.clearParameters();
                batch.clear();
                LOG.error("batch execution failed, cause {}", ex.getMessage());
                throw new SQLException(ex);
            }
        }
    }

    private List<Integer> parseError(SQLException e) {
        List<Integer> errors = new ArrayList<>();
        errors.add(getPosFromMessage(e.getMessage()));
        SQLException next = e.getNextException();
        if(next != null) {
            errors.addAll(parseError(next));
        }
        return errors;
    }

    private int getPosFromMessage(String message) {
        Matcher matcher = pattern.matcher(message);
        if (matcher.find()) {
            int pos = Integer.parseInt(matcher.group(1));
            //duplicate key is a special case, return the second duplicate instead of the first
            if (message.contains("duplicate key")) {
                return getSecondOccurance(pos);
            }
            return pos;
        }
        return -1;
     }

    private int getSecondOccurance(int pos) {
        RowData record = batch.get(pos);
        int counter = 0;
        for (int i = 0; i < batch.size(); i++) {
            if (batch.get(i).equals(record)){
                counter ++;
            }
            if (counter == 2) {
                return i;
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
