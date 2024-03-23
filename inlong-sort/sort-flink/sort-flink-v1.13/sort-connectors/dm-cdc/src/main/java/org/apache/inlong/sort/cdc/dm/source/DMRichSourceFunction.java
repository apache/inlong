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

package org.apache.inlong.sort.cdc.dm.source;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricOption.RegisteredMetric;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.SourceMetricData;
import org.apache.inlong.sort.base.util.MetricStateUtils;
import org.apache.inlong.sort.cdc.dm.table.DMDeserializationSchema;
import org.apache.inlong.sort.cdc.dm.table.DMRecord;
import org.apache.inlong.sort.cdc.dm.utils.DMSQLClient;
import org.apache.inlong.sort.cdc.dm.utils.LogMinerDmlParser;
import org.apache.inlong.sort.protocol.ddl.enums.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.apache.inlong.sort.base.Constants.INLONG_METRIC_STATE_NAME;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN;

/**
 * The source implementation for DM that read snapshot events first and then read the change
 * event.
 *
 * @param <T> The type created by the deserializer.
 */
public class DMRichSourceFunction<T> extends RichSourceFunction<T>
        implements
            CheckpointListener,
            CheckpointedFunction,
            ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DMRichSourceFunction.class);

    // reading snapshot
    private final String username;
    private final String password;
    private final String databaseName;
    private final String schemaName;
    private final String tableName;
    private final Duration connectTimeout;
    private final String hostname;
    private final Integer port;
    private final Properties jdbcProperties;

    // one single connection used to handle incremental records
    private transient volatile DMConnection snapshotConnection;
    private transient volatile DMSQLClient client;
    private final DMDeserializationSchema<T> deserializer;

    // state related parameters
    private volatile long scn;
    private volatile boolean snapshot;

    private transient Set<String> tableSet;
    private transient ListState<Long> offsetState;
    private transient OutputCollector<T> outputCollector;
    private transient LogMinerDmlParser logMinerDmlParser;
    private boolean isRunning;

    // inlong metric related configs
    private final String inlongMetric;
    private final String inlongAudit;
    private SourceMetricData metricData;
    private transient ListState<MetricState> metricStateListState;
    private MetricState metricState;

    // reserved for whole db migration
    private final boolean sourceMultipleEnable = false;

    public DMRichSourceFunction(
            boolean snapshot,
            String username,
            String password,
            String databaseName,
            String schemaName,
            String tableName,
            String tableList,
            Duration connectTimeout,
            String hostname,
            Integer port,
            Properties jdbcProperties,
            DMDeserializationSchema<T> deserializer,
            String inlongMetric,
            String inlongAudit) {
        this.snapshot = snapshot;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.connectTimeout = connectTimeout;
        this.hostname = hostname;
        this.port = port;
        this.jdbcProperties = jdbcProperties;
        this.deserializer = deserializer;
        this.inlongMetric = inlongMetric;
        this.inlongAudit = inlongAudit;
    }

    @Override
    public void open(final Configuration config) throws Exception {
        LOG.info("starting to open");
        super.open(config);
        this.outputCollector = new OutputCollector<>();
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        LOG.info("starting to open logminer");
        instantiateStates(ctx);
        DMConnection incrementalConnection = new DMConnection(
                hostname,
                port,
                username,
                password,
                connectTimeout,
                "dm.jdbc.driver.DmDriver",
                jdbcProperties,
                getClass().getClassLoader());

        // logs related to this part are printed in dmsqlclient, not here.
        this.client = new DMSQLClient(incrementalConnection, tableName);
        outputCollector.context = ctx;
        try {
            LOG.info("Start to initialize table whitelist");
            initTableWhiteList();
            // check where to do the snapshot phase
            LOG.info("Snapshot reading started");
            readSnapshotRecords();
            LOG.info("Snapshot reading finished, start readChangeRecords process");
            scn = client.openlogminer();
            isRunning = true;
            readChangeRecords();
        } finally {
            isRunning = false;
            cancel();
        }
    }

    private void instantiateStates(SourceContext<T> ctx) {
        try {
            final Method getMetricGroupMethod =
                    getRuntimeContext().getClass().getMethod("getMetricGroup");
            getMetricGroupMethod.setAccessible(true);
            final MetricGroup metricGroup =
                    (MetricGroup) getMetricGroupMethod.invoke(getRuntimeContext());
            MetricOption metricOption = MetricOption.builder()
                    .withInlongLabels(inlongMetric)
                    .withInlongAudit(inlongAudit)
                    .withInitRecords(metricState != null ? metricState.getMetricValue(NUM_RECORDS_IN) : 0L)
                    .withInitBytes(metricState != null ? metricState.getMetricValue(NUM_BYTES_IN) : 0L)
                    .withRegisterMetric(RegisteredMetric.ALL)
                    .build();
            if (metricOption != null) {
                metricData = new SourceMetricData(metricOption, metricGroup);
            }
        } catch (Throwable e) {
            LOG.error("initialize inlong metric failed ", e);
        }
    }

    private DMConnection getSnapshotConnection() {
        if (snapshotConnection == null) {
            snapshotConnection =
                    new DMConnection(
                            hostname,
                            port,
                            username,
                            password,
                            connectTimeout,
                            "dm.jdbc.driver.DmDriver",
                            jdbcProperties,
                            getClass().getClassLoader());
        }
        return snapshotConnection;
    }

    private void closeSnapshotConnection() {
        if (snapshotConnection != null) {
            try {
                snapshotConnection.close();
            } catch (SQLException e) {
                LOG.error("Failed to close snapshotConnection", e);
            }
            snapshotConnection = null;
        }
    }

    private void initTableWhiteList() {
        if (tableSet != null && !tableSet.isEmpty()) {
            return;
        }

        final Set<String> localTableSet = new HashSet<>();
        localTableSet.add(schemaName + "." + tableName);

        LOG.info("Table list: {}", localTableSet);
        this.tableSet = localTableSet;
    }

    protected void readSnapshotRecords() {
        tableSet.forEach(
                table -> {
                    String[] schema = table.split("\\.");
                    readSnapshotRecordsByTable(databaseName, schema[0], schema[1]);
                });
        snapshot = false;
    }

    private void readSnapshotRecordsByTable(String databaseName, String schemaName, String tableName) {
        // snapshot phase can't determine exact scn, assign 0 to all.
        DMRecord.SourceInfo sourceInfo = new DMRecord.SourceInfo(databaseName, schemaName, tableName, 0);
        String fullName = String.format("%s.%s", schemaName, tableName);
        StringBuilder queryString = new StringBuilder("SELECT * FROM " + fullName);
        // for incremental mode, just read one snapshot to get the table schema
        if (!snapshot) {
            queryString.append(" LIMIT 1");
        }
        try {
            LOG.info("Start to read snapshot from {}", fullName);
            getSnapshotConnection()
                    .query(queryString.toString(),
                            rs -> {
                                ResultSetMetaData metaData = rs.getMetaData();
                                List<String> columns = new ArrayList<>();
                                while (rs.next()) {
                                    Map<String, Object> fieldMap = new HashMap<>();
                                    for (int i = 0; i < metaData.getColumnCount(); i++) {
                                        fieldMap.put(
                                                metaData.getColumnName(i + 1), rs.getObject(i + 1));
                                        if (logMinerDmlParser == null) {
                                            columns.add(metaData.getColumnName(i + 1));
                                        }
                                    }

                                    if (logMinerDmlParser == null) {
                                        // the actual columns are the reverse of the actual order for dm, error-prone.
                                        Collections.reverse(columns);
                                        this.logMinerDmlParser = new LogMinerDmlParser(columns);
                                        // dml parser must be initialized, so snapshot phase must be executed.
                                        if (!snapshot) {
                                            return;
                                        }
                                    }
                                    DMRecord record = new DMRecord(sourceInfo, OperationType.INSERT, fieldMap);
                                    if (metricData != null) {
                                        metricData.outputMetrics(1, record.toString().getBytes().length * 8L);
                                    }
                                    try {
                                        deserializer.deserialize(record, outputCollector);
                                    } catch (Exception e) {
                                        LOG.error("Deserialize snapshot record failed ", e);
                                        throw new FlinkRuntimeException(e);
                                    }

                                }
                            });
            LOG.info("Read snapshot from {} finished", fullName);
        } catch (SQLException e) {
            LOG.error("Read snapshot from table " + fullName + " failed", e);
            throw new FlinkRuntimeException(e);
        }
    }

    private void readChangeRecords() {
        LOG.info("starting to read change records");
        while (isRunning) {
            // TODO: make this multithreaded (one connection per thread), and improve performance
            final CountDownLatch latch = new CountDownLatch(1);
            List<DMRecord> records = getNewRecords();
            // wait for timeout and restart logminer if current session has no incremental records.
            if (!records.isEmpty()) {
                latch.countDown();
            } else {
                LOG.warn("no change detected, restarting logminer");
                // if the last redo log is exhausted, no new change will come, so restart logminer.
                try {
                    client.closelogminer();
                    client.openlogminer();
                    latch.countDown();
                    LOG.info("restart logminer success");
                } catch (Throwable e) {
                    LOG.error("restart logminer failed ", e);
                }
            }
        }
    }

    private List<DMRecord> getNewRecords() {
        List<DMRecord> records = client.processIncrementalRecords(databaseName, schemaName, scn, logMinerDmlParser);
        try {
            records.forEach(
                    r -> {
                        try {
                            deserializer.deserialize(r, outputCollector);
                            scn = r.getSourceInfo().getSCN();
                        } catch (Exception e) {
                            throw new FlinkRuntimeException(e);
                        }
                    });
        } catch (Throwable e) {
            LOG.error("read change records failed ", e);
        }

        // add record size, process specially for update records
        int size = 0;
        for (DMRecord record : records) {
            if (record.getOpt().equals(OperationType.UPDATE)) {
                size += 2;
            } else {
                size++;
            }
        }

        if (metricData != null) {
            metricData.outputMetrics(size, records.size() * 8L);
            LOG.info("registering metrics {}", records.size());
        }
        LOG.info("finished processing {} incremental records", records.size());
        return records;
    }

    @Override
    public void notifyCheckpointComplete(long l) {
        // do nothing
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializer.getProducedType();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info(
                "snapshotState checkpoint: {} at scn: {}",
                context.getCheckpointId(),
                scn);
        offsetState.clear();
        offsetState.add(scn);
        if (metricData != null && metricStateListState != null) {
            MetricStateUtils.snapshotMetricStateForSourceMetricData(metricStateListState, metricData,
                    getRuntimeContext().getIndexOfThisSubtask());
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("initialize checkpoint");
        OperatorStateStore stateStore = context.getOperatorStateStore();
        offsetState = stateStore.getListState(
                new ListStateDescriptor<>(
                        "scnState", LongSerializer.INSTANCE));

        if (this.inlongMetric != null) {
            this.metricStateListState =
                    stateStore.getUnionListState(
                            new ListStateDescriptor<>(
                                    INLONG_METRIC_STATE_NAME, TypeInformation.of(new TypeHint<MetricState>() {
                                    })));
        }

        if (context.isRestored()) {
            for (final Long offset : offsetState.get()) {
                scn = offset;
                LOG.info("Restore State from scn: {}", scn);
                return;
            }
        }
    }

    @Override
    public void cancel() {
        closeSnapshotConnection();
        client.closelogminer();
    }

    private static class OutputCollector<T> implements Collector<T> {

        private SourceContext<T> context;

        @Override
        public void collect(T record) {
            context.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
