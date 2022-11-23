/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.inlong.sort.base.metric;

import com.google.common.collect.Maps;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.inlong.audit.AuditOperator;
import org.apache.inlong.sort.base.Constants;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.inlong.sort.base.MetricType;
import org.apache.inlong.sort.base.metric.MetricOption.RegisteredMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.inlong.sort.base.Constants.DELIMITER;
import static org.apache.inlong.sort.base.Constants.INCREASE_PHASE;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN_FOR_METER;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN_PER_SECOND;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN_FOR_METER;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN_PER_SECOND;
import static org.apache.inlong.sort.base.Constants.READ_PHASE;
import static org.apache.inlong.sort.base.Constants.SNAPSHOT_PHASE;

/**
 * A collection class for handling metrics
 */
public class SourceMetricData implements MetricData {

    public static final Logger LOGGER = LoggerFactory.getLogger(SourceMetricData.class);

    private final MetricGroup metricGroup;
    private final Map<String, String> labels;
    private Counter numRecordsIn;
    private Counter numBytesIn;
    private Counter readPhase;
    private Counter numRecordsInForMeter;
    private Counter numBytesInForMeter;
    private Meter numRecordsInPerSecond;
    private Meter numBytesInPerSecond;
    private AuditOperator auditOperator;
    private final MetricType metricType;
    private final Map<String, SourceMetricData> subSourceMetricMap = Maps.newHashMap();

    public SourceMetricData(MetricOption option, MetricGroup metricGroup) {
        this(option, metricGroup, MetricType.NODE);
    }

    public SourceMetricData(MetricOption option, MetricGroup metricGroup, MetricType metricType) {
        this.metricGroup = metricGroup;
        this.labels = option.getLabels();
        this.metricType = metricType;

        ThreadSafeCounter recordsInCounter = new ThreadSafeCounter();
        ThreadSafeCounter bytesInCounter = new ThreadSafeCounter();

        switch (option.getRegisteredMetric()) {
            case ALL:
                ThreadSafeCounter readPhaseCounter = new ThreadSafeCounter();
                readPhaseCounter.inc(option.getReadPhase());
                recordsInCounter.inc(option.getInitRecords());
                bytesInCounter.inc(option.getInitBytes());
                registerMetricsForReadPhase(readPhaseCounter);
                registerMetricsForNumRecordsIn(recordsInCounter);
                registerMetricsForNumBytesIn(bytesInCounter);
                registerMetricsForNumBytesInForMeter(new ThreadSafeCounter());
                registerMetricsForNumRecordsInForMeter(new ThreadSafeCounter());
                registerMetricsForNumBytesInPerSecond();
                registerMetricsForNumRecordsInPerSecond();
                break;
            default:
                recordsInCounter.inc(option.getInitRecords());
                bytesInCounter.inc(option.getInitBytes());
                registerMetricsForNumRecordsIn(recordsInCounter);
                registerMetricsForNumBytesIn(bytesInCounter);
                registerMetricsForNumBytesInForMeter(new ThreadSafeCounter());
                registerMetricsForNumRecordsInForMeter(new ThreadSafeCounter());
                registerMetricsForNumBytesInPerSecond();
                registerMetricsForNumRecordsInPerSecond();
                break;
        }

        if (option.getIpPorts().isPresent()) {
            AuditOperator.getInstance().setAuditProxy(option.getIpPortList());
            this.auditOperator = AuditOperator.getInstance();
        }
    }

    /**
     * Default counter is {@link SimpleCounter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     */
    public void registerMetricsForNumRecordsInForMeter() {
        registerMetricsForNumRecordsInForMeter(new SimpleCounter());
    }

    /**
     * User can use custom counter that extends from {@link Counter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     */
    public void registerMetricsForNumRecordsInForMeter(Counter counter) {
        numRecordsInForMeter = registerCounter(NUM_RECORDS_IN_FOR_METER, counter, this.metricType);
    }

    /**
     * User can use custom counter that extends from {@link Counter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     */
    private void registerMetricsForReadPhase(Counter counter) {
        readPhase = registerCounter(READ_PHASE, counter, this.metricType);
    }

    /**
     * Default counter is {@link SimpleCounter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     */
    public void registerMetricsForNumBytesInForMeter() {
        registerMetricsForNumBytesInForMeter(new SimpleCounter());
    }

    /**
     * User can use custom counter that extends from {@link Counter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     */
    public void registerMetricsForNumBytesInForMeter(Counter counter) {
        numBytesInForMeter = registerCounter(NUM_BYTES_IN_FOR_METER, counter, this.metricType);
    }

    /**
     * Default counter is {@link SimpleCounter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     */
    public void registerMetricsForNumRecordsIn() {
        registerMetricsForNumRecordsIn(new SimpleCounter());
    }

    /**
     * User can use custom counter that extends from {@link Counter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     */
    public void registerMetricsForNumRecordsIn(Counter counter) {
        numRecordsIn = registerCounter(NUM_RECORDS_IN, counter, this.metricType);
    }

    /**
     * Default counter is {@link SimpleCounter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     */
    public void registerMetricsForNumBytesIn() {
        registerMetricsForNumBytesIn(new SimpleCounter());
    }

    /**
     * User can use custom counter that extends from {@link Counter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     */
    public void registerMetricsForNumBytesIn(Counter counter) {
        numBytesIn = registerCounter(NUM_BYTES_IN, counter, this.metricType);
    }

    public void registerMetricsForNumRecordsInPerSecond() {
        numRecordsInPerSecond = registerMeter(NUM_RECORDS_IN_PER_SECOND, this.numRecordsInForMeter, this.metricType);
    }

    public void registerMetricsForNumBytesInPerSecond() {
        numBytesInPerSecond = registerMeter(NUM_BYTES_IN_PER_SECOND, this.numBytesInForMeter);
    }

    public Counter getNumRecordsIn() {
        return numRecordsIn;
    }

    public Counter getNumBytesIn() {
        return numBytesIn;
    }

    public Counter getReadPhase() {
        return readPhase;
    }

    public Meter getNumRecordsInPerSecond() {
        return numRecordsInPerSecond;
    }

    public Meter getNumBytesInPerSecond() {
        return numBytesInPerSecond;
    }

    public Counter getNumRecordsInForMeter() {
        return numRecordsInForMeter;
    }

    public Counter getNumBytesInForMeter() {
        return numBytesInForMeter;
    }

    public Map<String, SourceMetricData> getSubSourceMetricMap() {
        return subSourceMetricMap;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    @Override
    public Map<String, String> getLabels() {
        return labels;
    }

    /**
     * register sub metrics group from metric state
     *
     * @param metricState MetricState
     */
    public void registerSubMetricsGroup(MetricState metricState) {
        if (metricState == null || metricState.getSubMetricStateMap() == null
                || metricState.getSubMetricStateMap().isEmpty()) {
            return;
        }

        Map<String, MetricState> subMetricStateMap = metricState.getSubMetricStateMap();
        for (Entry<String, MetricState> subMetricStateEntry : subMetricStateMap.entrySet()) {
            String schemaIdentify = subMetricStateEntry.getKey();
            SourceRecordSchemaInfo sourceRecordSchemaInfo = new SourceRecordSchemaInfo(schemaIdentify);
            final MetricState subMetricState = subMetricStateEntry.getValue();
            SourceMetricData subSourceMetricData = buildSubSourceMetricData(sourceRecordSchemaInfo,
                    subMetricState, this);
            subSourceMetricMap.put(subMetricStateEntry.getKey(), subSourceMetricData);
        }
        LOGGER.info("register subMetricsGroup from metricState,sub metric map size:{}", subSourceMetricMap.size());
    }

    /**
     * build sub source metric data
     *
     * @param recordSchemaInfo source record schema info
     * @param subMetricState sub metric state
     * @param sourceMetricData source metric data
     * @return sub source metric data
     */
    private SourceMetricData buildSubSourceMetricData(SourceRecordSchemaInfo recordSchemaInfo,
            MetricState subMetricState, SourceMetricData sourceMetricData) {
        if (sourceMetricData == null || recordSchemaInfo == null) {
            return null;
        }
        final MetricGroup metricGroup = sourceMetricData.getMetricGroup();

        // build sub metricGroup
        MetricGroup nodeMetricGroup = metricGroup
                .addGroup(Constants.GROUP_ID, sourceMetricData.getGroupId())
                .addGroup(Constants.STREAM_ID, sourceMetricData.getStreamId())
                .addGroup(Constants.NODE_ID, sourceMetricData.getNodeId());

        MetricGroup subMetricGroup;
        String topicName = recordSchemaInfo.getTopicName();
        if (StringUtils.isNotBlank(topicName)) {
            // judging only the topic
            subMetricGroup = nodeMetricGroup.addGroup(Constants.TOPIC_NAME, topicName);
        } else {
            String databaseName = recordSchemaInfo.getDatabaseName();
            MetricGroup databaseMetricGroup = nodeMetricGroup.addGroup(Constants.DATABASE_NAME, databaseName);
            String schemaName = recordSchemaInfo.getSchemaName();
            String tableName = recordSchemaInfo.getTableName();
            if (StringUtils.isNotBlank(schemaName)) {
                subMetricGroup = databaseMetricGroup.addGroup(Constants.SCHEMA_NAME, schemaName)
                        .addGroup(Constants.TABLE_NAME, tableName);
            } else {
                subMetricGroup = databaseMetricGroup.addGroup(Constants.TABLE_NAME, tableName);
            }
        }

        // build option labels
        String subLabels = this.labels.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(DELIMITER));

        MetricOption metricOption = MetricOption.builder()
                .withInitRecords(subMetricState != null ? subMetricState.getMetricValue(NUM_RECORDS_IN) : 0L)
                .withInitBytes(subMetricState != null ? subMetricState.getMetricValue(NUM_BYTES_IN) : 0L)
                .withReadPhase(subMetricState != null ? subMetricState.getMetricValue(READ_PHASE) : 0L)
                .withInlongLabels(subLabels)
                .withRegisterMetric(RegisteredMetric.NORMAL)
                .build();

        return new SourceMetricData(metricOption, subMetricGroup, MetricType.TABLE);
    }

    /**
     * build record schema identify
     *
     * @param recordSchemaInfo source record schema info
     * @return record schema identify
     */
    public String buildSchemaIdentify(SourceRecordSchemaInfo recordSchemaInfo) {
        String database = recordSchemaInfo.getDatabaseName();
        String topicName = recordSchemaInfo.getTopicName();
        // Judging only the topic
        if (StringUtils.isBlank(database) && StringUtils.isNotBlank(topicName)) {
            return topicName;
        }

        String table = recordSchemaInfo.getTableName();
        String schema = recordSchemaInfo.getSchemaName();
        StringBuilder identifyBuilder = new StringBuilder();
        identifyBuilder.append(database).append(Constants.SEMICOLON);
        if (StringUtils.isNotBlank(schema)) {
            identifyBuilder.append(schema).append(Constants.SEMICOLON);
        }
        identifyBuilder.append(table);
        return identifyBuilder.toString();
    }

    public void outputMetricsWithEstimate(Object o) {
        long size = o.toString().getBytes(StandardCharsets.UTF_8).length;
        outputMetrics(1, size);
    }

    public void outputMetricsWithEstimate(SourceRecordSchemaInfo recordSchemaInfo, Object o) {
        if (recordSchemaInfo == null) {
            LOGGER.warn("record schema info is null when outputting metrics with estimate");
            outputMetricsWithEstimate(o);
            return;
        }
        String identify = buildSchemaIdentify(recordSchemaInfo);
        SourceMetricData subSourceMetricData;
        if (subSourceMetricMap.containsKey(identify)) {
            subSourceMetricData = subSourceMetricMap.get(identify);
        } else {
            subSourceMetricData = buildSubSourceMetricData(recordSchemaInfo, null, this);
            subSourceMetricMap.put(identify, subSourceMetricData);
        }
        // sourceMetric and subSourceMetric output metrics
        long rowCountSize = 1L;
        long rowDataSize = o.toString().getBytes(StandardCharsets.UTF_8).length;
        this.outputMetrics(rowCountSize, rowDataSize, recordSchemaInfo.getSnapshotRecord());
        subSourceMetricData.outputMetrics(rowCountSize, rowDataSize, recordSchemaInfo.getSnapshotRecord());
    }

    public void outputMetrics(long rowCountSize, long rowDataSize) {
        if (numRecordsIn != null) {
            this.numRecordsIn.inc(rowCountSize);
        }

        if (numBytesIn != null) {
            this.numBytesIn.inc(rowDataSize);
        }

        if (numRecordsInForMeter != null) {
            this.numRecordsInForMeter.inc(rowCountSize);
        }

        if (numBytesInForMeter != null) {
            this.numBytesInForMeter.inc(rowDataSize);
        }

        if (auditOperator != null) {
            auditOperator.add(
                    Constants.AUDIT_SORT_INPUT,
                    getGroupId(),
                    getStreamId(),
                    System.currentTimeMillis(),
                    rowCountSize,
                    rowDataSize);
        }
    }

    public void outputMetrics(long rowCountSize, long rowDataSize, boolean isSnapshotRecord) {

        outputMetrics(rowCountSize, rowDataSize);

        if (readPhase == null) {
            return;
        }
        long count = this.readPhase.getCount();
        if (isSnapshotRecord && count != SNAPSHOT_PHASE) {
            this.readPhase.dec(count);
            this.readPhase.inc(SNAPSHOT_PHASE);
        } else if (!isSnapshotRecord && count != INCREASE_PHASE) {
            this.readPhase.dec(count);
            this.readPhase.inc(INCREASE_PHASE);
        }
    }

    @Override
    public String toString() {
        return "SourceMetricData{"
                + "metricGroup=" + metricGroup
                + ", labels=" + labels
                + ", metricType=" + metricType
                + ", readPhase=" + (readPhase != null ? readPhase.getCount() : null)
                + ", numRecordsIn=" + numRecordsIn.getCount()
                + ", numBytesIn=" + numBytesIn.getCount()
                + ", numRecordsInForMeter=" + numRecordsInForMeter.getCount()
                + ", numBytesInForMeter=" + numBytesInForMeter.getCount()
                + ", numRecordsInPerSecond=" + numRecordsInPerSecond.getRate()
                + ", numBytesInPerSecond=" + numBytesInPerSecond.getRate()
                + ", auditOperator=" + auditOperator
                + ", subSourceMetricMap=" + subSourceMetricMap
                + '}';
    }

    /**
     * Source Record Schema Info
     */
    public static class SourceRecordSchemaInfo {

        private String databaseName;
        private String schemaName;
        private String tableName;
        private String topicName;
        private Boolean snapshotRecord;

        public SourceRecordSchemaInfo(@NonNull String databaseName, @NonNull String schemaName,
                @NonNull String tableName, Boolean snapshotRecord) {
            this.databaseName = databaseName;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.snapshotRecord = snapshotRecord;
        }

        public SourceRecordSchemaInfo(@NonNull String databaseName, @NonNull String tableName, Boolean snapshotRecord) {
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.snapshotRecord = snapshotRecord;
        }

        public SourceRecordSchemaInfo(@NonNull String topicName, Boolean snapshotRecord) {
            this.topicName = topicName;
            this.snapshotRecord = snapshotRecord;
        }

        public SourceRecordSchemaInfo(String metricStateSchemaIdentify) {
            String[] identifyArr = metricStateSchemaIdentify.split(Constants.SPILT_SEMICOLON);
            int identifyLength = identifyArr.length;
            if (identifyLength == 1) {
                // judge The case of topic
                this.topicName = identifyArr[0];
            } else {
                // judge The case of database.schema.table or database.table
                this.databaseName = identifyArr[0];
                if (identifyArr.length == 3) {
                    this.schemaName = identifyArr[1];
                    this.tableName = identifyArr[2];
                } else {
                    this.tableName = identifyArr[1];
                }
            }
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public void setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public Boolean getSnapshotRecord() {
            return snapshotRecord != null && snapshotRecord;
        }

        public void setSnapshotRecord(Boolean snapshotRecord) {
            this.snapshotRecord = snapshotRecord;
        }

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("databaseName", databaseName)
                    .append("schemaName", schemaName)
                    .append("tableName", tableName)
                    .append("topicName", topicName)
                    .append("snapshotRecord", snapshotRecord)
                    .toString();
        }
    }
}
