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

package org.apache.inlong.sort.base.metric;

import org.apache.inlong.audit.AuditReporterImpl;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.audit.consts.ConfigConstants.DEFAULT_AUDIT_TAG;
import static org.apache.inlong.common.constant.Constants.DEFAULT_AUDIT_VERSION;
import static org.apache.inlong.sort.base.Constants.CURRENT_EMIT_EVENT_TIME_LAG;
import static org.apache.inlong.sort.base.Constants.CURRENT_FETCH_EVENT_TIME_LAG;
import static org.apache.inlong.sort.base.Constants.DESERIALIZE_TIME_LAG;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN_FOR_METER;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN_PER_SECOND;
import static org.apache.inlong.sort.base.Constants.NUM_COMPLETED_SNAPSHOTS;
import static org.apache.inlong.sort.base.Constants.NUM_DESERIALIZE_ERROR;
import static org.apache.inlong.sort.base.Constants.NUM_DESERIALIZE_SUCCESS;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN_FOR_METER;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN_PER_SECOND;
import static org.apache.inlong.sort.base.Constants.NUM_SNAPSHOT_CREATE;
import static org.apache.inlong.sort.base.Constants.NUM_SNAPSHOT_ERROR;
import static org.apache.inlong.sort.base.Constants.SNAPSHOT_TO_CHECKPOINT_TIME_LAG;
import static org.apache.inlong.sort.base.util.CalculateObjectSizeUtils.getDataSize;

public class SourceExactlyMetric implements MetricData, Serializable, SourceMetricsReporter {

    private static final long serialVersionUID = 1L;
    private MetricGroup metricGroup;
    private final Map<String, String> labels;
    private Counter numRecordsIn;
    private Counter numBytesIn;
    private Counter numRecordsInForMeter;
    private Counter numBytesInForMeter;
    private Counter numDeserializeSuccess;
    private Counter numDeserializeError;
    private Gauge<Long> deserializeTimeLag;
    private Counter numSnapshotCreate;
    private Counter numSnapshotError;
    private Counter numCompletedSnapshots;
    private Gauge<Long> snapshotToCheckpointTimeLag;
    private Meter numRecordsInPerSecond;
    private Meter numBytesInPerSecond;
    private AuditReporterImpl auditReporter;
    private List<Integer> auditKeys;
    private Long currentCheckpointId = 0L;
    private Long lastCheckpointId = 0L;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private Gauge currentFetchEventTimeLag;
    /**
     * currentEmitEventTimeLag = EmitTime - messageTimestamp, where the EmitTime is the time the record leaves the
     * source operator.
     */
    private Gauge currentEmitEventTimeLag;

    /**
     * fetchDelay = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = 0L;

    /**
     * emitDelay = EmitTime - messageTimestamp, where the EmitTime is the time the record leaves the
     * source operator.
     */
    private volatile long emitDelay = 0L;

    /**
     * deserializeDelay = deserializeEndTime - deserializeStartTime, where the deserializeStartTime is the time method deserialize is called,
     * and deserializeEndTime is the time the record is emitted
     */
    private volatile long deserializeDelay = 0L;

    /**
     * snapshotToCheckpointDelay = snapShotCompleteTime - snapShotStartTimeById, where the snapShotCompleteTime is the time the logic of notifyCheckpointComplete is finished
     */
    private volatile long snapshotToCheckpointDelay = 0L;

    public SourceExactlyMetric(MetricOption option, MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.labels = option.getLabels();

        ThreadSafeCounter recordsInCounter = new ThreadSafeCounter();
        ThreadSafeCounter bytesInCounter = new ThreadSafeCounter();
        switch (option.getRegisteredMetric()) {
            default:
                recordsInCounter.inc(option.getInitRecords());
                bytesInCounter.inc(option.getInitBytes());
                registerMetricsForNumRecordsIn(recordsInCounter);
                registerMetricsForNumBytesIn(bytesInCounter);
                registerMetricsForNumBytesInForMeter(new ThreadSafeCounter());
                registerMetricsForNumRecordsInForMeter(new ThreadSafeCounter());
                registerMetricsForNumBytesInPerSecond();
                registerMetricsForNumRecordsInPerSecond();
                registerMetricsForCurrentFetchEventTimeLag();
                registerMetricsForCurrentEmitEventTimeLag();
                registerMetricsForDeserializeTimeLag();
                registerMetricsForNumDeserializeSuccess(new ThreadSafeCounter());
                registerMetricsForNumDeserializeError(new ThreadSafeCounter());
                registerMetricsForNumSnapshotCreate(new ThreadSafeCounter());
                registerMetricsForNumSnapshotError(new ThreadSafeCounter());
                registerMetricsForSnapshotToCheckpointTimeLag();
                break;
        }

        if (option.getIpPorts().isPresent()) {
            this.auditReporter = new AuditReporterImpl();
            auditReporter.setAutoFlush(false);
            auditReporter.setAuditProxy(option.getIpPortSet());
            this.auditKeys = option.getInlongAuditKeys();
        }
    }

    public SourceExactlyMetric(MetricOption option) {
        this.labels = option.getLabels();
        if (option.getIpPorts().isPresent()) {
            this.auditReporter = new AuditReporterImpl();
            auditReporter.setAutoFlush(false);
            auditReporter.setAuditProxy(option.getIpPortSet());
            this.auditKeys = option.getInlongAuditKeys();
        }
    }

    /**
     * Users can custom counter that extends from {@link SimpleCounter}
     * groupId and streamId and nodeId are label values,
     * user can use it to filter metric data when using metric reporter Prometheus
     * The following method is similar
     */
    public void registerMetricsForNumRecordsInForMeter() {
        registerMetricsForNumRecordsInForMeter(new SimpleCounter());
    }

    /**
     * Users can custom counter that extends from {@link Counter}
     * groupId and streamId and nodeId are label values,
     * user can use it to filter metric data when using metric reporter Prometheus
     * The following method is similar
     */
    public void registerMetricsForNumRecordsInForMeter(Counter counter) {
        numRecordsInForMeter = registerCounter(NUM_RECORDS_IN_FOR_METER, counter);
    }

    public void registerMetricsForNumBytesInForMeter() {
        registerMetricsForNumBytesInForMeter(new SimpleCounter());
    }

    public void registerMetricsForNumBytesInForMeter(Counter counter) {
        numBytesInForMeter = registerCounter(NUM_BYTES_IN_FOR_METER, counter);
    }

    public void registerMetricsForNumRecordsIn() {
        registerMetricsForNumRecordsIn(new SimpleCounter());
    }

    public void registerMetricsForNumRecordsIn(Counter counter) {
        numRecordsIn = registerCounter(NUM_RECORDS_IN, counter);
    }

    public void registerMetricsForNumBytesIn() {
        registerMetricsForNumBytesIn(new SimpleCounter());
    }

    public void registerMetricsForNumBytesIn(Counter counter) {
        numBytesIn = registerCounter(NUM_BYTES_IN, counter);
    }

    public void registerMetricsForNumRecordsInPerSecond() {
        numRecordsInPerSecond = registerMeter(NUM_RECORDS_IN_PER_SECOND, this.numRecordsInForMeter);
    }

    public void registerMetricsForNumBytesInPerSecond() {
        numBytesInPerSecond = registerMeter(NUM_BYTES_IN_PER_SECOND, this.numBytesInForMeter);
    }

    public void registerMetricsForCurrentFetchEventTimeLag() {
        currentFetchEventTimeLag = registerGauge(CURRENT_FETCH_EVENT_TIME_LAG, (Gauge<Long>) this::getFetchDelay);
    }

    public void registerMetricsForCurrentEmitEventTimeLag() {
        currentEmitEventTimeLag = registerGauge(CURRENT_EMIT_EVENT_TIME_LAG, (Gauge<Long>) this::getEmitDelay);
    }
    public void registerMetricsForDeserializeTimeLag() {
        deserializeTimeLag = registerGauge(DESERIALIZE_TIME_LAG, (Gauge<Long>) this::getDeserializeDelay);
    }

    public void registerMetricsForNumDeserializeSuccess(Counter counter) {
        numDeserializeSuccess = registerCounter(NUM_DESERIALIZE_SUCCESS, counter);
    }

    public void registerMetricsForNumDeserializeError(Counter counter) {
        numDeserializeError = registerCounter(NUM_DESERIALIZE_ERROR, counter);
    }

    public void registerMetricsForNumSnapshotCreate(Counter counter) {
        numSnapshotCreate = registerCounter(NUM_SNAPSHOT_CREATE, counter);
    }

    public void registerMetricsForNumSnapshotError(Counter counter) {
        numSnapshotError = registerCounter(NUM_SNAPSHOT_ERROR, counter);
    }

    public void registerMetricsForNumCompletedCheckpoints(Counter counter) {
        numCompletedSnapshots = registerCounter(NUM_COMPLETED_SNAPSHOTS, counter);
    }

    public void registerMetricsForSnapshotToCheckpointTimeLag() {
        snapshotToCheckpointTimeLag =
                registerGauge(SNAPSHOT_TO_CHECKPOINT_TIME_LAG, (Gauge<Long>) this::getSnapshotToCheckpointDelay);
    }

    public Gauge getDeserializeTimeLag() {
        return deserializeTimeLag;
    }

    public Gauge getSnapshotToCheckpointTimeLag() {
        return snapshotToCheckpointTimeLag;
    }

    public Counter getNumDeserializeSuccess() {
        return numDeserializeSuccess;
    }

    public Counter getNumDeserializeError() {
        return numDeserializeError;
    }

    public Counter getNumSnapshotCreate() {
        return numSnapshotCreate;
    }

    public Counter getNumSnapshotError() {
        return numSnapshotError;
    }

    public Counter getNumRecordsIn() {
        return numRecordsIn;
    }

    public Counter getNumBytesIn() {
        return numBytesIn;
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

    public long getFetchDelay() {
        return fetchDelay;
    }

    public long getEmitDelay() {
        return emitDelay;
    }

    public long getDeserializeDelay() {
        return deserializeDelay;
    }

    public long getSnapshotToCheckpointDelay() {
        return snapshotToCheckpointDelay;
    }

    public Counter getNumCompletedSnapshots() {
        return numCompletedSnapshots;
    }

    public void recordDeserializeDelay(long deserializeDelay) {
        this.deserializeDelay = deserializeDelay;
    }

    public void recordSnapshotToCheckpointDelay(long snapshotToCheckpointDelay) {
        this.snapshotToCheckpointDelay = snapshotToCheckpointDelay;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    @Override
    public Map<String, String> getLabels() {
        return labels;
    }

    @Override
    public void outputMetricsWithEstimate(Object data, long dataTime) {
        outputMetrics(1, getDataSize(data), dataTime);
    }

    public void outputMetrics(long rowCountSize, long rowDataSize, long dataTime) {
        outputDefaultMetrics(rowCountSize, rowDataSize);
        if (auditReporter != null) {
            for (Integer key : auditKeys) {
                auditReporter.add(
                        this.currentCheckpointId,
                        key,
                        DEFAULT_AUDIT_TAG,
                        getGroupId(),
                        getStreamId(),
                        dataTime,
                        rowCountSize,
                        rowDataSize,
                        DEFAULT_AUDIT_VERSION);
            }
        }
    }

    private void outputDefaultMetrics(long rowCountSize, long rowDataSize) {
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
    }

    public void incNumDeserializeSuccess() {
        if (numDeserializeSuccess != null) {
            numDeserializeSuccess.inc();
        }
    }

    public void incNumDeserializeError() {
        if (numDeserializeError != null) {
            numDeserializeError.inc();
        }
    }

    public void incNumSnapshotCreate() {
        if (numSnapshotCreate != null) {
            numSnapshotCreate.inc();
        }
    }

    public void incNumSnapshotError() {
        if (numSnapshotError != null) {
            numSnapshotError.inc();
        }
    }

    public void incNumCompletedSnapshots() {
        if (numCompletedSnapshots != null) {
            numCompletedSnapshots.inc();
        }
    }

    /**
     * flush audit data
     * usually call this method in close method or when checkpointing
     */
    public void flushAudit() {
        if (auditReporter != null) {
            auditReporter.flush(lastCheckpointId);
        }
    }

    public void updateLastCheckpointId(Long checkpointId) {
        lastCheckpointId = checkpointId;
    }
    public void updateCurrentCheckpointId(Long checkpointId) {
        currentCheckpointId = checkpointId;
    }

    @Override
    public String toString() {
        return "SourceMetricData{"
                + "metricGroup=" + metricGroup
                + ", labels=" + labels
                + ", numRecordsIn=" + numRecordsIn.getCount()
                + ", numBytesIn=" + numBytesIn.getCount()
                + ", numRecordsInForMeter=" + numRecordsInForMeter.getCount()
                + ", numBytesInForMeter=" + numBytesInForMeter.getCount()
                + ", numRecordsInPerSecond=" + numRecordsInPerSecond.getRate()
                + ", numBytesInPerSecond=" + numBytesInPerSecond.getRate()
                + ", currentFetchEventTimeLag=" + currentFetchEventTimeLag.getValue()
                + ", currentEmitEventTimeLag=" + currentEmitEventTimeLag.getValue()
                + ", deserializeTimeLag=" + deserializeTimeLag.getValue()
                + ", numDeserializeSuccess=" + numDeserializeSuccess.getCount()
                + ", numDeserializeError=" + numDeserializeError.getCount()
                + ", numSnapshotCreate=" + numSnapshotCreate.getCount()
                + ", numSnapshotError=" + numSnapshotError.getCount()
                + ", snapshotToCheckpointTimeLag=" + snapshotToCheckpointTimeLag.getValue()
                + ", numRecordsInPerSecond=" + numRecordsInPerSecond.getRate()
                + ", numBytesInPerSecond=" + numBytesInPerSecond.getRate()
                + ", auditReporter=" + auditReporter
                + '}';
    }
}
