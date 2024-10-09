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
import org.apache.inlong.sort.base.metric.MetricOption.RegisteredMetric;

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
import static org.apache.inlong.sort.base.Constants.DIRTY_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.DIRTY_RECORDS_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT_FOR_METER;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT_PER_SECOND;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT_FOR_METER;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT_PER_SECOND;
import static org.apache.inlong.sort.base.Constants.NUM_SERIALIZE_ERROR;
import static org.apache.inlong.sort.base.Constants.NUM_SERIALIZE_SUCCESS;
import static org.apache.inlong.sort.base.Constants.NUM_SNAPSHOT_COMPLETE;
import static org.apache.inlong.sort.base.Constants.NUM_SNAPSHOT_CREATE;
import static org.apache.inlong.sort.base.Constants.NUM_SNAPSHOT_ERROR;
import static org.apache.inlong.sort.base.Constants.SERIALIZE_TIME_LAG;
import static org.apache.inlong.sort.base.Constants.SNAPSHOT_TO_CHECKPOINT_TIME_LAG;
import static org.apache.inlong.sort.base.util.CalculateObjectSizeUtils.getDataSize;

/**
 * A collection class for handling metrics
 * <p>
 * Copy from SinkMetricData
 */
public class SinkExactlyMetric implements MetricData, Serializable {

    private static final long serialVersionUID = 1L;
    private final MetricGroup metricGroup;
    private final Map<String, String> labels;
    private final RegisteredMetric registeredMetric;
    private AuditReporterImpl auditReporter;
    private Counter numRecordsOut;
    private Counter numBytesOut;
    private Counter numRecordsOutForMeter;
    private Counter numBytesOutForMeter;
    private Counter dirtyRecordsOut;
    private Counter dirtyBytesOut;
    private Counter numSerializeSuccess;
    private Counter numSerializeError;
    private Gauge<Long> serializeTimeLag;
    /** number of attempts to create a snapshot, i.e. <code>snapshotState()</code> method call */
    private Counter numSnapshotCreate;
    /** number of errors when creating a snapshot */

    private Counter numSnapshotError;
    /** number of successful snapshot completions, i.e. <code>notifyCheckpointComplete</code> method call */
    private Counter numSnapshotComplete;
    private Gauge<Long> snapshotToCheckpointTimeLag;
    private Meter numRecordsOutPerSecond;
    private Meter numBytesOutPerSecond;
    private List<Integer> auditKeys;
    private Long currentCheckpointId = 0L;
    private Long lastCheckpointId = 0L;
    private Long snapshotToCheckpointDelay = 0L;
    private Long serializeDelay = 0L;

    public SinkExactlyMetric(MetricOption option, MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.labels = option.getLabels();
        this.registeredMetric = option.getRegisteredMetric();

        ThreadSafeCounter recordsOutCounter = new ThreadSafeCounter();
        ThreadSafeCounter bytesOutCounter = new ThreadSafeCounter();
        ThreadSafeCounter dirtyRecordsOutCounter = new ThreadSafeCounter();
        ThreadSafeCounter dirtyBytesOutCounter = new ThreadSafeCounter();
        switch (registeredMetric) {
            case DIRTY:
                registerMetricsForDirtyBytesOut(new ThreadSafeCounter());
                registerMetricsForDirtyRecordsOut(new ThreadSafeCounter());
                registerMetricForNumSerializeError(new ThreadSafeCounter());
                registerMetricForNumSnapshotError(new ThreadSafeCounter());
                break;
            case NORMAL:
                recordsOutCounter.inc(option.getInitRecords());
                bytesOutCounter.inc(option.getInitBytes());
                registerMetricsForNumBytesOut(bytesOutCounter);
                registerMetricsForNumRecordsOut(recordsOutCounter);
                registerMetricsForNumBytesOutForMeter(new ThreadSafeCounter());
                registerMetricsForNumRecordsOutForMeter(new ThreadSafeCounter());
                registerMetricsForNumBytesOutPerSecond();
                registerMetricsForNumRecordsOutPerSecond();
                registerMetricForSnapshotToCheckpointTimeLag();
                registerMetricForSerializeTimeLag();
                registerMetricForNumSerializeSuccess(new ThreadSafeCounter());
                registerMetricForNumSnapshotCreate(new ThreadSafeCounter());
                registerMetricForNumSnapshotComplete(new ThreadSafeCounter());
                break;
            default:
                recordsOutCounter.inc(option.getInitRecords());
                bytesOutCounter.inc(option.getInitBytes());
                dirtyRecordsOutCounter.inc(option.getInitDirtyRecords());
                dirtyBytesOutCounter.inc(option.getInitDirtyBytes());
                registerMetricsForNumBytesOut(bytesOutCounter);
                registerMetricsForNumRecordsOut(recordsOutCounter);
                registerMetricsForDirtyRecordsOut(dirtyRecordsOutCounter);
                registerMetricsForDirtyBytesOut(dirtyBytesOutCounter);
                registerMetricsForNumBytesOutForMeter(new ThreadSafeCounter());
                registerMetricsForNumRecordsOutForMeter(new ThreadSafeCounter());
                registerMetricsForNumBytesOutPerSecond();
                registerMetricsForNumRecordsOutPerSecond();
                registerMetricForSnapshotToCheckpointTimeLag();
                registerMetricForSerializeTimeLag();
                registerMetricForNumSerializeSuccess(new ThreadSafeCounter());
                registerMetricForNumSerializeError(new ThreadSafeCounter());
                registerMetricForNumSnapshotCreate(new ThreadSafeCounter());
                registerMetricForNumSnapshotError(new ThreadSafeCounter());
                registerMetricForNumSnapshotComplete(new ThreadSafeCounter());
                break;

        }

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
    public void registerMetricsForNumRecordsOutForMeter() {
        registerMetricsForNumRecordsOutForMeter(new SimpleCounter());
    }

    /**
     * Users can custom counter that extends from {@link Counter}
     * groupId and streamId and nodeId are label values,
     * user can use it to filter metric data when using metric reporter Prometheus
     * The following method is similar
     */
    public void registerMetricsForNumRecordsOutForMeter(Counter counter) {
        numRecordsOutForMeter = registerCounter(NUM_RECORDS_OUT_FOR_METER, counter);
    }

    public void registerMetricsForNumBytesOutForMeter() {
        registerMetricsForNumBytesOutForMeter(new SimpleCounter());

    }

    public void registerMetricsForNumBytesOutForMeter(Counter counter) {
        numBytesOutForMeter = registerCounter(NUM_BYTES_OUT_FOR_METER, counter);
    }

    public void registerMetricsForNumRecordsOut() {
        registerMetricsForNumRecordsOut(new SimpleCounter());
    }

    public void registerMetricsForNumRecordsOut(Counter counter) {
        numRecordsOut = registerCounter(NUM_RECORDS_OUT, counter);
    }

    public void registerMetricsForNumBytesOut() {
        registerMetricsForNumBytesOut(new SimpleCounter());

    }

    public void registerMetricsForNumBytesOut(Counter counter) {
        numBytesOut = registerCounter(NUM_BYTES_OUT, counter);
    }

    public void registerMetricsForNumRecordsOutPerSecond() {
        numRecordsOutPerSecond = registerMeter(NUM_RECORDS_OUT_PER_SECOND, this.numRecordsOutForMeter);
    }

    public void registerMetricsForNumBytesOutPerSecond() {
        numBytesOutPerSecond = registerMeter(NUM_BYTES_OUT_PER_SECOND, this.numBytesOutForMeter);
    }

    public void registerMetricsForDirtyRecordsOut() {
        registerMetricsForDirtyRecordsOut(new SimpleCounter());
    }

    public void registerMetricsForDirtyRecordsOut(Counter counter) {
        dirtyRecordsOut = registerCounter(DIRTY_RECORDS_OUT, counter);
    }

    public void registerMetricsForDirtyBytesOut() {
        registerMetricsForDirtyBytesOut(new SimpleCounter());
    }

    public void registerMetricsForDirtyBytesOut(Counter counter) {
        dirtyBytesOut = registerCounter(DIRTY_BYTES_OUT, counter);
    }

    public void registerMetricForNumSerializeSuccess() {
        numSerializeSuccess = registerCounter(NUM_SERIALIZE_SUCCESS, new SimpleCounter());
    }

    public void registerMetricForNumSerializeSuccess(Counter counter) {
        numSerializeSuccess = registerCounter(NUM_SERIALIZE_SUCCESS, counter);
    }

    public void registerMetricForNumSerializeError() {
        numSerializeError = registerCounter(NUM_SERIALIZE_ERROR, new SimpleCounter());
    }

    public void registerMetricForNumSerializeError(Counter counter) {
        numSerializeError = registerCounter(NUM_SERIALIZE_ERROR, counter);
    }

    public void registerMetricForSerializeTimeLag() {
        serializeTimeLag = registerGauge(SERIALIZE_TIME_LAG, (Gauge<Long>) this::getSerializeDelay);
    }

    public void registerMetricForNumSnapshotCreate() {
        numSnapshotCreate = registerCounter(NUM_SNAPSHOT_CREATE, new SimpleCounter());
    }

    public void registerMetricForNumSnapshotCreate(Counter counter) {
        numSnapshotCreate = registerCounter(NUM_SNAPSHOT_CREATE, counter);
    }

    public void registerMetricForNumSnapshotError() {
        numSnapshotError = registerCounter(NUM_SNAPSHOT_ERROR, new SimpleCounter());
    }

    public void registerMetricForNumSnapshotError(Counter counter) {
        numSnapshotError = registerCounter(NUM_SNAPSHOT_ERROR, counter);
    }

    public void registerMetricForNumSnapshotComplete() {
        numSnapshotComplete = registerCounter(NUM_SNAPSHOT_COMPLETE, new SimpleCounter());
    }

    public void registerMetricForNumSnapshotComplete(Counter counter) {
        numSnapshotComplete = registerCounter(NUM_SNAPSHOT_COMPLETE, counter);
    }

    public void registerMetricForSnapshotToCheckpointTimeLag() {
        snapshotToCheckpointTimeLag =
                registerGauge(SNAPSHOT_TO_CHECKPOINT_TIME_LAG, (Gauge<Long>) this::getSnapshotToCheckpointDelay);
    }

    public Counter getNumRecordsOut() {
        return numRecordsOut;
    }

    public Counter getNumBytesOut() {
        return numBytesOut;
    }

    public Counter getDirtyRecordsOut() {
        return dirtyRecordsOut;
    }

    public Counter getDirtyBytesOut() {
        return dirtyBytesOut;
    }

    public Meter getNumRecordsOutPerSecond() {
        return numRecordsOutPerSecond;
    }

    public Meter getNumBytesOutPerSecond() {
        return numBytesOutPerSecond;
    }

    public Counter getNumSerializeSuccess() {
        return numSerializeSuccess;
    }

    public Counter getNumSerializeError() {
        return numSerializeError;
    }

    public Counter getNumSnapshotCreate() {
        return numSnapshotCreate;
    }

    public Counter getNumSnapshotError() {
        return numSnapshotError;
    }

    public Counter getNumSnapshotComplete() {
        return numSnapshotComplete;
    }

    public Gauge<Long> getSerializeTimeLag() {
        return serializeTimeLag;
    }

    public Gauge<Long> getSnapshotToCheckpointTimeLag() {
        return snapshotToCheckpointTimeLag;
    }

    public Long getSnapshotToCheckpointDelay() {
        return snapshotToCheckpointDelay;
    }

    public Long getSerializeDelay() {
        return serializeDelay;
    }

    public void recordSerializeDelay(Long delay) {
        this.serializeDelay = delay;
    }

    public void recordSnapshotToCheckpointDelay(Long delay) {
        this.snapshotToCheckpointDelay = delay;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    @Override
    public Map<String, String> getLabels() {
        return labels;
    }

    public void incNumSerializeSuccess() {
        if (numSerializeSuccess != null) {
            numSerializeSuccess.inc();
        }
    }

    public void incNumSerializeError() {
        if (numSerializeError != null) {
            numSerializeError.inc();
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

    public void incNumSnapshotComplete() {
        if (numSnapshotComplete != null) {
            numSnapshotComplete.inc();
        }
    }

    public Counter getNumRecordsOutForMeter() {
        return numRecordsOutForMeter;
    }

    public Counter getNumBytesOutForMeter() {
        return numBytesOutForMeter;
    }

    public void invokeDirtyWithEstimate(Object o) {
        invokeDirty(1, getDataSize(o));
    }

    public void invoke(long rowCount, long rowSize, long dataTime) {
        outputDefaultMetrics(rowCount, rowSize);
        outputAuditMetricsWithId(rowCount, rowSize, dataTime);
    }

    private void outputAuditMetricsWithId(long rowCount, long rowSize, long dataTime) {
        if (auditReporter != null) {
            for (Integer key : auditKeys) {
                auditReporter.add(
                        currentCheckpointId,
                        key,
                        DEFAULT_AUDIT_TAG,
                        getGroupId(),
                        getStreamId(),
                        dataTime,
                        rowCount,
                        rowSize,
                        DEFAULT_AUDIT_VERSION);
            }
        }
    }

    private void outputDefaultMetrics(long rowCount, long rowSize) {
        if (numRecordsOut != null) {
            numRecordsOut.inc(rowCount);
        }

        if (numBytesOut != null) {
            numBytesOut.inc(rowSize);
        }

        if (numRecordsOutForMeter != null) {
            numRecordsOutForMeter.inc(rowCount);
        }

        if (numBytesOutForMeter != null) {
            numBytesOutForMeter.inc(rowSize);
        }
    }

    public void invokeDirty(long rowCount, long rowSize) {
        if (dirtyRecordsOut != null) {
            dirtyRecordsOut.inc(rowCount);
        }

        if (dirtyBytesOut != null) {
            dirtyBytesOut.inc(rowSize);
        }
    }

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
        switch (registeredMetric) {
            case DIRTY:
                return "SinkMetricData{"
                        + "metricGroup=" + metricGroup
                        + ", labels=" + labels
                        + ", dirtyRecords=" + dirtyRecordsOut.getCount()
                        + ", dirtyBytes=" + dirtyBytesOut.getCount()
                        + ", numSerializeError=" + numSerializeError.getCount()
                        + ", numSnapshotError=" + numSnapshotError.getCount()
                        + '}';
            case NORMAL:
                return "SinkMetricData{"
                        + "metricGroup=" + metricGroup
                        + ", labels=" + labels
                        + ", auditReporter=" + auditReporter
                        + ", numRecordsOut=" + numRecordsOut.getCount()
                        + ", numBytesOut=" + numBytesOut.getCount()
                        + ", numRecordsOutForMeter=" + numRecordsOutForMeter.getCount()
                        + ", numBytesOutForMeter=" + numBytesOutForMeter.getCount()
                        + ", numRecordsOutPerSecond=" + numRecordsOutPerSecond.getRate()
                        + ", numBytesOutPerSecond=" + numBytesOutPerSecond.getRate()
                        + ", serializeTimeLag=" + serializeTimeLag.getValue()
                        + ", snapshotToCheckpointTimeLag=" + snapshotToCheckpointTimeLag.getValue()
                        + ", numSerializeSuccess=" + numSerializeSuccess.getCount()
                        + ", numSnapshotCreate=" + numSnapshotCreate.getCount()
                        + ", numSnapshotComplete=" + numSnapshotComplete.getCount()
                        + '}';
            default:
                return "SinkMetricData{"
                        + "metricGroup=" + metricGroup
                        + ", labels=" + labels
                        + ", auditReporter=" + auditReporter
                        + ", numRecordsOut=" + numRecordsOut.getCount()
                        + ", numBytesOut=" + numBytesOut.getCount()
                        + ", numRecordsOutForMeter=" + numRecordsOutForMeter.getCount()
                        + ", numBytesOutForMeter=" + numBytesOutForMeter.getCount()
                        + ", dirtyRecordsOut=" + dirtyRecordsOut.getCount()
                        + ", dirtyBytesOut=" + dirtyBytesOut.getCount()
                        + ", numRecordsOutPerSecond=" + numRecordsOutPerSecond.getRate()
                        + ", numBytesOutPerSecond=" + numBytesOutPerSecond.getRate()
                        + ", serializeTimeLag=" + serializeTimeLag.getValue()
                        + ", snapshotToCheckpointTimeLag=" + snapshotToCheckpointTimeLag.getValue()
                        + ", numSerializeSuccess=" + numSerializeSuccess.getCount()
                        + ", numSnapshotCreate=" + numSnapshotCreate.getCount()
                        + ", numSnapshotComplete=" + numSnapshotComplete.getCount()
                        + ", numSerializeError=" + numSerializeError.getCount()
                        + ", numSnapshotError=" + numSnapshotError.getCount()
                        + '}';
        }
    }
}