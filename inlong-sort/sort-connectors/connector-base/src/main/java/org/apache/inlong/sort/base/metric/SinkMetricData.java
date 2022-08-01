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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

/**
 * A collection class for handling metrics
 */
public class SinkMetricData {

    private final MetricGroup metricGroup;

    private Counter numRecordsOut;
    private Counter numBytesOut;
    private Counter dirtyRecords;
    private Counter dirtyBytes;
    private Meter numRecordsOutPerSecond;
    private Meter numBytesOutPerSecond;
    private static Integer TIME_SPAN_IN_SECONDS = 60;
    private static String STREAM_ID = "streamId";
    private static String GROUP_ID = "groupId";
    private static String NODE_ID = "nodeId";

    public SinkMetricData(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    /**
     * Default counter is {@link SimpleCounter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     *
     * @param groupId inlong groupId
     * @param streamId inlong streamId
     * @param nodeId inlong nodeId
     * @param metricName metric name
     */
    public void registerMetricsForNumRecordsOut(String groupId, String streamId, String nodeId, String metricName) {
        registerMetricsForNumRecordsOut(groupId, streamId, nodeId, metricName, new SimpleCounter());
    }

    /**
     * User can use custom counter that extends from {@link Counter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     *
     * @param groupId inlong groupId
     * @param streamId inlong streamId
     * @param nodeId inlong nodeId
     * @param metricName metric name
     */
    public void registerMetricsForNumRecordsOut(String groupId, String streamId, String nodeId, String metricName,
            Counter counter) {
        numRecordsOut =
                metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID, nodeId)
                        .counter(metricName, counter);
    }

    /**
     * Default counter is {@link SimpleCounter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     *
     * @param groupId inlong groupId
     * @param streamId inlong streamId
     * @param nodeId inlong nodeId
     * @param metricName metric name
     */
    public void registerMetricsForNumBytesOut(String groupId, String streamId, String nodeId, String metricName) {
        registerMetricsForNumBytesOut(groupId, streamId, nodeId, metricName, new SimpleCounter());
    }

    /**
     * User can use custom counter that extends from {@link Counter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     *
     * @param groupId inlong groupId
     * @param streamId inlong streamId
     * @param nodeId inlong nodeId
     * @param metricName metric name
     */
    public void registerMetricsForNumBytesOut(String groupId, String streamId, String nodeId, String metricName,
            Counter counter) {
        numBytesOut =
                metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID, nodeId)
                        .counter(metricName, counter);
    }

    public void registerMetricsForNumRecordsOutPerSecond(String groupId, String streamId, String nodeId,
            String metricName) {
        numRecordsOutPerSecond = metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID,
                        nodeId)
                .meter(metricName, new MeterView(this.numRecordsOut, TIME_SPAN_IN_SECONDS));
    }

    public void registerMetricsForNumBytesOutPerSecond(String groupId, String streamId, String nodeId,
            String metricName) {
        numBytesOutPerSecond = metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId)
                .addGroup(NODE_ID, nodeId)
                .meter(metricName, new MeterView(this.numBytesOut, TIME_SPAN_IN_SECONDS));
    }

    public void registerMetricsForDirtyRecords(String groupId, String streamId, String nodeId,
            String metricName) {
        registerMetricsForDirtyRecords(groupId, streamId, nodeId, metricName, new SimpleCounter());
    }

    public void registerMetricsForDirtyRecords(String groupId, String streamId, String nodeId,
            String metricName, Counter counter) {
        dirtyRecords = metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID, nodeId)
                .counter(metricName, counter);
    }

    /**
     * Default counter is {@link SimpleCounter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     *
     * @param groupId inlong groupId
     * @param streamId inlong streamId
     * @param nodeId inlong nodeId
     * @param metricName metric name
     */
    public void registerMetricsForDirtyBytes(String groupId, String streamId, String nodeId,
            String metricName) {
        registerMetricsForDirtyBytes(groupId, streamId, nodeId, metricName, new SimpleCounter());
    }

    /**
     * User can use custom counter that extends from {@link Counter}
     * groupId and streamId and nodeId are label value, user can use it filter metric data when use metric reporter
     * prometheus
     *
     * @param groupId inlong groupId
     * @param streamId inlong streamId
     * @param nodeId inlong nodeId
     * @param metricName metric name
     */
    public void registerMetricsForDirtyBytes(String groupId, String streamId, String nodeId,
            String metricName, Counter counter) {
        dirtyBytes =
                metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID, nodeId)
                        .counter(metricName, counter);
    }

    public Counter getNumRecordsOut() {
        return numRecordsOut;
    }

    public Counter getNumBytesOut() {
        return numBytesOut;
    }

    public Counter getDirtyRecords() {
        return dirtyRecords;
    }

    public Counter getDirtyBytes() {
        return dirtyBytes;
    }

    public Meter getNumRecordsOutPerSecond() {
        return numRecordsOutPerSecond;
    }

    public Meter getNumBytesOutPerSecond() {
        return numBytesOutPerSecond;
    }

}
