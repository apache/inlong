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
public class SourceMetricData {

    private static Integer TIME_SPAN_IN_SECONDS = 60;
    private static String STREAM_ID = "streamId";
    private static String GROUP_ID = "groupId";
    private static String NODE_ID = "nodeId";
    private final MetricGroup metricGroup;
    private Counter numRecordsIn;
    private Counter numBytesIn;
    private Meter numRecordsInPerSecond;
    private Meter numBytesInPerSecond;

    public SourceMetricData(MetricGroup metricGroup) {
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
    public void registerMetricsForNumRecordsIn(String groupId, String streamId, String nodeId, String metricName) {
        registerMetricsForNumRecordsIn(groupId, streamId, nodeId, metricName, new SimpleCounter());
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
    public void registerMetricsForNumRecordsIn(String groupId, String streamId, String nodeId, String metricName,
            Counter counter) {
        numRecordsIn =
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
    public void registerMetricsForNumBytesIn(String groupId, String streamId, String nodeId, String metricName) {
        registerMetricsForNumBytesIn(groupId, streamId, nodeId, metricName, new SimpleCounter());
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
    public void registerMetricsForNumBytesIn(String groupId, String streamId, String nodeId, String metricName,
            Counter counter) {
        numBytesIn =
                metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID, nodeId)
                        .counter(metricName, counter);
    }

    public void registerMetricsForNumRecordsInPerSecond(String groupId, String streamId, String nodeId,
            String metricName) {
        numRecordsInPerSecond = metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID,
                        nodeId)
                .meter(metricName, new MeterView(this.numRecordsIn, TIME_SPAN_IN_SECONDS));
    }

    public void registerMetricsForNumBytesInPerSecond(String groupId, String streamId, String nodeId,
            String metricName) {
        numBytesInPerSecond = metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId)
                .addGroup(NODE_ID, nodeId)
                .meter(metricName, new MeterView(this.numBytesIn, TIME_SPAN_IN_SECONDS));
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

}
