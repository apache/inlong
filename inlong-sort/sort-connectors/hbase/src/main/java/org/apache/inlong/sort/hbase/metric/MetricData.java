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

package org.apache.inlong.sort.hbase.metric;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

/**
 * A collection class for handling metrics
 */
public class MetricData {

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

    public MetricData(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetricsForNumRecordsOut(String groupId, String streamId, String nodeId, String metricName) {
        numRecordsOut =
                metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID, nodeId)
                        .counter(metricName);
    }

    public void registerMetricsForNumBytesOut(String groupId, String streamId, String nodeId, String metricName) {
        numBytesOut =
                metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID, nodeId)
                        .counter(metricName);
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
        dirtyRecords = metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID, nodeId)
                .counter(metricName);
    }

    public void registerMetricsForDirtyBytes(String groupId, String streamId, String nodeId,
            String metricName) {
        dirtyBytes =
                metricGroup.addGroup(GROUP_ID, groupId).addGroup(STREAM_ID, streamId).addGroup(NODE_ID, nodeId)
                        .counter(metricName);

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
