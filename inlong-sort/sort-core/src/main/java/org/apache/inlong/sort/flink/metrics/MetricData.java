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

package org.apache.inlong.sort.flink.metrics;

import com.google.common.base.Preconditions;
import java.io.Serializable;

public class MetricData implements Serializable {

    private static final long serialVersionUID = -3294877245954460452L;

    private MetricSource metricSource;

    private MetricType metricType;

    /**
     * If the instance is an raw MetricsData, this field represents the event time in millis of this metric.
     *
     * MetricData will be aggregated into different time windows,
     * if the instance is an aggregated MetricData, this field represents the start timestamp
     * of the aggregated window in millis.
     */
    private long timestampMillis;

    private long dataFlowId;

    private String attachment;

    // User defined partition, support multi-level partitions
    private String partitions;

    private long count;

    public MetricData() {
    }

    public MetricData(
            MetricSource metricSource,
            MetricType metricType,
            long timestampMillis,
            long dataFlowId,
            String attachment,
            String partitions,
            long count) {
        this.metricSource = Preconditions.checkNotNull(metricSource);
        this.metricType = Preconditions.checkNotNull(metricType);
        this.timestampMillis = timestampMillis;
        this.dataFlowId = dataFlowId;
        this.attachment = Preconditions.checkNotNull(attachment);
        this.partitions = Preconditions.checkNotNull(partitions);
        this.count = count;
    }

    public MetricData(
        MetricSource metricSource,
        MetricType metricType,
        long timestampMillis,
        long dataFlowId,
        String attachment,
        long count) {
        this(metricSource,
                metricType,
                timestampMillis,
                dataFlowId,
                attachment,
                "",
                count);
    }

    public enum MetricType {
        SUCCESSFUL,
        ABANDONED
    }

    public enum MetricSource {
        SOURCE,
        DESERIALIZATION,
        TRANSFORMER,
        SINK,
        COMMITTER
    }

    @Override
    public String toString() {
        return "metricSource : " + metricSource
                       + ", metricType : " + metricType
                       + ", timestampMillis : " + timestampMillis
                       + ", dataFlowId : " + dataFlowId
                       + ", attachment : " + attachment
                       + ", partitions : " + partitions
                       + ", count : " + count;
    }

    public String getKey() {
        return metricSource.name()
                + "|" + metricType.name()
                + "|" + dataFlowId
                + "|" + attachment
                + "|" + partitions;
    }

    public MetricSource getMetricSource() {
        return metricSource;
    }

    public void setMetricSource(MetricSource metricSource) {
        this.metricSource = metricSource;
    }

    public MetricType getMetricType() {
        return metricType;
    }

    public void setMetricType(MetricType metricType) {
        this.metricType = metricType;
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public void setTimestampMillis(long timestampMillis) {
        this.timestampMillis = timestampMillis;
    }

    public long getDataFlowId() {
        return dataFlowId;
    }

    public void setDataFlowId(long dataFlowId) {
        this.dataFlowId = dataFlowId;
    }

    public String getAttachment() {
        return attachment;
    }

    public void setAttachment(String attachment) {
        this.attachment = attachment;
    }

    public String getPartitions() {
        return partitions;
    }

    public void setPartitions(String partitions) {
        this.partitions = partitions;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void increase(long count) {
        this.count += count;
    }
}
