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

package org.apache.inlong.sort.iceberg.source.reader;

import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.SourceExactlyMetric;
import org.apache.inlong.sort.iceberg.utils.RecyclableJoinedRowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.source.reader.IcebergSourceReaderMetrics;

import java.nio.charset.StandardCharsets;

/**
 * Inlong iceberg source reader metrics
 */
@Slf4j
public class InlongIcebergSourceReaderMetrics<T> extends IcebergSourceReaderMetrics {

    private final MetricGroup metrics;
    private SourceExactlyMetric sourceExactlyMetric;

    public InlongIcebergSourceReaderMetrics(MetricGroup metrics, String fullTableName) {
        super(metrics, fullTableName);
        this.metrics = metrics;
    }

    public void registerMetrics(MetricOption metricOption) {
        if (metricOption != null) {
            sourceExactlyMetric = new SourceExactlyMetric(metricOption, metrics);
        } else {
            log.warn("failed to init sourceMetricData since the metricOption is null");
        }
    }

    public void outputMetricsWithEstimate(ArrayBatchRecords<T> batchRecord) {
        if (sourceExactlyMetric != null) {
            int dataCount = batchRecord.numberOfRecords();
            T[] records = batchRecord.records();
            for (int i = 0; i < dataCount; i++) {
                long dataSize = getDataSize(records[i]);
                long dataTime = getDataTime(records[i]);
                sourceExactlyMetric.outputMetrics(1, dataSize, dataTime);
            }

        }
    }

    private long getDataTime(T object) {
        if (object instanceof RecyclableJoinedRowData) {
            return ((RecyclableJoinedRowData) object).getDataTime();
        }
        return System.currentTimeMillis();
    }

    private long getDataSize(T object) {
        if (object instanceof RecyclableJoinedRowData) {
            RowData physical = ((RecyclableJoinedRowData) object).getPhysicalRowData();
            return physical.toString().getBytes(StandardCharsets.UTF_8).length;
        }
        return object.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    void flushAudit() {
        if (sourceExactlyMetric != null) {
            sourceExactlyMetric.flushAudit();
        }
    }

    void updateCurrentCheckpointId(long checkpointId) {
        if (sourceExactlyMetric != null) {
            sourceExactlyMetric.updateCurrentCheckpointId(checkpointId);
        }
    }

    void updateLastCheckpointId(long checkpointId) {
        if (sourceExactlyMetric != null) {
            sourceExactlyMetric.updateLastCheckpointId(checkpointId);
        }
    }
}
