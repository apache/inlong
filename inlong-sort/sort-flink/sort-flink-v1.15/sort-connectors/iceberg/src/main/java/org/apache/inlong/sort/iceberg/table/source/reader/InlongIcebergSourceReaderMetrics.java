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

package org.apache.inlong.sort.iceberg.table.source.reader;

import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.SourceMetricData;

import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.flink.source.reader.IcebergSourceReaderMetrics;

/**
 * Inlong iceberg source reader metrics
 */
public class InlongIcebergSourceReaderMetrics extends IcebergSourceReaderMetrics {

    private final MetricGroup metrics;
    private SourceMetricData sourceMetricData;

    public InlongIcebergSourceReaderMetrics(MetricGroup metrics, String fullTableName) {
        super(metrics, fullTableName);
        this.metrics = metrics;
    }

    public void registerMetrics(MetricOption metricOption) {
        if (metricOption != null) {
            sourceMetricData = new SourceMetricData(metricOption, metrics);
        }
    }

    public void incrementAssignedSplits(long count) {
        super.incrementAssignedSplits(count);
    }

    public void incrementAssignedBytes(long count) {
        super.incrementAssignedBytes(count);
    }

    public void incrementFinishedSplits(long count) {
        super.incrementFinishedSplits(count);
    }

    public void incrementFinishedBytes(long count) {
        super.incrementFinishedBytes(count);
        sourceMetricData.outputMetrics(1, count);
    }

    public void incrementSplitReaderFetchCalls(long count) {
        super.incrementSplitReaderFetchCalls(count);
    }
}
