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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsLogSink extends RichSinkFunction<MetricData> implements CheckpointedFunction {

    private static final long serialVersionUID = -2741257263371632560L;

    private static final Logger LOG = LoggerFactory.getLogger(MetricsLogSink.class);

    private transient SimpleDateFormat partitionSecondDateFormat;

    private transient SimpleDateFormat partitionDayDateFormat;

    public MetricsLogSink() {
    }   

    @Override
    public void open(Configuration configuration) throws Exception {
        partitionSecondDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        partitionDayDateFormat = new SimpleDateFormat("yyyyMMdd");
    }

    @Override
    public void invoke(MetricData metricData, Context context) throws Exception {
        Timestamp timestamp = new Timestamp(metricData.getTimestampMillis());
        LOG.info("Record metric, partition_day = '{}', partition_second = '{}', metric_source = '{}',"
                + " metric_type = '{}', attachment = '{}', dataflow_id = '{}', user_partition = '{}', count = '{}'",
                Long.valueOf(partitionDayDateFormat.format(timestamp)),
                partitionSecondDateFormat.format(timestamp),
                metricData.getMetricSource(),
                metricData.getMetricType(),
                metricData.getAttachment(),
                metricData.getDataFlowId(),
                metricData.getPartitions(),
                metricData.getCount());
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    }
}
