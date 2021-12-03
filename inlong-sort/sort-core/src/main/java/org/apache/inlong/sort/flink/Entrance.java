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

package org.apache.inlong.sort.flink;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.inlong.sort.configuration.Constants.SINK_TYPE_DORIS;
import static org.apache.inlong.sort.configuration.Constants.SINK_TYPE_HIVE;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.clickhouse.ClickHouseMultiSinkFunction;
import org.apache.inlong.sort.flink.deserialization.DeserializationSchema;
import org.apache.inlong.sort.flink.doris.DorisMultiSinkFunction;
import org.apache.inlong.sort.flink.hive.HiveMultiTenantCommitter;
import org.apache.inlong.sort.flink.hive.HiveMultiTenantWriter;
import org.apache.inlong.sort.flink.metrics.MetricData;
import org.apache.inlong.sort.flink.metrics.MetricsAggregator.MetricsAggregateFunction;
import org.apache.inlong.sort.flink.metrics.MetricsAggregator.MetricsProcessWindowFunction;
import org.apache.inlong.sort.flink.metrics.MetricsAssignerWithPeriodicWatermarks;
import org.apache.inlong.sort.flink.metrics.MetricsLogSink;
import org.apache.inlong.sort.flink.tubemq.MultiTopicTubeSourceFunction;
import org.apache.inlong.sort.util.ParameterTool;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class Entrance {

    /**
     * Entrance of a flink job.
     */
    public static void main(String[] args) throws Exception {
        final ParameterTool parameter = ParameterTool.fromArgs(args);
        final Configuration config = parameter.getConfiguration();

        final String clusterId = checkNotNull(config.getString(Constants.CLUSTER_ID));
        final String sourceType = checkNotNull(config.getString(Constants.SOURCE_TYPE));
        final String sinkType = checkNotNull(config.getString(Constants.SINK_TYPE));

        final int sourceParallelism = config.getInteger(Constants.SOURCE_PARALLELISM);
        final int deserializationParallelism = config.getInteger(Constants.DESERIALIZATION_PARALLELISM);
        final int sinkParallelism = config.getInteger(Constants.SINK_PARALLELISM);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(config.getInteger(Constants.CHECKPOINT_INTERVAL_MS));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(
                config.getInteger(Constants.MIN_PAUSE_BETWEEN_CHECKPOINTS_MS));
        env.getCheckpointConfig().setCheckpointTimeout(config.getInteger(Constants.CHECKPOINT_TIMEOUT_MS));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Data stream
        DataStream<SerializedRecord> sourceStream;
        if (sourceType.equals(Constants.SOURCE_TYPE_TUBE)) {
            sourceStream = env
                    .addSource(new MultiTopicTubeSourceFunction(config))
                    .setParallelism(sourceParallelism)
                    .uid(Constants.SOURCE_UID)
                    .name("TubeMQ source")
                    .rebalance(); // there might be data lean of source, so re-balance it
        } else {
            throw new IllegalArgumentException("Unsupported source type " + sourceType);
        }

        final SingleOutputStreamOperator<SerializedRecord> deserializationStream = sourceStream
               .process(new DeserializationSchema(config))
               .setParallelism(deserializationParallelism)
               .uid(Constants.DESERIALIZATION_SCHEMA_UID)
               .name("Deserialization");

        if (sinkType.equals(Constants.SINK_TYPE_CLICKHOUSE)) {
            deserializationStream
                    .process(new ClickHouseMultiSinkFunction(config))
                    .setParallelism(sinkParallelism)
                    .uid(Constants.SINK_UID)
                    .name("Clickhouse Sink");
        } else if (sinkType.equals(SINK_TYPE_DORIS)) {
            deserializationStream
                    .process(new DorisMultiSinkFunction(config))
                    .uid(Constants.SINK_UID)
                    .name("Doris Sink")
                    .setParallelism(sinkParallelism);
        } else if (sinkType.equals(SINK_TYPE_HIVE)) {
            deserializationStream
                    .process(new HiveMultiTenantWriter(config))
                    .name("Hive Sink")
                    .uid(Constants.SINK_UID)
                    .setParallelism(sinkParallelism)
                    .process(new HiveMultiTenantCommitter(config))
                    .name("hive Committer")
                    .setParallelism(config.getInteger(Constants.COMMITTER_PARALLELISM));
        } else {
            throw new IllegalArgumentException("Unsupported sink type " + sinkType);
        }

        // Metric stream
        final boolean enableOutputMetrics = config.getBoolean(Constants.METRICS_ENABLE_OUTPUT);
        if (enableOutputMetrics) {
            final int metricsAggregatorParallelism = config.getInteger(Constants.METRICS_AGGREGATOR_PARALLELISM);
            final int metricsTimestampWatermarkAssignerParallelism = config
                    .getInteger(Constants.METRICS_TIMESTAMP_WATERMARK_ASSIGNER_PARALLELISM);
            final int metricsMySQLSinkParallelism = config.getInteger(Constants.METRICS_SINK_PARALLELISM);
            final OutputTag<MetricData> outputTag = new OutputTag<MetricData>(Constants.METRIC_DATA_OUTPUT_TAG_ID) {};

            final DataStream<MetricData> metricsDataStream = deserializationStream
                    .getSideOutput(outputTag)
                    .assignTimestampsAndWatermarks(new MetricsAssignerWithPeriodicWatermarks())
                    .setParallelism(metricsTimestampWatermarkAssignerParallelism)
                    .uid(Constants.METRICS_TIMESTAMP_AND_WATERMARK_ASSIGNER_UID)
                    .name("Metrics timestamp/watermark assigner");

            final DataStream<MetricData> metricsAggregatorStream = metricsDataStream
                    .keyBy((KeySelector<MetricData, String>) MetricData::getKey)
                    .window(TumblingEventTimeWindows.of(
                            Time.minutes(config.getInteger(Constants.METRICS_AGGREGATOR_WINDOW_SIZE))))
                    .allowedLateness(Time.milliseconds(Long.MAX_VALUE))
                    .aggregate(new MetricsAggregateFunction(), new MetricsProcessWindowFunction())
                    .setParallelism(metricsAggregatorParallelism)
                    .uid(Constants.METRICS_AGGREGATOR_UID)
                    .name("Metrics aggregator");

            metricsAggregatorStream
                .addSink(new MetricsLogSink())
                .setParallelism(metricsMySQLSinkParallelism)
                .uid(Constants.METRICS_SINK_UID)
                .name("Metrics sink");
        }

        env.execute(clusterId);
    }
}
