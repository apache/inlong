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

package org.apache.inlong.sort.tubemq.table;

import org.apache.inlong.sort.tubemq.FlinkTubeMQProducer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.TreeSet;

public class TubeMQTableSink implements DynamicTableSink {

    /**
     * Format for encoding values from TubeMQ.
     */
    private final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;
    /**
     * Data type to configure the formats.
     */
    private final DataType physicalDataType;
    /**
     * The TubeMQ topic name.
     */
    private final String topic;
    /**
     * The address of TubeMQ master, format eg: 127.0.0.1:8715,127.0.0.2:8715.
     */
    private final String masterAddress;
    /**
     * The TubeMQ streamId filter collection.
     */
    private final TreeSet<String> streamIdSet;
    /**
     * The parameters collection for tubemq producer.
     */
    private final Configuration configuration;

    public TubeMQTableSink(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            String topic,
            String masterAddress,
            TreeSet<String> streamIdSet,
            Configuration configuration) {
        Preconditions.checkNotNull(valueEncodingFormat, "The serialization schema must not be null.");
        Preconditions.checkNotNull(physicalDataType, "Physical data type must not be null.");
        Preconditions.checkNotNull(topic, "Topic must not be null.");
        Preconditions.checkNotNull(masterAddress, "Master address must not be null.");
        Preconditions.checkNotNull(configuration, "The configuration must not be null.");
        Preconditions.checkNotNull(streamIdSet, "The streamId set must not be null.");

        this.valueEncodingFormat = valueEncodingFormat;
        this.physicalDataType = physicalDataType;
        this.topic = topic;
        this.masterAddress = masterAddress;
        this.streamIdSet = streamIdSet;
        this.configuration = configuration;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return valueEncodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> serialization = createSerialization(context,
                valueEncodingFormat, physicalDataType);

        final FlinkTubeMQProducer<RowData> tubeMQProducer =
                createTubeMQProducer(topic, masterAddress, serialization, configuration);

        return SinkFunctionProvider.of(tubeMQProducer, 1);
    }

    private FlinkTubeMQProducer<RowData> createTubeMQProducer(
            String topic,
            String masterAddress,
            SerializationSchema<RowData> serializationSchema,
            Configuration configuration) {
        final FlinkTubeMQProducer<RowData> tubeMQProducer =
                new FlinkTubeMQProducer(topic, masterAddress, serializationSchema, streamIdSet, configuration);
        return tubeMQProducer;
    }

    private SerializationSchema<RowData> createSerialization(
            Context context,
            EncodingFormat<SerializationSchema<RowData>> format,
            DataType physicalDataType) {
        return format.createRuntimeEncoder(context, physicalDataType);
    }

    @Override
    public DynamicTableSink copy() {
        return new TubeMQTableSink(
                physicalDataType,
                valueEncodingFormat,
                topic,
                masterAddress,
                streamIdSet,
                configuration);
    }

    @Override
    public String asSummaryString() {
        return "TubeMQ table sink";
    }
}
