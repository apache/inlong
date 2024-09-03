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

package org.apache.inlong.sort.pulsar.table;

import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricsCollector;
import org.apache.inlong.sort.base.metric.SourceExactlyMetric;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.pulsar.client.api.Message;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A specific {@link PulsarDeserializationSchema} for {@link PulsarTableSource}.
 * Modified from {@link org.apache.flink.connector.pulsar.table.source.PulsarTableDeserializationSchema}
 */
public class PulsarTableDeserializationSchema implements PulsarDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final TypeInformation<RowData> producedTypeInfo;

    @Nullable
    private final DeserializationSchema<RowData> keyDeserialization;

    private final DeserializationSchema<RowData> valueDeserialization;

    private final PulsarRowDataConverter rowDataConverter;

    private final boolean upsertMode;

    private SourceExactlyMetric sourceExactlyMetric;

    private MetricOption metricOption;

    public PulsarTableDeserializationSchema(
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo,
            PulsarRowDataConverter rowDataConverter,
            boolean upsertMode,
            MetricOption metricOption) {
        if (upsertMode) {
            checkNotNull(keyDeserialization, "upsert mode must specify a key format");
        }
        this.keyDeserialization = keyDeserialization;
        this.valueDeserialization = checkNotNull(valueDeserialization);
        this.rowDataConverter = checkNotNull(rowDataConverter);
        this.producedTypeInfo = checkNotNull(producedTypeInfo);
        this.upsertMode = upsertMode;
        this.metricOption = metricOption;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context, SourceConfiguration configuration)
            throws Exception {
        if (keyDeserialization != null) {
            keyDeserialization.open(context);
        }
        if (metricOption != null) {
            sourceExactlyMetric = new SourceExactlyMetric(metricOption);
        }
        valueDeserialization.open(context);
    }

    @Override
    public void deserialize(Message<byte[]> message, Collector<RowData> collector)
            throws IOException {
        // Get the key row data
        List<RowData> keyRowData = new ArrayList<>();
        if (keyDeserialization != null) {
            keyDeserialization.deserialize(message.getKeyBytes(), new ListCollector<>(keyRowData));
        }

        // Get the value row data
        List<RowData> valueRowData = new ArrayList<>();

        if (upsertMode && message.getData().length == 0) {
            rowDataConverter.projectToRowWithNullValueRow(message, keyRowData, collector);
            return;
        }

        MetricsCollector<RowData> metricsCollector =
                new MetricsCollector<>(collector, sourceExactlyMetric);

        valueDeserialization.deserialize(message.getData(), new ListCollector<>(valueRowData));

        rowDataConverter.projectToProducedRowAndCollect(
                message, keyRowData, valueRowData, metricsCollector);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    public void flushAudit() {
        if (sourceExactlyMetric != null) {
            sourceExactlyMetric.flushAudit();
        }
    }

    public void updateCurrentCheckpointId(long checkpointId) {
        if (sourceExactlyMetric != null) {
            sourceExactlyMetric.updateCurrentCheckpointId(checkpointId);
        }
    }

    public void updateLastCheckpointId(long checkpointId) {
        if (sourceExactlyMetric != null) {
            sourceExactlyMetric.updateLastCheckpointId(checkpointId);
        }
    }
}