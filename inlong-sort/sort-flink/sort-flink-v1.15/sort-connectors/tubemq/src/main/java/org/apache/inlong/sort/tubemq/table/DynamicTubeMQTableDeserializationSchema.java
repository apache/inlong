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

import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.sort.base.dirty.DirtyData;
import org.apache.inlong.sort.base.dirty.DirtyOptions;
import org.apache.inlong.sort.base.dirty.DirtyType;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricsCollector;
import org.apache.inlong.sort.base.metric.SourceExactlyMetric;
import org.apache.inlong.tubemq.corebase.Message;

import com.google.common.base.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class DynamicTubeMQTableDeserializationSchema implements DynamicTubeMQDeserializationSchema<RowData> {

    /**
     * data buffer message
     */
    private final DeserializationSchema<RowData> deserializationSchema;

    /**
     * {@link MetadataConverter} of how to produce metadata from message.
     */
    private final MetadataConverter[] metadataConverters;

    /**
     * {@link TypeInformation} of the produced {@link RowData} (physical + meta data).
     */
    private final TypeInformation<RowData> producedTypeInfo;

    /**
     * status of error
     */
    private final boolean ignoreErrors;

    private final boolean innerFormat;

    private SourceExactlyMetric sourceExactlyMetric;

    private final MetricOption metricOption;

    private final DirtySink<byte[]> dirtySink;

    private final DirtyOptions dirtyOptions;

    public DynamicTubeMQTableDeserializationSchema(
            DeserializationSchema<RowData> schema,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo,
            boolean ignoreErrors,
            boolean innerFormat,
            MetricOption metricOption,
            DirtySink<byte[]> dirtySink,
            DirtyOptions dirtyOptions) {
        this.deserializationSchema = schema;
        this.metadataConverters = metadataConverters;
        this.producedTypeInfo = producedTypeInfo;
        this.ignoreErrors = ignoreErrors;
        this.innerFormat = innerFormat;
        this.metricOption = metricOption;
        this.dirtySink = dirtySink;
        this.dirtyOptions = dirtyOptions;
    }

    @Override
    public void open() throws Exception {
        if (metricOption != null) {
            sourceExactlyMetric = new SourceExactlyMetric(metricOption);
        }
        if (dirtySink != null) {
            dirtySink.open(new Configuration());
        }
    }

    @Override
    public RowData deserialize(Message message) throws IOException {
        return deserializationSchema.deserialize(message.getData());
    }

    @Override
    public void deserialize(Message message, Collector<RowData> out) throws IOException {
        List<RowData> rows = new ArrayList<>();

        MetricsCollector<RowData> metricsCollector =
                new MetricsCollector<>(new ListCollector<>(rows), sourceExactlyMetric);

        // reset time stamp if the deserialize schema has not inner format
        if (!innerFormat) {
            metricsCollector.resetTimestamp(System.currentTimeMillis());
        }

        if (!dirtyOptions.ignoreDirty()) {
            deserializationSchema.deserialize(message.getData(), metricsCollector);
        } else {
            try {
                deserializationSchema.deserialize(message.getData(), metricsCollector);
            } catch (Throwable t) {
                if (dirtySink != null) {
                    DirtyData.Builder<byte[]> builder = DirtyData.builder();
                    try {

                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
                        long dataTime = LocalDateTime.parse(message.getMsgTime(), formatter)
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();

                        builder.setData(message.getData())
                                .setDirtyType(DirtyType.KEY_DESERIALIZE_ERROR)
                                .setServerType(MQType.TUBEMQ)
                                .setDirtyDataTime(dataTime)
                                .setExtParams(message.getAttribute())
                                .setLabels(dirtyOptions.getLabels())
                                .setLogTag(dirtyOptions.getLogTag())
                                .setDirtyMessage(t.getMessage())
                                .setIdentifier(dirtyOptions.getIdentifier());
                        dirtySink.invoke(builder.build());
                    } catch (Exception ex) {
                        if (!dirtyOptions.ignoreSideOutputErrors()) {
                            throw new IOException(ex);
                        }
                        log.warn("Dirty sink failed", ex);
                    }
                }
            }
        }

        rows.forEach(row -> emitRow(message, (GenericRowData) row, out));

    }

    @Override
    public void flushAudit() {
        if (sourceExactlyMetric != null) {
            sourceExactlyMetric.flushAudit();
        }
    }
    @Override
    public void setCurrentCheckpointId(long checkpointId) {
        if (sourceExactlyMetric != null) {
            sourceExactlyMetric.updateCurrentCheckpointId(checkpointId);
        }
    }

    @Override
    public void updateLastCheckpointId(Long checkpointId) {
        if (sourceExactlyMetric != null) {
            sourceExactlyMetric.updateLastCheckpointId(checkpointId);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DynamicTubeMQTableDeserializationSchema)) {
            return false;
        }
        DynamicTubeMQTableDeserializationSchema that = (DynamicTubeMQTableDeserializationSchema) o;
        return ignoreErrors == that.ignoreErrors
                && Objects.equal(Arrays.stream(metadataConverters).collect(Collectors.toList()),
                        Arrays.stream(that.metadataConverters).collect(Collectors.toList()))
                && Objects.equal(deserializationSchema, that.deserializationSchema)
                && Objects.equal(producedTypeInfo, that.producedTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(deserializationSchema, metadataConverters, producedTypeInfo, ignoreErrors);
    }

    /**
     * add metadata column
     */
    private void emitRow(Message head, GenericRowData physicalRow, Collector<RowData> out) {
        if (metadataConverters.length == 0) {
            out.collect(physicalRow);
            return;
        }
        final int physicalArity = physicalRow.getArity();
        final int metadataArity = metadataConverters.length;
        final GenericRowData producedRow =
                new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);
        for (int physicalPos = 0; physicalPos < physicalArity; physicalPos++) {
            producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
        }
        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    physicalArity + metadataPos, metadataConverters[metadataPos].read(head));
        }
        out.collect(producedRow);
    }

    interface MetadataConverter extends Serializable {

        Object read(Message head);
    }
}
