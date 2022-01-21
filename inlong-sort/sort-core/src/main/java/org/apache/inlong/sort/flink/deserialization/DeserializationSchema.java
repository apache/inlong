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

package org.apache.inlong.sort.flink.deserialization;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.inlong.audit.AuditImp;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.flink.InLongMsgMixedSerializedRecord;
import org.apache.inlong.sort.flink.metrics.MetricData;
import org.apache.inlong.sort.flink.metrics.MetricData.MetricSource;
import org.apache.inlong.sort.flink.metrics.MetricData.MetricType;
import org.apache.inlong.sort.flink.transformation.FieldMappingTransformer;
import org.apache.inlong.sort.flink.transformation.RecordTransformer;
import org.apache.inlong.sort.meta.MetaManager;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.flink.util.OutputTag;
import org.apache.inlong.sort.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeserializationSchema extends ProcessFunction<SerializedRecord, SerializedRecord> {

    private static final long serialVersionUID = 5380421870587560943L;

    private static final Logger LOG = LoggerFactory.getLogger(DeserializationSchema.class);

    private static final OutputTag<MetricData> METRIC_DATA_OUTPUT_TAG
            = new OutputTag<MetricData>(Constants.METRIC_DATA_OUTPUT_TAG_ID) {};

    private transient RecordTransformer recordTransformer;

    private transient FieldMappingTransformer fieldMappingTransformer;

    private final Configuration config;

    private transient Object schemaLock;

    private transient MultiTenancyInLongMsgMixedDeserializer multiTenancyInLongMsgMixedDeserializer;

    private transient MultiTenancyDeserializer multiTenancyDeserializer;

    private transient MetaManager metaManager;

    private transient Boolean enableOutputMetrics;

    // dataflow id -> Pair<group id, stream id>
    private transient Map<Long, Pair<String, String>> inLongGroupIdAndStreamIdMap;

    private transient AuditImp auditImp;

    public DeserializationSchema(Configuration config) {
        this.config = Preconditions.checkNotNull(config);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        schemaLock = new Object();
        multiTenancyInLongMsgMixedDeserializer = new MultiTenancyInLongMsgMixedDeserializer();
        multiTenancyDeserializer = new MultiTenancyDeserializer();
        fieldMappingTransformer = new FieldMappingTransformer();
        recordTransformer = new RecordTransformer(config.getInteger(Constants.ETL_RECORD_SERIALIZATION_BUFFER_SIZE));
        metaManager = MetaManager.getInstance(config);
        metaManager.registerDataFlowInfoListener(new DataFlowInfoListenerImpl());
        enableOutputMetrics = config.getBoolean(Constants.METRICS_ENABLE_OUTPUT);

        inLongGroupIdAndStreamIdMap = new HashMap<>();
        String auditHostAndPorts = config.getString(Constants.METRICS_AUDIT_SDK_HOSTS);
        if (auditHostAndPorts != null) {
            AuditImp.getInstance().setAuditProxy(new HashSet<>(Arrays.asList(auditHostAndPorts.split(","))));
            auditImp = AuditImp.getInstance();
        }
    }

    @Override
    public void close() throws Exception {
        if (metaManager != null) {
            metaManager.close();
            metaManager = null;
        }

        if (auditImp != null) {
            auditImp.sendReport();
        }
    }

    @Override
    public void processElement(
            SerializedRecord serializedRecord,
            Context context,
            Collector<SerializedRecord> collector) throws Exception {
        try {
            if (enableOutputMetrics
                    && !config.getString(Constants.SOURCE_TYPE).equals(Constants.SOURCE_TYPE_TUBE)) {
                // If source is tube, we do not output metrics of package number
                final MetricData metricData = new MetricData(
                        // since source could not have side-outputs, so it outputs metrics for source here
                        MetricSource.SOURCE,
                        MetricType.SUCCESSFUL,
                        serializedRecord.getTimestampMillis(),
                        serializedRecord.getDataFlowId(),
                        "",
                        1L);
                context.output(METRIC_DATA_OUTPUT_TAG, metricData);
            }

            final CallbackCollector<Record> transformCollector = new CallbackCollector<>(sourceRecord -> {
                final Record sinkRecord = fieldMappingTransformer.transform(sourceRecord);

                if (enableOutputMetrics) {
                    MetricData metricData = new MetricData(
                            // TODO, outputs this metric in Sink Function
                            MetricSource.SINK,
                            MetricType.SUCCESSFUL,
                            sinkRecord.getTimestampMillis(),
                            sinkRecord.getDataflowId(),
                            "",
                            1);

                    context.output(METRIC_DATA_OUTPUT_TAG, metricData);
                }

                SerializedRecord serializedSinkRecord = recordTransformer.toSerializedRecord(sinkRecord);

                if (auditImp != null) {
                    Pair<String, String> groupIdAndStreamId = inLongGroupIdAndStreamIdMap.getOrDefault(
                            serializedRecord.getDataFlowId(),
                            Pair.of("", ""));

                    auditImp.add(
                            Constants.METRIC_AUDIT_ID_FOR_INPUT,
                            groupIdAndStreamId.getLeft(),
                            groupIdAndStreamId.getRight(),
                            sinkRecord.getTimestampMillis(),
                            1,
                            serializedSinkRecord.getData().length);
                }

                collector.collect(serializedSinkRecord);
            });

            if (serializedRecord instanceof InLongMsgMixedSerializedRecord) {
                final InLongMsgMixedSerializedRecord
                        inlongmsgRecord = (InLongMsgMixedSerializedRecord) serializedRecord;
                synchronized (schemaLock) {
                    multiTenancyInLongMsgMixedDeserializer.deserialize(inlongmsgRecord, transformCollector);
                }
            } else {
                synchronized (schemaLock) {
                    multiTenancyDeserializer.deserialize(serializedRecord, transformCollector);
                }
            }
        } catch (Exception e) {
            if (enableOutputMetrics
                    && !config.getString(Constants.SOURCE_TYPE).equals(Constants.SOURCE_TYPE_TUBE)) {
                MetricData metricData = new MetricData(
                        MetricSource.DESERIALIZATION,
                        MetricType.ABANDONED,
                        serializedRecord.getTimestampMillis(),
                        serializedRecord.getDataFlowId(),
                        (e.getMessage() == null || e.getMessage().isEmpty()) ? "Exception caught" : e.getMessage(),
                        1);
                context.output(METRIC_DATA_OUTPUT_TAG, metricData);
            }

            LOG.warn("Abandon data", e);
        }
    }

    private class DataFlowInfoListenerImpl implements DataFlowInfoListener {

        @Override
        public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (schemaLock) {
                multiTenancyInLongMsgMixedDeserializer.addDataFlow(dataFlowInfo);
                multiTenancyDeserializer.addDataFlow(dataFlowInfo);
                fieldMappingTransformer.addDataFlow(dataFlowInfo);
                recordTransformer.addDataFlow(dataFlowInfo);

                inLongGroupIdAndStreamIdMap.put(
                        dataFlowInfo.getId(),
                        CommonUtils.getInLongGroupIdAndStreamId(dataFlowInfo));
            }
        }

        @Override
        public void updateDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (schemaLock) {
                multiTenancyInLongMsgMixedDeserializer.updateDataFlow(dataFlowInfo);
                multiTenancyDeserializer.updateDataFlow(dataFlowInfo);
                fieldMappingTransformer.updateDataFlow(dataFlowInfo);
                recordTransformer.updateDataFlow(dataFlowInfo);

                inLongGroupIdAndStreamIdMap.put(
                        dataFlowInfo.getId(),
                        CommonUtils.getInLongGroupIdAndStreamId(dataFlowInfo));
            }
        }

        @Override
        public void removeDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (schemaLock) {
                multiTenancyInLongMsgMixedDeserializer.removeDataFlow(dataFlowInfo);
                multiTenancyDeserializer.removeDataFlow(dataFlowInfo);
                fieldMappingTransformer.removeDataFlow(dataFlowInfo);
                recordTransformer.removeDataFlow(dataFlowInfo);

                inLongGroupIdAndStreamIdMap.remove(dataFlowInfo.getId());
            }
        }
    }
}
