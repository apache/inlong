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

import static org.apache.inlong.sort.configuration.Constants.UNKNOWN_DATAFLOW_ID;

import com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.flink.TDMsgSerializedRecord;
import org.apache.inlong.sort.flink.transformation.FieldMappingTransformer;
import org.apache.inlong.sort.flink.transformation.RecordTransformer;
import org.apache.inlong.sort.meta.MetaManager;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeserializationSchema extends ProcessFunction<SerializedRecord, SerializedRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(DeserializationSchema.class);

    private transient RecordTransformer recordTransformer;

    private transient FieldMappingTransformer fieldMappingTransformer;

    private final Configuration config;

    private transient Object schemaLock;

    private transient MultiTenancyTDMsgMixedDeserializer multiTenancyTdMsgMixedDeserializer;

    private transient MetaManager metaManager;

    public DeserializationSchema(Configuration config) {
        this.config = Preconditions.checkNotNull(config);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        schemaLock = new Object();
        multiTenancyTdMsgMixedDeserializer = new MultiTenancyTDMsgMixedDeserializer();
        fieldMappingTransformer = new FieldMappingTransformer();
        recordTransformer = new RecordTransformer(config.getInteger(Constants.ETL_RECORD_SERIALIZATION_BUFFER_SIZE));
        metaManager = MetaManager.getInstance(config);
        metaManager.registerDataFlowInfoListener(new DataFlowInfoListenerImpl());
    }

    @Override
    public void close() throws Exception {
        if (metaManager != null) {
            metaManager.close();
            metaManager = null;
        }
    }

    @Override
    public void processElement(
            SerializedRecord serializedRecord,
            Context context,
            Collector<SerializedRecord> collector) throws Exception {
        try {
            if (serializedRecord instanceof TDMsgSerializedRecord
                    && serializedRecord.getDataFlowId() == UNKNOWN_DATAFLOW_ID) {
                final TDMsgSerializedRecord tdmsgRecord = (TDMsgSerializedRecord) serializedRecord;
                synchronized (schemaLock) {
                    multiTenancyTdMsgMixedDeserializer.deserialize(
                            tdmsgRecord,
                            new CallbackCollector<>(sourceRecord -> {
                                final Record sinkRecord = fieldMappingTransformer.transform(sourceRecord);
                                collector.collect(recordTransformer.toSerializedRecord(sinkRecord));
                            }));
                }
            } else {
                // TODO, support metrics and more data types
                LOG.warn("Abandon data due to unsupported record {}", serializedRecord);
            }
        } catch (Exception e) {
            // TODO, support metrics
            LOG.warn("Abandon data", e);
        }
    }

    private class DataFlowInfoListenerImpl implements DataFlowInfoListener {

        @Override
        public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (schemaLock) {
                multiTenancyTdMsgMixedDeserializer.addDataFlow(dataFlowInfo);
                fieldMappingTransformer.addDataFlow(dataFlowInfo);
                recordTransformer.addDataFlow(dataFlowInfo);
            }
        }

        @Override
        public void updateDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (schemaLock) {
                multiTenancyTdMsgMixedDeserializer.updateDataFlow(dataFlowInfo);
                fieldMappingTransformer.updateDataFlow(dataFlowInfo);
                recordTransformer.updateDataFlow(dataFlowInfo);
            }
        }

        @Override
        public void removeDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (schemaLock) {
                multiTenancyTdMsgMixedDeserializer.removeDataFlow(dataFlowInfo);
                fieldMappingTransformer.removeDataFlow(dataFlowInfo);
                recordTransformer.removeDataFlow(dataFlowInfo);
            }
        }
    }
}
