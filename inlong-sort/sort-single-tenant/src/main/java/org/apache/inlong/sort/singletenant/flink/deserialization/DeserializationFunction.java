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

package org.apache.inlong.sort.singletenant.flink.deserialization;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.audit.AuditImp;
import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.singletenant.flink.SerializedRecord;

import java.util.Iterator;

public class DeserializationFunction extends ProcessFunction<SerializedRecord, Row> {

    private static final long serialVersionUID = 6245991845787657154L;

    private final DeserializationSchema<Row> deserializationSchema;

    private final FieldMappingTransformer fieldMappingTransformer;

    private final boolean appendAttributes;

    private final Configuration config;

    private final String inLongGroupId;

    private final String inLongStreamId;

    private transient AuditImp auditImp;

    public DeserializationFunction(
            DeserializationSchema<Row> deserializationSchema,
            FieldMappingTransformer fieldMappingTransformer,
            boolean appendAttributes,
            Configuration config,
            String inLongGroupId,
            String inLongStreamId) {
        this.deserializationSchema = deserializationSchema;
        this.fieldMappingTransformer = fieldMappingTransformer;
        this.appendAttributes = appendAttributes;
        this.config = Preconditions.checkNotNull(config);
        this.inLongGroupId = inLongGroupId;
        this.inLongStreamId = inLongStreamId;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        String auditHostAndPorts = config.getString(Constants.METRICS_AUDIT_PROXY_HOSTS);
        if (auditHostAndPorts != null) {
            AuditImp.getInstance().setAuditProxy(new HashSet<>(Arrays.asList(auditHostAndPorts.split(","))));
            auditImp = AuditImp.getInstance();
        }
    }

    @Override
    public void processElement(
            SerializedRecord value,
            ProcessFunction<SerializedRecord, Row>.Context ctx,
            Collector<Row> out
    ) throws Exception {
        InLongMsg inLongMsg = InLongMsg.parseFrom(value.getData());

        for (String attr : inLongMsg.getAttrs()) {
            Iterator<byte[]> iterator = inLongMsg.getIterator(attr);
            if (iterator == null) {
                continue;
            }

            while (iterator.hasNext()) {

                byte[] bodyBytes = iterator.next();
                long bodyLength = bodyBytes == null ? 0 : bodyBytes.length;

                // Currently, we can not get the number of records in source because they are packed in InLongMsg.
                // So we output metrics for input here.
                if (auditImp != null) {
                    auditImp.add(
                            Constants.METRIC_AUDIT_ID_FOR_INPUT,
                            inLongGroupId,
                            inLongStreamId,
                            value.getTimestampMillis(),
                            1,
                            bodyLength);
                }

                if (bodyLength == 0) {
                    continue;
                }

                deserializationSchema.deserialize(bodyBytes, new CallbackCollector<>(inputRow -> {
                    if (appendAttributes) {
                        inputRow = Row.join(Row.of(new HashMap<>()), inputRow);
                    }

                    // Currently, the transformer operator and the sink operator do not discard data.
                    // So we simply output metrics for output here.
                    if (auditImp != null) {
                        auditImp.add(
                                Constants.METRIC_AUDIT_ID_FOR_OUTPUT,
                                inLongGroupId,
                                inLongStreamId,
                                value.getTimestampMillis(),
                                1,
                                bodyLength);
                    }

                    out.collect(fieldMappingTransformer.transform(inputRow, value.getTimestampMillis()));
                }));
            }
        }
    }

    @Override
    public void close() {
        if (auditImp != null) {
            auditImp.sendReport();
        }
    }
}
