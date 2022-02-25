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

import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;

import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.InLongMsgMixedSerializedRecord;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.AbstractInLongMsgMixedFormatDeserializer;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgMixedFormatConverter;
import org.apache.inlong.sort.formats.inlongmsgcsv.InLongMsgCsvMixedFormatConverter;
import org.apache.inlong.sort.formats.inlongmsgcsv.InLongMsgCsvMixedFormatDeserializer;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.InLongMsgCsvDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.InLongMsgDeserializationInfo;
import org.apache.inlong.sort.protocol.source.PulsarSourceInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.apache.inlong.sort.util.CommonUtils;

/**
 * A deserializer to handle mixed InLongMsg records.
 */
public class MultiTenancyInLongMsgMixedDeserializer implements DataFlowInfoListener,
        Deserializer<InLongMsgMixedSerializedRecord, Record> {

    /**
     * Maps topic to mixed deserializer.
     */
    private final Map<String, InLongMsgMixedDeserializer> mixedDeserializerMap = new HashMap<>();

    @Override
    public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        updateDataFlow(dataFlowInfo);
    }

    @Override
    public void updateDataFlow(DataFlowInfo dataFlowInfo) {
        if (!isInLongMsgDataFlow(dataFlowInfo)) {
            return;
        }
        final InLongMsgDeserializationInfo inLongMsgDeserializationInfo =
                (InLongMsgDeserializationInfo) dataFlowInfo.getSourceInfo().getDeserializationInfo();

        Pair<AbstractInLongMsgMixedFormatDeserializer, InLongMsgMixedFormatConverter>
                allDeserializer = generateDeserializer(
                dataFlowInfo.getSourceInfo().getFields(), inLongMsgDeserializationInfo);
        final AbstractInLongMsgMixedFormatDeserializer preDeserializer = allDeserializer.getLeft();
        final InLongMsgMixedFormatConverter deserializer = allDeserializer.getRight();

        final String topic;
        if (dataFlowInfo.getSourceInfo() instanceof TubeSourceInfo) {
            topic = ((TubeSourceInfo) dataFlowInfo.getSourceInfo()).getTopic();
        } else if (dataFlowInfo.getSourceInfo() instanceof PulsarSourceInfo) {
            topic = ((PulsarSourceInfo) dataFlowInfo.getSourceInfo()).getTopic();
        } else {
            throw new UnsupportedOperationException("Unknown source type " + dataFlowInfo.getSourceInfo());
        }

        final InLongMsgMixedDeserializer mixedDeserializer = mixedDeserializerMap
                 .computeIfAbsent(topic, key -> new InLongMsgMixedDeserializer());
        mixedDeserializer.updateDataFlow(
                dataFlowInfo.getId(), inLongMsgDeserializationInfo.getTid(), preDeserializer, deserializer);
    }

    @Override
    public void removeDataFlow(DataFlowInfo dataFlowInfo) {
        if (!isInLongMsgDataFlow(dataFlowInfo)) {
            return;
        }
        final TubeSourceInfo tubeSourceInfo = (TubeSourceInfo) dataFlowInfo.getSourceInfo();
        final InLongMsgDeserializationInfo inLongMsgDeserializationInfo = (InLongMsgDeserializationInfo) tubeSourceInfo
                .getDeserializationInfo();
        final InLongMsgMixedDeserializer mixedDeserializer = mixedDeserializerMap.get(tubeSourceInfo.getTopic());
        if (mixedDeserializer != null) {
            mixedDeserializer.removeDataFlow(dataFlowInfo.getId(), inLongMsgDeserializationInfo.getTid());
            if (mixedDeserializer.isEmpty()) {
                mixedDeserializerMap.remove(tubeSourceInfo.getTopic());
            }
        }
    }

    @VisibleForTesting
    static boolean isInLongMsgDataFlow(DataFlowInfo dataFlowInfo) {
        final DeserializationInfo deserializationInfo = dataFlowInfo.getSourceInfo().getDeserializationInfo();
        return deserializationInfo instanceof InLongMsgDeserializationInfo;
    }

    public void deserialize(InLongMsgMixedSerializedRecord record, Collector<Record> collector) throws Exception {
        final String topic = record.getTopic();
        final InLongMsgMixedDeserializer mixedDeserializer = mixedDeserializerMap.get(topic);
        if (mixedDeserializer == null) {
            throw new Exception("No schema found for topic:" + topic);
        }
        mixedDeserializer.deserialize(record, collector);
    }

    @VisibleForTesting
    Pair<AbstractInLongMsgMixedFormatDeserializer, InLongMsgMixedFormatConverter> generateDeserializer(
            FieldInfo[] fields,
            InLongMsgDeserializationInfo inLongMsgDeserializationInfo) {

        final RowFormatInfo rowFormatInfo = CommonUtils.generateDeserializationRowFormatInfo(fields);

        final AbstractInLongMsgMixedFormatDeserializer preDeserializer;
        final InLongMsgMixedFormatConverter deserializer;
        if (inLongMsgDeserializationInfo instanceof InLongMsgCsvDeserializationInfo) {
            final InLongMsgCsvDeserializationInfo csvDeserializationInfo =
                    (InLongMsgCsvDeserializationInfo) inLongMsgDeserializationInfo;
            preDeserializer = new InLongMsgCsvMixedFormatDeserializer(
                    StandardCharsets.UTF_8.name(),
                    csvDeserializationInfo.getDelimiter(),
                    null,
                    null,
                    csvDeserializationInfo.isDeleteHeadDelimiter(),
                    false);
            deserializer = new InLongMsgCsvMixedFormatConverter(
                    rowFormatInfo,
                    DEFAULT_TIME_FIELD_NAME,
                    DEFAULT_ATTRIBUTES_FIELD_NAME,
                    null,
                    false);
        } else {
            throw new UnsupportedOperationException(
                    "Not supported yet " + inLongMsgDeserializationInfo.getClass().getSimpleName());
        }

        return Pair.of(preDeserializer, deserializer);
    }
}