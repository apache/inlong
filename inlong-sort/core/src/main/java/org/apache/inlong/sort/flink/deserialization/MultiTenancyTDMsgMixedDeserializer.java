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

import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.DEFAULT_TIME_FIELD_NAME;

import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.TDMsgSerializedRecord;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.tdmsg.AbstractTDMsgMixedFormatDeserializer;
import org.apache.inlong.sort.formats.tdmsg.TDMsgMixedFormatConverter;
import org.apache.inlong.sort.formats.tdmsgcsv.TDMsgCsvMixedFormatConverter;
import org.apache.inlong.sort.formats.tdmsgcsv.TDMsgCsvMixedFormatDeserializer;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgCsv2DeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgCsvDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgKvDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgTlogCsvDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgTlogKvDeserializationInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.apache.inlong.sort.util.CommonUtils;

/**
 * A deserializer to handle mixed TDMsg records.
 */
public class MultiTenancyTDMsgMixedDeserializer implements DataFlowInfoListener,
        Deserializer<TDMsgSerializedRecord, Record> {

    /**
     * Maps topic to mixed deserializer.
     */
    private final Map<String, TDMsgMixedDeserializer> mixedDeserializerMap = new HashMap<>();

    @Override
    public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        updateDataFlow(dataFlowInfo);
    }

    @Override
    public void updateDataFlow(DataFlowInfo dataFlowInfo) {
        if (!isTDMsgDataFlow(dataFlowInfo)) {
            return;
        }
        final TDMsgDeserializationInfo tdMsgDeserializationInfo =
                (TDMsgDeserializationInfo) dataFlowInfo.getSourceInfo().getDeserializationInfo();

        Pair<AbstractTDMsgMixedFormatDeserializer, TDMsgMixedFormatConverter> allDeserializer = generateDeserializer(
                dataFlowInfo.getSourceInfo().getFields(), tdMsgDeserializationInfo);
        final AbstractTDMsgMixedFormatDeserializer preDeserializer = allDeserializer.getLeft();
        final TDMsgMixedFormatConverter deserializer = allDeserializer.getRight();

        // currently only tubeMQ source supports TDMsg format
        final TubeSourceInfo tubeSourceInfo = (TubeSourceInfo) dataFlowInfo.getSourceInfo();
        final TDMsgMixedDeserializer mixedDeserializer = mixedDeserializerMap
                .computeIfAbsent(tubeSourceInfo.getTopic(), topic -> new TDMsgMixedDeserializer());
        mixedDeserializer.updateDataFlow(
                dataFlowInfo.getId(), tdMsgDeserializationInfo.getTid(), preDeserializer, deserializer);
    }

    @Override
    public void removeDataFlow(DataFlowInfo dataFlowInfo) {
        if (!isTDMsgDataFlow(dataFlowInfo)) {
            return;
        }
        final TubeSourceInfo tubeSourceInfo = (TubeSourceInfo) dataFlowInfo.getSourceInfo();
        final TDMsgDeserializationInfo tdMsgDeserializationInfo = (TDMsgDeserializationInfo) tubeSourceInfo
                .getDeserializationInfo();
        final TDMsgMixedDeserializer mixedDeserializer = mixedDeserializerMap.get(tubeSourceInfo.getTopic());
        if (mixedDeserializer != null) {
            mixedDeserializer.removeDataFlow(dataFlowInfo.getId(), tdMsgDeserializationInfo.getTid());
            if (mixedDeserializer.isEmpty()) {
                mixedDeserializerMap.remove(tubeSourceInfo.getTopic());
            }
        }
    }

    @VisibleForTesting
    static boolean isTDMsgDataFlow(DataFlowInfo dataFlowInfo) {
        final DeserializationInfo deserializationInfo = dataFlowInfo.getSourceInfo().getDeserializationInfo();
        return deserializationInfo instanceof TDMsgDeserializationInfo;
    }

    public void deserialize(TDMsgSerializedRecord record, Collector<Record> collector) throws Exception {
        final String topic = record.getTopic();
        final TDMsgMixedDeserializer mixedDeserializer = mixedDeserializerMap.get(topic);
        if (mixedDeserializer == null) {
            throw new Exception("No schema found for topic:" + topic);
        }
        mixedDeserializer.deserialize(record, collector);
    }

    @VisibleForTesting
    Pair<AbstractTDMsgMixedFormatDeserializer, TDMsgMixedFormatConverter> generateDeserializer(
            FieldInfo[] fields,
            TDMsgDeserializationInfo tdMsgDeserializationInfo) {

        final RowFormatInfo rowFormatInfo =
                CommonUtils.generateRowFormatInfo(fields);

        final AbstractTDMsgMixedFormatDeserializer preDeserializer;
        final TDMsgMixedFormatConverter deserializer;
        if (tdMsgDeserializationInfo instanceof TDMsgCsvDeserializationInfo) {
            final TDMsgCsvDeserializationInfo csvDeserializationInfo =
                    (TDMsgCsvDeserializationInfo) tdMsgDeserializationInfo;
            preDeserializer = new TDMsgCsvMixedFormatDeserializer(
                    StandardCharsets.UTF_8.name(),
                    csvDeserializationInfo.getDelimiter(),
                    null,
                    null,
                    csvDeserializationInfo.isDeleteHeadDelimiter(),
                    false);
            deserializer = new TDMsgCsvMixedFormatConverter(
                    rowFormatInfo,
                    DEFAULT_TIME_FIELD_NAME,
                    DEFAULT_ATTRIBUTES_FIELD_NAME,
                    null,
                    false);
        } else {
            throw new UnsupportedOperationException(
                    "Not supported yet " + tdMsgDeserializationInfo.getClass().getSimpleName());
        }

        return Pair.of(preDeserializer, deserializer);
    }
}