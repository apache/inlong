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
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.formats.base.TableFormatConstants;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.inlongmsgcsv.InLongMsgCsvFormatDeserializer;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.InLongMsgCsvDeserializationInfo;
import org.apache.inlong.sort.util.CommonUtils;

public class MultiTenancyDeserializer implements DataFlowInfoListener, Deserializer<SerializedRecord, Record> {
    /**
     * Date flow id -> Deserializer.
     */
    private final Map<Long, Deserializer<SerializedRecord, Record>> deserializers = new HashMap<>();

    @Override
    public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        updateDataFlow(dataFlowInfo);
    }

    @Override
    public void updateDataFlow(DataFlowInfo dataFlowInfo) {
        final DeserializationInfo deserializationInfo = dataFlowInfo.getSourceInfo().getDeserializationInfo();

        final Deserializer<SerializedRecord, Record> deserializer = generateDeserializer(
                dataFlowInfo.getSourceInfo().getFields(), deserializationInfo);

        deserializers.put(dataFlowInfo.getId(), deserializer);
    }

    @Override
    public void removeDataFlow(DataFlowInfo dataFlowInfo) {
        deserializers.remove(dataFlowInfo.getId());
    }

    @Override
    public void deserialize(SerializedRecord record, Collector<Record> collector) throws Exception {
        final Deserializer<SerializedRecord, Record> deserializer = deserializers.get(record.getDataFlowId());
        if (deserializer == null) {
            throw new Exception("No schema found for data flow:" + record.getDataFlowId());
        }
        deserializer.deserialize(record, collector);
    }

    @VisibleForTesting
    Deserializer<SerializedRecord, Record> generateDeserializer(
            FieldInfo[] fields,
            DeserializationInfo deserializationInfo) {

        final RowFormatInfo rowFormatInfo = CommonUtils.generateDeserializationRowFormatInfo(fields);

        final Deserializer<SerializedRecord, Record> deserializer;
        if (deserializationInfo instanceof InLongMsgCsvDeserializationInfo) {
            InLongMsgCsvDeserializationInfo
                    inLongMsgCsvDeserializationInfo = (InLongMsgCsvDeserializationInfo) deserializationInfo;
            InLongMsgCsvFormatDeserializer inLongMsgCsvFormatDeserializer = new InLongMsgCsvFormatDeserializer(
                    rowFormatInfo,
                    DEFAULT_TIME_FIELD_NAME,
                    DEFAULT_ATTRIBUTES_FIELD_NAME,
                    TableFormatConstants.DEFAULT_CHARSET,
                    inLongMsgCsvDeserializationInfo.getDelimiter(),
                    null,
                    null,
                    null,
                    inLongMsgCsvDeserializationInfo.isDeleteHeadDelimiter(),
                    TableFormatConstants.DEFAULT_IGNORE_ERRORS);
            deserializer = new InLongMsgDeserializer(inLongMsgCsvFormatDeserializer);
        } else {
            // TODO, support more formats here
            throw new UnsupportedOperationException(
                    "Not supported yet " + deserializationInfo.getClass().getSimpleName());
        }

        return deserializer;
    }
}
