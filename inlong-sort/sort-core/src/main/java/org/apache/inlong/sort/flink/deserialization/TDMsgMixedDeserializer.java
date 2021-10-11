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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.TDMsgSerializedRecord;
import org.apache.inlong.sort.formats.tdmsg.AbstractTDMsgFormatDeserializer;
import org.apache.inlong.sort.formats.tdmsg.TDMsgMixedFormatConverter;
import org.apache.inlong.sort.formats.tdmsg.TDMsgUtils;

/**
 * A deserializer to handle mixed TDMsg records of one topic.
 */
public class TDMsgMixedDeserializer implements Deserializer<TDMsgSerializedRecord, Record> {

    /**
     * Each topic should have same preDeserializer, so just keep one.
     */
    private AbstractTDMsgFormatDeserializer preDeserializer;

    /**
     * Tid -> deserializer.
     */
    private final Map<String, TDMsgMixedFormatConverter> deserializers = new HashMap<>();

    /**
     * Tid -> data flow ids.
     */
    private final Map<String, Set<Long>> interface2DataFlowsMap = new HashMap<>();

    public TDMsgMixedDeserializer() {
    }

    public void updateDataFlow(long dataFlowId, String tid, AbstractTDMsgFormatDeserializer preDeserializer,
            TDMsgMixedFormatConverter deserializer) {
        // always updates preDeserializer
        this.preDeserializer = preDeserializer;
        deserializers.put(tid, deserializer);
        interface2DataFlowsMap.computeIfAbsent(tid, k -> new HashSet<>()).add(dataFlowId);
    }

    public void removeDataFlow(long dataFlowId, String tid) {
        deserializers.remove(tid);
        final Set<Long> dataFlows = interface2DataFlowsMap.get(tid);
        if (dataFlows != null) {
            dataFlows.remove(dataFlowId);
            if (dataFlows.isEmpty()) {
                interface2DataFlowsMap.remove(tid);
            }
        }
    }

    public boolean isEmpty() {
        return interface2DataFlowsMap.isEmpty();
    }

    @Override
    public void deserialize(TDMsgSerializedRecord tdMsgRecord, Collector<Record> collector) throws Exception {
        preDeserializer.flatMap(tdMsgRecord.getData(), new CallbackCollector<>(mixedRow -> {
            final String tid = TDMsgUtils.getTidFromMixedRow(mixedRow);
            final Set<Long> dataFlowIds = interface2DataFlowsMap.get(tid);
            if (dataFlowIds.isEmpty()) {
                throw new Exception("No data flow found for tid:" + tid);
            }
            final TDMsgMixedFormatConverter deserializer = deserializers.get(tid);
            if (deserializer == null) {
                throw new Exception("No data flow found for tid:" + tid);
            }
            deserializer.flatMap(mixedRow, new CallbackCollector<>((row -> {
                // each tid might be associated with multiple data flows
                for (long dataFlowId : dataFlowIds) {
                    collector.collect(new Record(dataFlowId, System.currentTimeMillis(), row));
                }
            })));
        }));
    }

    @VisibleForTesting
    AbstractTDMsgFormatDeserializer getPreDeserializer() {
        return preDeserializer;
    }

    @VisibleForTesting
    Map<String, TDMsgMixedFormatConverter> getDeserializers() {
        return deserializers;
    }

    @VisibleForTesting
    Map<String, Set<Long>> getInterface2DataFlowsMap() {
        return interface2DataFlowsMap;
    }
}
