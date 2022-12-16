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

package org.apache.inlong.tubemq.server.broker.offset;

import java.util.HashMap;
import java.util.Map;
import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.corebase.utils.DateTimeConvertUtils;
import org.apache.inlong.tubemq.server.common.TServerConstants;

/**
 * The offset snapshot of the consumer group on the broker.
 */
public class OffsetHistoryInfo {

    private final int brokerId;
    private final String groupName;
    private final Map<String, Map<Integer, OffsetCsmRecord>> histOffsetMap = new HashMap<>();

    public OffsetHistoryInfo(int brokerId, String groupName) {
        this.brokerId = brokerId;
        this.groupName = groupName;
    }

    /**
     * Add confirmed offset of topic-partitionId.
     *
     * @param topicName      topic name
     * @param partitionId    partition id
     * @param cfmOffset      the confirmed offset
     */
    public void addCfmOffsetInfo(String topicName, int partitionId, long cfmOffset) {
        final int storeId = partitionId < TBaseConstants.META_STORE_INS_BASE
                ? 0
                : partitionId / TBaseConstants.META_STORE_INS_BASE;
        Map<Integer, OffsetCsmRecord> storeOffsetMap = histOffsetMap.get(topicName);
        if (storeOffsetMap == null) {
            Map<Integer, OffsetCsmRecord> tmpMap = new HashMap<>();
            storeOffsetMap = histOffsetMap.putIfAbsent(topicName, tmpMap);
            if (storeOffsetMap == null) {
                storeOffsetMap = tmpMap;
            }
        }
        OffsetCsmRecord offsetCsmRecord = storeOffsetMap.get(storeId);
        if (offsetCsmRecord == null) {
            OffsetCsmRecord tmpRecord = new OffsetCsmRecord(storeId);
            offsetCsmRecord = storeOffsetMap.putIfAbsent(storeId, tmpRecord);
            if (offsetCsmRecord == null) {
                offsetCsmRecord = tmpRecord;
            }
        }
        offsetCsmRecord.addOffsetCfmInfo(partitionId, cfmOffset);
    }

    /**
     * Add inflight offset of topic-partitionId.
     *
     * @param topicName      topic name
     * @param partitionId    partition id
     * @param tmpOffset      the inflight offset
     */
    public void addInflightOffsetInfo(String topicName, int partitionId, long tmpOffset) {
        final int storeId = partitionId < TBaseConstants.META_STORE_INS_BASE
                ? 0
                : partitionId / TBaseConstants.META_STORE_INS_BASE;
        Map<Integer, OffsetCsmRecord> storeOffsetMap = histOffsetMap.get(topicName);
        if (storeOffsetMap == null) {
            Map<Integer, OffsetCsmRecord> tmpMap = new HashMap<>();
            storeOffsetMap = histOffsetMap.putIfAbsent(topicName, tmpMap);
            if (storeOffsetMap == null) {
                storeOffsetMap = tmpMap;
            }
        }
        OffsetCsmRecord offsetCsmRecord = storeOffsetMap.get(storeId);
        if (offsetCsmRecord == null) {
            OffsetCsmRecord tmpRecord = new OffsetCsmRecord(storeId);
            offsetCsmRecord = storeOffsetMap.putIfAbsent(storeId, tmpRecord);
            if (offsetCsmRecord == null) {
                offsetCsmRecord = tmpRecord;
            }
        }
        offsetCsmRecord.addOffsetFetchInfo(partitionId, tmpOffset);
    }

    public String getGroupName() {
        return groupName;
    }

    public Map<String, Map<Integer, OffsetCsmRecord>> getOffsetMap() {
        return histOffsetMap;
    }

    /**
     * Build brief consumption offset information in string format
     *
     * @param strBuff     string buffer
     * @param dataTime    record build time
     */
    public void buildRecordInfo(StringBuilder strBuff, long dataTime) {
        int topicCnt = 0;
        strBuff.append("{\"dt\":\"")
                .append(DateTimeConvertUtils.ms2yyyyMMddHHmmss(dataTime))
                .append("\",\"bId\":").append(brokerId)
                .append(",\"ver\":").append(TServerConstants.OFFSET_HISTORY_RECORD_SHORT_VERSION)
                .append(",\"records\":[");
        for (Map.Entry<String, Map<Integer, OffsetCsmRecord>> entry : histOffsetMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (topicCnt++ > 0) {
                strBuff.append(",");
            }
            int recordCnt = 0;
            strBuff.append("{\"topic\":\"").append(entry.getKey()).append("\",\"offsets\":[");
            Map<Integer, OffsetCsmRecord> csmOffsetRecordMap = entry.getValue();
            for (OffsetCsmRecord offsetRecord : csmOffsetRecordMap.values()) {
                if (offsetRecord == null) {
                    continue;
                }
                if (recordCnt++ > 0) {
                    strBuff.append(",");
                }
                strBuff.append("{\"storeId\":").append(offsetRecord.storeId)
                        .append(",\"iMin\":").append(offsetRecord.offsetMin)
                        .append(",\"iMax\":").append(offsetRecord.offsetMax)
                        .append(",\"dMin\":").append(offsetRecord.dataMin)
                        .append(",\"dMax\":").append(offsetRecord.dataMax)
                        .append(",\"parts\":[");
                int partCnt = 0;
                for (OffsetCsmItem csmOffsetItem : offsetRecord.partitionCsmMap.values()) {
                    if (csmOffsetItem == null) {
                        continue;
                    }
                    if (partCnt++ > 0) {
                        strBuff.append(",");
                    }
                    strBuff.append("{\"partId\":").append(csmOffsetItem.partitionId)
                            .append(",\"iCfm\":").append(csmOffsetItem.offsetCfm)
                            .append("}");
                }
                strBuff.append("]}");
            }
            strBuff.append("]}");
        }
        strBuff.append("]}");
    }
}
