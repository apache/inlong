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

package org.apache.inlong.tubemq.server.broker.offset.topicpub;

import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;
import org.apache.inlong.tubemq.corebase.utils.Tuple4;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The offset snapshot of the topic.
 */
public class TopicPubInfo {

    private final String topicName;
    Map<Integer, OffsetPubItem> storeOffsetMap = new HashMap<>();

    public TopicPubInfo(String topicName) {
        this.topicName = topicName;
    }

    public void addStorePubInfo(int storeId, int numPart,
            long indexMinOffset, long indexMaxOffset,
            long dataMinOffset, long dataMaxOffset) {
        storeOffsetMap.put(storeId, new OffsetPubItem(storeId, numPart,
                indexMinOffset, indexMaxOffset, dataMinOffset, dataMaxOffset));
    }

    public String getTopicName() {
        return topicName;
    }

    public OffsetPubItem getTopicStorePubInfo(int storeId) {
        return storeOffsetMap.get(storeId);
    }

    public Map<Integer, OffsetPubItem> getStoreOffsetMap() {
        return storeOffsetMap;
    }

    /**
     * Build brief topic produce offset information in string format
     *
     * @param brokerId    node id
     * @param strBuff     string buffer
     * @param dataTime    record build time
     */
    public void buildRecordInfo(int brokerId, StringBuilder strBuff, long dataTime) {
        int itemCnt = 0;
        strBuff.append("{\"dt\":").append(dataTime)
                .append(",\"bId\":").append(brokerId)
                .append(",\"ver\":").append(TBaseConstants.OFFSET_TOPIC_PUBLISH_RECORD_VERSION_1)
                .append(",\"topic\":\"").append(topicName)
                .append("\",\"offsets\":[");
        for (Map.Entry<Integer, OffsetPubItem> entry : storeOffsetMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (itemCnt++ > 0) {
                strBuff.append(",");
            }
            strBuff.append("{\"storeId\":").append(entry.getValue().getStoreId())
                    .append(",\"numPart\":").append(entry.getValue().getNumPart())
                    .append(",\"iMin\":").append(entry.getValue().getIndexMin())
                    .append(",\"iMax\":").append(entry.getValue().getIndexMax())
                    .append(",\"dMin\":").append(entry.getValue().getDataMin())
                    .append(",\"dMax\":").append(entry.getValue().getDataMax())
                    .append("}");
        }
        strBuff.append("]}");
    }

    /**
     * Parse produce offset record info
     *
     * @param jsonData  string offset information
     * @param result    process result
     */
    public static boolean parseRecordInfo(String topicName,
            int curNumStores, int curNumParts,
            String jsonData, ProcessResult result) {
        JsonObject jsonObject;
        // parse record
        try {
            jsonObject = JsonParser.parseString(jsonData).getAsJsonObject();
        } catch (Throwable e1) {
            result.setFailResult(String.format(
                    "Parse topic %s offset value failure, reason is %s", topicName, e1.getMessage()));
            return result.isSuccess();
        }
        // check record
        if (jsonObject == null) {
            result.setFailResult(String.format(
                    "Parse error, topic %s offset value must be valid json format!", topicName));
            return result.isSuccess();
        }
        if (!isFieldExist(jsonObject, topicName, "ver", result)
                || !isFieldExist(jsonObject, topicName, "dt", result)
                || !isFieldExist(jsonObject, topicName, "offsets", result)
                || !isFieldExist(jsonObject, topicName, "topic", result)) {
            return result.isSuccess();
        }
        int verValue = jsonObject.get("ver").getAsInt();
        if (verValue != TBaseConstants.OFFSET_TOPIC_PUBLISH_RECORD_VERSION_1) {
            result.setFailResult(String.format(
                    "Un-support %d version in %s's offset value!", verValue, topicName));
            return result.isSuccess();
        }
        long dataTime = jsonObject.get("dt").getAsLong();
        String targetTopic = jsonObject.get("topic").getAsString();
        if (!topicName.equals(targetTopic)) {
            result.setFailResult(String.format(
                    "FIELD topic value %s not equals required value %s!", targetTopic, topicName));
            return result.isSuccess();
        }
        // get offsets
        int storeId;
        int baseValue;
        long offsetVal;
        JsonObject itemInfo;
        JsonArray records = jsonObject.get("offsets").getAsJsonArray();
        List<Tuple4<Long, String, Integer, Long>> resetOffsets = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            itemInfo = records.get(i).getAsJsonObject();
            if (itemInfo == null) {
                continue;
            }
            storeId = itemInfo.get("storeId").getAsInt();
            offsetVal = itemInfo.get("iMax").getAsLong();
            baseValue = storeId * TBaseConstants.META_STORE_INS_BASE;
            for (int j = 0; j < curNumParts; j++) {
                resetOffsets.add(new Tuple4<>(dataTime, topicName, baseValue + j, offsetVal));
            }
        }
        for (int i = records.size(); i < curNumStores; i++) {
            baseValue = i * TBaseConstants.META_STORE_INS_BASE;
            for (int j = 0; j < curNumParts; j++) {
                resetOffsets.add(new Tuple4<>(dataTime, topicName, baseValue + j, 0L));
            }
        }
        result.setSuccResult(resetOffsets);
        return result.isSuccess();
    }

    private static boolean isFieldExist(
            JsonObject jsonObject, String topic, String key, ProcessResult result) {
        if (!jsonObject.has(key)) {
            result.setFailResult(String.format(
                    "FIELD %s is required in %s's offset value!", key, topic));
            return result.isSuccess();
        }
        return true;
    }
}
