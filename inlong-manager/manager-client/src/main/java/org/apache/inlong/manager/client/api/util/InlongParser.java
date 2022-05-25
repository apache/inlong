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

package org.apache.inlong.manager.client.api.util;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarDTO;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;
import org.apache.inlong.manager.common.util.AssertUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import java.util.List;

/**
 * Parser for Inlong entity
 */
@Slf4j
@UtilityClass
public class InlongParser {

    public static final String GROUP_INFO = "groupInfo";
    public static final String MQ_EXT_INFO = "mqExtInfo";
    public static final String MQ_TYPE = "mqType";
    public static final String SINK_INFO = "sinkInfo";
    public static final String SOURCE_INFO = "sourceInfo";
    public static final String SINK_TYPE = "sinkType";
    public static final String SOURCE_TYPE = "sourceType";

    public static <T> Response<T> parseResponse(String responseBody, Class<T> resDataType) {
        AssertUtils.notNull(resDataType, "resDataType must not be null");

        JavaType javaType = new ObjectMapper()
                .getTypeFactory()
                .constructParametricType(Response.class, resDataType);
        return JsonUtils.parseObject(responseBody, javaType);
    }

    /**
     * Parse forms of group.
     */
    public static Pair<InlongGroupApproveRequest, List<InlongStreamApproveRequest>> parseGroupForm(String formJson) {
        JsonObject formData = GsonUtils.fromJson(formJson, JsonObject.class);
        JsonObject groupJson = formData.getAsJsonObject(GROUP_INFO);
        InlongGroupApproveRequest groupApproveInfo = GsonUtils.fromJson(groupJson.toString(),
                InlongGroupApproveRequest.class);
        JsonObject mqExtInfo = groupJson.getAsJsonObject(MQ_EXT_INFO);
        if (mqExtInfo != null && mqExtInfo.get(MQ_TYPE) != null) {
            MQType mqType = MQType.forType(mqExtInfo.get(MQ_TYPE).getAsString());
            if (mqType == MQType.PULSAR || mqType == MQType.TDMQ_PULSAR) {
                InlongPulsarDTO pulsarInfo = GsonUtils.fromJson(mqExtInfo.toString(),
                        InlongPulsarDTO.class);
                groupApproveInfo.setAckQuorum(pulsarInfo.getAckQuorum());
                groupApproveInfo.setEnsemble(pulsarInfo.getEnsemble());
                groupApproveInfo.setWriteQuorum(pulsarInfo.getWriteQuorum());
                groupApproveInfo.setRetentionTime(pulsarInfo.getRetentionTime());
                groupApproveInfo.setRetentionTimeUnit(pulsarInfo.getRetentionTimeUnit());
                groupApproveInfo.setTtl(pulsarInfo.getTtl());
                groupApproveInfo.setTtlUnit(pulsarInfo.getTtlUnit());
                groupApproveInfo.setRetentionSize(pulsarInfo.getRetentionSize());
                groupApproveInfo.setRetentionSizeUnit(pulsarInfo.getRetentionSizeUnit());
            }
        }
        JsonArray streamJson = formData.getAsJsonArray("streamInfoList");
        List<InlongStreamApproveRequest> streamApproveList = GsonUtils.fromJson(streamJson.toString(),
                new TypeToken<List<InlongStreamApproveRequest>>() {
                }.getType());
        return Pair.of(groupApproveInfo, streamApproveList);
    }

}
