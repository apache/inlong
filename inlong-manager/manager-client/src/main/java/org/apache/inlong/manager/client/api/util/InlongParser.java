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

import com.github.pagehelper.PageInfo;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceListResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.EventLogView;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;

import java.util.List;

import static org.apache.inlong.manager.common.enums.SourceType.BINLOG;
import static org.apache.inlong.manager.common.enums.SourceType.KAFKA;

/**
 * Parser for Inlong entity
 */
public class InlongParser {

    public static Response parseResponse(String responseBody) {
        Response response = GsonUtil.fromJson(responseBody, Response.class);
        return response;
    }

    public static WorkflowResult parseWorkflowResult(Response response) {
        Object data = response.getData();
        String resultData = GsonUtil.toJson(data);
        return GsonUtil.fromJson(resultData, WorkflowResult.class);
    }

    public static InlongGroupResponse parseGroupInfo(Response response) {
        Object data = response.getData();
        return GsonUtil.fromJson(GsonUtil.toJson(data), InlongGroupResponse.class);
    }

    public static PageInfo<InlongGroupListResponse> parseGroupList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtil.toJson(data);
        return GsonUtil.fromJson(pageInfoJson,
                new TypeToken<PageInfo<InlongGroupListResponse>>() {
                }.getType());
    }

    public static InlongStreamInfo parseStreamInfo(Response response) {
        Object data = response.getData();
        return GsonUtil.fromJson(GsonUtil.toJson(data), InlongStreamInfo.class);
    }

    public static PageInfo<FullStreamResponse> parseStreamList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtil.toJson(data);
        return GsonUtil.fromJson(pageInfoJson,
                new TypeToken<PageInfo<FullStreamResponse>>() {
                }.getType());
    }

    public static PageInfo<SourceListResponse> parseSourceList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtil.toJson(data);
        PageInfo<SourceListResponse> pageInfo = GsonUtil.fromJson(pageInfoJson,
                new TypeToken<PageInfo<SourceListResponse>>() {
                }.getType());
        if (pageInfo.getList() != null && !pageInfo.getList().isEmpty()) {
            SourceListResponse sourceListResponse = pageInfo.getList().get(0);
            SourceType sourceType = SourceType.forType(sourceListResponse.getSourceType());
            if (sourceType == BINLOG) {
                return GsonUtil.fromJson(pageInfoJson,
                        new TypeToken<PageInfo<BinlogSourceListResponse>>() {
                        }.getType());
            }
            if (sourceType == KAFKA) {
                return GsonUtil.fromJson(pageInfoJson,
                        new TypeToken<PageInfo<KafkaSourceListResponse>>() {
                        }.getType());
            }
            throw new IllegalArgumentException(
                    String.format("Unsupported sourceType=%s for Inlong", sourceType));

        }
        throw new IllegalArgumentException(String.format("pageInfo is empty for Inlong"));
    }

    public static PageInfo<SinkListResponse> parseSinkList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtil.toJson(data);
        return GsonUtil.fromJson(pageInfoJson,
                new TypeToken<PageInfo<SinkListResponse>>() {
                }.getType());
    }

    public static Pair<InlongGroupApproveRequest, List<InlongStreamApproveRequest>> parseGroupForm(String formJson) {
        final String groupInfoField = "groupInfo";
        final String mqExtInfoField = "mqExtInfo";
        JsonObject formData = GsonUtil.fromJson(formJson, JsonObject.class);
        JsonObject groupJson = formData.getAsJsonObject(groupInfoField);
        InlongGroupApproveRequest groupApproveInfo = GsonUtil.fromJson(groupJson.toString(),
                InlongGroupApproveRequest.class);
        JsonObject mqExtInfo = groupJson.getAsJsonObject(mqExtInfoField);
        if (mqExtInfo != null && mqExtInfo.get("middlewareType") != null
                && Constant.MIDDLEWARE_PULSAR.equals(mqExtInfo.get("middlewareType").getAsString())) {
            InlongGroupPulsarInfo pulsarInfo = GsonUtil.fromJson(mqExtInfo.toString(), InlongGroupPulsarInfo.class);
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
        JsonArray streamJson = formData.getAsJsonArray("streamInfoList");
        List<InlongStreamApproveRequest> streamApproveList = GsonUtil.fromJson(streamJson.toString(),
                new TypeToken<List<InlongStreamApproveRequest>>() {
                }.getType());
        return Pair.of(groupApproveInfo, streamApproveList);
    }

    public static PageInfo<EventLogView> parseEventLogViewList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtil.toJson(data);
        return GsonUtil.fromJson(pageInfoJson,
                new TypeToken<PageInfo<EventLogView>>() {
                }.getType());
    }

}
