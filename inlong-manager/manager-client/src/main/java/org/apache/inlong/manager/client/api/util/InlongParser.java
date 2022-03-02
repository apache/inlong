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
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.EventLogView;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;

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
        WorkflowResult workflowResult = GsonUtil.fromJson(resultData, WorkflowResult.class);
        return workflowResult;
    }

    public static InlongGroupResponse parseGroupInfo(Response response) {
        Object data = response.getData();
        return GsonUtil.fromJson(GsonUtil.toJson(data), InlongGroupResponse.class);
    }

    public static PageInfo<InlongGroupListResponse> parseGroupList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtil.toJson(data);
        PageInfo<InlongGroupListResponse> pageInfo = GsonUtil.fromJson(pageInfoJson,
                new TypeToken<PageInfo<InlongGroupListResponse>>() {
                }.getType());
        return pageInfo;
    }

    public static InlongStreamInfo parseStreamInfo(Response response) {
        Object data = response.getData();
        InlongStreamInfo streamInfo = GsonUtil.fromJson(GsonUtil.toJson(data), InlongStreamInfo.class);
        return streamInfo;
    }

    public static PageInfo<FullStreamResponse> parseStreamList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtil.toJson(data);
        PageInfo<FullStreamResponse> pageInfo = GsonUtil.fromJson(pageInfoJson,
                new TypeToken<PageInfo<FullStreamResponse>>() {
                }.getType());
        return pageInfo;
    }

    public static PageInfo<SourceListResponse> parseSourceList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtil.toJson(data);
        PageInfo<SourceListResponse> pageInfo = GsonUtil.fromJson(pageInfoJson,
                new TypeToken<PageInfo<SourceListResponse>>() {
                }.getType());
        return pageInfo;
    }

    public static PageInfo<SinkListResponse> parseSinkList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtil.toJson(data);
        PageInfo<SinkListResponse> pageInfo = GsonUtil.fromJson(pageInfoJson,
                new TypeToken<PageInfo<SinkListResponse>>() {
                }.getType());
        return pageInfo;
    }

    public static Pair<InlongGroupApproveRequest, List<InlongStreamApproveRequest>> parseGroupForm(String formJson) {
        final String groupInfoField = "groupInfo";
        final String mqExtInfoField = "mqExtInfo";
        JsonObject formData = GsonUtil.fromJson(formJson, JsonObject.class);
        JsonObject groupJson = formData.getAsJsonObject(groupInfoField);
        InlongGroupApproveRequest groupApproveInfo = GsonUtil.fromJson(groupJson.toString(),
                InlongGroupApproveRequest.class);
        JsonObject mqExtInfo = groupJson.getAsJsonObject(mqExtInfoField);
        if (mqExtInfo.get("middlewareType").getAsString().equals("PULSAR")) {
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
        PageInfo<EventLogView> pageInfo = GsonUtil.fromJson(pageInfoJson,
                new TypeToken<PageInfo<EventLogView>>() {
                }.getType());
        return pageInfo;
    }
}
