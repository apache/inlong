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
import javafx.util.Pair;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.business.BusinessApproveInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessPulsarInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamApproveInfo;
import org.apache.inlong.manager.common.pojo.datastream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;

public class InlongParser {

    public static Response parseResponse(String responseBody) {
        Response response = GsonUtil.fromJson(responseBody, Response.class);
        return response;
    }

    public static WorkflowResult parseWorkflowResult(Response response) {
        Object data = response.getData();
        WorkflowResult workflowResult = GsonUtil.fromJson(GsonUtil.toJson(data), WorkflowResult.class);
        return workflowResult;
    }

    public static BusinessInfo parseBusinessInfo(Response response) {
        Object data = response.getData();
        BusinessInfo businessInfo = GsonUtil.fromJson(GsonUtil.toJson(data), BusinessInfo.class);
        return businessInfo;
    }

    public static PageInfo<FullStreamResponse> parseStreamList(Response response) {
        Object data = response.getData();
        PageInfo<FullStreamResponse> pageInfo = GsonUtil.fromJson(GsonUtil.toJson(data),
                new TypeToken<PageInfo<FullStreamResponse>>() {
                }.getType());
        return pageInfo;
    }

    public static Pair<BusinessApproveInfo, List<DataStreamApproveInfo>> parseBusinessForm(String formJson) {
        final String businessInfoField = "businessInfo";
        final String mqExtInfoField = "mqExtInfo";
        JsonObject formData = GsonUtil.fromJson(formJson, JsonObject.class);
        JsonObject businessJson = formData.getAsJsonObject(businessInfoField);
        BusinessApproveInfo businessApproveInfo = GsonUtil.fromJson(businessJson.toString(), BusinessApproveInfo.class);
        JsonObject mqExtInfo = businessJson.getAsJsonObject(mqExtInfoField);
        if (mqExtInfo.get("middlewareType").getAsString().equals("PULSAR")) {
            BusinessPulsarInfo businessPulsarInfo = GsonUtil.fromJson(mqExtInfo.toString(), BusinessPulsarInfo.class);
            businessApproveInfo.setAckQuorum(businessPulsarInfo.getAckQuorum());
            businessApproveInfo.setEnsemble(businessPulsarInfo.getEnsemble());
            businessApproveInfo.setWriteQuorum(businessPulsarInfo.getWriteQuorum());
            businessApproveInfo.setRetentionTime(businessPulsarInfo.getRetentionTime());
            businessApproveInfo.setRetentionTimeUnit(businessPulsarInfo.getRetentionTimeUnit());
            businessApproveInfo.setTtl(businessPulsarInfo.getTtl());
            businessApproveInfo.setTtlUnit(businessPulsarInfo.getTtlUnit());
            businessApproveInfo.setRetentionSize(businessPulsarInfo.getRetentionSize());
            businessApproveInfo.setRetentionSizeUnit(businessPulsarInfo.getRetentionSizeUnit());
        }
        JsonArray streamJson = formData.getAsJsonArray("streamInfoList");
        List<DataStreamApproveInfo> dataStreamApproveInfoList = GsonUtil.fromJson(streamJson.toString(),
                new TypeToken<List<DataStreamApproveInfo>>() {
                }.getType());
        return new Pair(businessApproveInfo, dataStreamApproveInfoList);
    }
}
