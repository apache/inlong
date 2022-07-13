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

package org.apache.inlong.manager.client.api.inner.client;

import com.github.pagehelper.PageInfo;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.enums.SimpleGroupStatus;
import org.apache.inlong.manager.client.api.service.InlongGroupApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupResetRequest;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import retrofit2.Call;

import static org.apache.inlong.manager.client.api.impl.InlongGroupImpl.MQ_FIELD;
import static org.apache.inlong.manager.client.api.impl.InlongGroupImpl.MQ_FIELD_OLD;

/**
 * Client for {@link InlongGroupApi}.
 */
public class InlongGroupClient {

    private final InlongGroupApi inlongGroupApi;

    public InlongGroupClient(ClientConfiguration configuration) {
        inlongGroupApi = ClientUtils.createRetrofit(configuration).create(InlongGroupApi.class);
    }

    /**
     * Check whether a group exists based on the group ID.
     */
    public Boolean isGroupExists(String inlongGroupId) {
        Preconditions.checkNotEmpty(inlongGroupId, "InlongGroupId should not be empty");

        Response<Boolean> response = ClientUtils.executeHttpCall(inlongGroupApi.isGroupExists(inlongGroupId));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Get inlong group by the given inlong group id.
     *
     * @param inlongGroupId the given inlong group id
     * @return inlong group info if exists, null will be returned if not exits
     */
    public InlongGroupInfo getGroupIfExists(String inlongGroupId) {
        if (this.isGroupExists(inlongGroupId)) {
            return getGroupInfo(inlongGroupId);
        }
        return null;
    }

    /**
     * Get info of group.
     */
    @SneakyThrows
    public InlongGroupInfo getGroupInfo(String inlongGroupId) {
        Preconditions.checkNotEmpty(inlongGroupId, "InlongGroupId should not be empty");

        Response<Object> responseBody = ClientUtils.executeHttpCall(inlongGroupApi.getGroupInfo(inlongGroupId));
        if (responseBody.isSuccess()) {
            JSONObject groupInfoJson = JsonUtils.parseObject(
                    JsonUtils.toJsonString(JsonUtils.toJsonString(responseBody.getData())),
                    JSONObject.class);
            if (groupInfoJson.has(MQ_FIELD_OLD) && !groupInfoJson.has(MQ_FIELD)) {
                groupInfoJson.put(MQ_FIELD, groupInfoJson.get(MQ_FIELD_OLD));
            }
            return JsonUtils.parseObject(groupInfoJson.toString(), InlongGroupInfo.class);
        }

        if (responseBody.getErrMsg().contains("not exist")) {
            return null;
        } else {
            throw new RuntimeException(responseBody.getErrMsg());
        }
    }

    /**
     * Get inlong group list.
     */
    public PageInfo<InlongGroupListResponse> listGroups(String keyword, int status, int pageNum, int pageSize) {
        InlongGroupPageRequest request = InlongGroupPageRequest.builder()
                .keyword(keyword)
                .status(status)
                .build();
        request.setPageNum(pageNum <= 0 ? 1 : pageNum);
        request.setPageSize(pageSize);

        Response<PageInfo<InlongGroupListResponse>> pageInfoResponse = ClientUtils.executeHttpCall(
                inlongGroupApi.listGroups(request));

        if (pageInfoResponse.isSuccess()) {
            return pageInfoResponse.getData();
        }
        if (pageInfoResponse.getErrMsg().contains("not exist")) {
            return null;
        } else {
            throw new RuntimeException(pageInfoResponse.getErrMsg());
        }
    }

    /**
     * List inlong group by the page request
     *
     * @param pageRequest The pageRequest
     * @return Response encapsulate of inlong group list
     */
    public PageInfo<InlongGroupListResponse> listGroups(InlongGroupPageRequest pageRequest) {
        Response<PageInfo<InlongGroupListResponse>> pageInfoResponse = ClientUtils.executeHttpCall(
                inlongGroupApi.listGroups(pageRequest));
        ClientUtils.assertRespSuccess(pageInfoResponse);
        return pageInfoResponse.getData();
    }

    /**
     * Create an inlong group
     */
    public String createGroup(InlongGroupRequest groupInfo) {
        Response<String> response = ClientUtils.executeHttpCall(inlongGroupApi.createGroup(groupInfo));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Update inlong group info
     *
     * @return groupId && errMsg
     */
    public Pair<String, String> updateGroup(InlongGroupRequest groupRequest) {
        Response<String> response = ClientUtils.executeHttpCall(inlongGroupApi.updateGroup(groupRequest));
        return Pair.of(response.getData(), response.getErrMsg());
    }

    /**
     * Reset inlong group info
     */
    public boolean resetGroup(InlongGroupResetRequest resetRequest) {
        Response<Boolean> response = ClientUtils.executeHttpCall(inlongGroupApi.resetGroup(resetRequest));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    public WorkflowResult initInlongGroup(InlongGroupRequest groupInfo) {
        Response<WorkflowResult> responseBody = ClientUtils.executeHttpCall(
                inlongGroupApi.initInlongGroup(groupInfo.getInlongGroupId()));
        ClientUtils.assertRespSuccess(responseBody);
        return responseBody.getData();
    }

    public boolean operateInlongGroup(String groupId, SimpleGroupStatus status) {
        return operateInlongGroup(groupId, status, false);
    }

    public boolean operateInlongGroup(String groupId, SimpleGroupStatus status, boolean async) {
        Call<Response<String>> responseCall;
        if (status == SimpleGroupStatus.STOPPED) {
            if (async) {
                responseCall = inlongGroupApi.suspendProcessAsync(groupId);
            } else {
                responseCall = inlongGroupApi.suspendProcess(groupId);
            }
        } else if (status == SimpleGroupStatus.STARTED) {
            if (async) {
                responseCall = inlongGroupApi.restartProcessAsync(groupId);
            } else {
                responseCall = inlongGroupApi.restartProcess(groupId);
            }
        } else {
            throw new IllegalArgumentException(String.format("Unsupported inlong group status: %s", status));
        }

        Response<String> responseBody = ClientUtils.executeHttpCall(responseCall);

        String errMsg = responseBody.getErrMsg();
        return responseBody.isSuccess()
                || errMsg == null
                || !errMsg.contains("not allowed");
    }

    public boolean deleteInlongGroup(String groupId) {
        return deleteInlongGroup(groupId, false);
    }

    public boolean deleteInlongGroup(String groupId, boolean async) {
        if (async) {
            Response<String> response = ClientUtils.executeHttpCall(inlongGroupApi.deleteGroupAsync(groupId));
            ClientUtils.assertRespSuccess(response);
            return groupId.equals(response.getData());
        } else {
            Response<Boolean> response = ClientUtils.executeHttpCall(inlongGroupApi.deleteGroup(groupId));
            ClientUtils.assertRespSuccess(response);
            return response.getData();
        }
    }
}
