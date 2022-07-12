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

package org.apache.inlong.manager.client.api.inner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.pagehelper.PageInfo;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.enums.SimpleGroupStatus;
import org.apache.inlong.manager.client.api.service.AuthInterceptor;
import org.apache.inlong.manager.client.api.service.InlongClusterApi;
import org.apache.inlong.manager.client.api.service.InlongGroupApi;
import org.apache.inlong.manager.client.api.service.InlongStreamApi;
import org.apache.inlong.manager.client.api.service.StreamSinkApi;
import org.apache.inlong.manager.client.api.service.StreamSourceApi;
import org.apache.inlong.manager.client.api.service.StreamTransformApi;
import org.apache.inlong.manager.client.api.service.WorkflowApi;
import org.apache.inlong.manager.common.auth.Authentication;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupResetRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.StreamSink;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.StreamSource;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamConfigLogListResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamPageRequest;
import org.apache.inlong.manager.common.pojo.transform.TransformRequest;
import org.apache.inlong.manager.common.pojo.transform.TransformResponse;
import org.apache.inlong.manager.common.pojo.workflow.EventLogView;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.pojo.workflow.form.process.NewGroupProcessForm;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.manager.client.api.impl.InlongGroupImpl.MQ_FIELD;
import static org.apache.inlong.manager.client.api.impl.InlongGroupImpl.MQ_FIELD_OLD;

/**
 * InnerInlongManagerClient is used to invoke http api of inlong manager.
 */
@Slf4j
public class InnerInlongManagerClient {

    private static final String REQUEST_FAILED_MSG = "Request to Inlong %s failed: %s";

    protected final String host;
    protected final int port;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final InlongClusterApi inlongClusterApi;
    private final InlongStreamApi inlongStreamApi;
    private final InlongGroupApi inlongGroupApi;
    private final StreamSourceApi streamSourceApi;
    private final StreamTransformApi streamTransformApi;
    private final StreamSinkApi streamSinkApi;
    private final WorkflowApi workflowApi;

    public InnerInlongManagerClient(ClientConfiguration configuration) {
        this.host = configuration.getBindHost();
        this.port = configuration.getBindPort();

        Authentication authentication = configuration.getAuthentication();
        Preconditions.checkNotNull(authentication, "inlong should be authenticated");
        Preconditions.checkTrue(authentication instanceof DefaultAuthentication,
                "inlong only support default authentication");
        DefaultAuthentication defaultAuthentication = (DefaultAuthentication) authentication;

        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .addInterceptor(
                        new AuthInterceptor(defaultAuthentication.getUsername(), defaultAuthentication.getPassword()))
                .connectTimeout(configuration.getConnectTimeout(), configuration.getTimeUnit())
                .readTimeout(configuration.getReadTimeout(), configuration.getTimeUnit())
                .writeTimeout(configuration.getWriteTimeout(), configuration.getTimeUnit())
                .retryOnConnectionFailure(true)
                .build();

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl("http://" + host + ":" + port + "/api/inlong/manager/")
                .addConverterFactory(JacksonConverterFactory.create(JsonUtils.OBJECT_MAPPER))
                .client(okHttpClient)
                .build();

        inlongStreamApi = retrofit.create(InlongStreamApi.class);
        inlongGroupApi = retrofit.create(InlongGroupApi.class);
        streamSinkApi = retrofit.create(StreamSinkApi.class);
        streamSourceApi = retrofit.create(StreamSourceApi.class);
        streamTransformApi = retrofit.create(StreamTransformApi.class);
        workflowApi = retrofit.create(WorkflowApi.class);
        inlongClusterApi = retrofit.create(InlongClusterApi.class);
    }

    /**
     * Save component cluster for Inlong
     *
     * @param request cluster create request
     * @return clusterIndex
     */
    public Integer saveCluster(ClusterRequest request) {
        Preconditions.checkNotEmpty(request.getName(), "cluster name should not be empty");
        Preconditions.checkNotEmpty(request.getType(), "cluster type should not be empty");
        Preconditions.checkNotEmpty(request.getClusterTags(), "cluster tags should not be empty");
        Response<Integer> clusterIndexResponse = executeHttpCall(inlongClusterApi.save(request));
        assertRespSuccess(clusterIndexResponse);
        return clusterIndexResponse.getData();
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
     * Check whether a group exists based on the group ID.
     */
    public Boolean isGroupExists(String inlongGroupId) {
        Preconditions.checkNotEmpty(inlongGroupId, "InlongGroupId should not be empty");

        Response<Boolean> response = executeHttpCall(inlongGroupApi.isGroupExists(inlongGroupId));
        assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Get info of group.
     */
    @SneakyThrows
    public InlongGroupInfo getGroupInfo(String inlongGroupId) {
        Preconditions.checkNotEmpty(inlongGroupId, "InlongGroupId should not be empty");

        Response<Object> responseBody = executeHttpCall(inlongGroupApi.getGroupInfo(inlongGroupId));
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

        Response<PageInfo<InlongGroupListResponse>> pageInfoResponse = executeHttpCall(
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
        Response<PageInfo<InlongGroupListResponse>> pageInfoResponse = executeHttpCall(
                inlongGroupApi.listGroups(pageRequest));
        assertRespSuccess(pageInfoResponse);
        return pageInfoResponse.getData();
    }

    /**
     * Create an inlong group
     */
    public String createGroup(InlongGroupRequest groupInfo) {
        Response<String> response = executeHttpCall(inlongGroupApi.createGroup(groupInfo));
        assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Update inlong group info
     *
     * @return groupId && errMsg
     */
    public Pair<String, String> updateGroup(InlongGroupRequest groupRequest) {
        Response<String> response = executeHttpCall(inlongGroupApi.updateGroup(groupRequest));
        return Pair.of(response.getData(), response.getErrMsg());
    }

    /**
     * Reset inlong group info
     */
    public boolean resetGroup(InlongGroupResetRequest resetRequest) {
        Response<Boolean> response = executeHttpCall(inlongGroupApi.resetGroup(resetRequest));
        assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Create an inlong stream.
     */
    public Integer createStreamInfo(InlongStreamInfo streamInfo) {
        Response<Integer> response = executeHttpCall(inlongStreamApi.createStream(streamInfo));
        assertRespSuccess(response);
        return response.getData();
    }

    public Boolean isStreamExists(InlongStreamInfo streamInfo) {
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        Preconditions.checkNotEmpty(groupId, "InlongGroupId should not be empty");
        Preconditions.checkNotEmpty(streamId, "InlongStreamId should not be empty");

        Response<Boolean> response = executeHttpCall(inlongStreamApi.isStreamExists(groupId, streamId));
        assertRespSuccess(response);
        return response.getData();
    }

    public Pair<Boolean, String> updateStreamInfo(InlongStreamInfo streamInfo) {
        Response<Boolean> resp = executeHttpCall(inlongStreamApi.updateStream(streamInfo));

        if (resp.getData() != null) {
            return Pair.of(resp.getData(), resp.getErrMsg());
        } else {
            return Pair.of(false, resp.getErrMsg());
        }
    }

    /**
     * Get inlong stream by the given groupId and streamId.
     */
    public InlongStreamInfo getStreamInfo(String groupId, String streamId) {
        Response<InlongStreamInfo> response = executeHttpCall(inlongStreamApi.getStream(groupId, streamId));

        if (response.isSuccess()) {
            return response.getData();
        }
        if (response.getErrMsg().contains("not exist")) {
            return null;
        } else {
            throw new RuntimeException(response.getErrMsg());
        }
    }

    /**
     * Get inlong stream info.
     */
    public List<InlongStreamInfo> listStreamInfo(String inlongGroupId) {
        InlongStreamPageRequest pageRequest = new InlongStreamPageRequest();
        pageRequest.setInlongGroupId(inlongGroupId);

        Response<PageInfo<InlongStreamInfo>> response = executeHttpCall(inlongStreamApi.listStream(pageRequest));
        assertRespSuccess(response);
        return response.getData().getList();
    }

    /**
     * Create an inlong stream source.
     */
    public Integer createSource(SourceRequest request) {
        Response<Integer> response = executeHttpCall(streamSourceApi.createSource(request));
        assertRespSuccess(response);
        return response.getData();
    }

    /**
     * List stream sources by the given groupId and streamId.
     */
    public List<StreamSource> listSources(String groupId, String streamId) {
        return listSources(groupId, streamId, null);
    }

    /**
     * List stream sources by the specified source type.
     */
    public List<StreamSource> listSources(String groupId, String streamId, String sourceType) {
        Response<PageInfo<StreamSource>> response = executeHttpCall(
                streamSourceApi.listSources(groupId, streamId, sourceType));
        assertRespSuccess(response);
        return response.getData().getList();
    }

    /**
     * Update the stream source info.
     */
    public Pair<Boolean, String> updateSource(SourceRequest request) {
        Response<Boolean> response = executeHttpCall(streamSourceApi.updateSource(request));
        if (response.getData() != null) {
            return Pair.of(response.getData(), response.getErrMsg());
        } else {
            return Pair.of(false, response.getErrMsg());
        }
    }

    /**
     * Delete the stream source info by id.
     */
    public boolean deleteSource(int id) {
        Preconditions.checkTrue(id > 0, "sourceId is illegal");
        Response<Boolean> response = executeHttpCall(streamSourceApi.deleteSource(id));
        assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Create a conversion function info.
     */
    public Integer createTransform(TransformRequest transformRequest) {
        Response<Integer> response = executeHttpCall(streamTransformApi.createTransform(transformRequest));
        assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Get all conversion function info.
     */
    public List<TransformResponse> listTransform(String groupId, String streamId) {
        Response<List<TransformResponse>> response = executeHttpCall(
                streamTransformApi.listTransform(groupId, streamId));
        assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Update conversion function info.
     */
    public Pair<Boolean, String> updateTransform(TransformRequest transformRequest) {
        Response<Boolean> response = executeHttpCall(streamTransformApi.updateTransform(transformRequest));

        if (response.getData() != null) {
            return Pair.of(response.getData(), response.getErrMsg());
        } else {
            return Pair.of(false, response.getErrMsg());
        }
    }

    /**
     * Delete conversion function info.
     */
    public boolean deleteTransform(TransformRequest transformRequest) {
        Preconditions.checkNotEmpty(transformRequest.getInlongGroupId(), "inlongGroupId should not be null");
        Preconditions.checkNotEmpty(transformRequest.getInlongStreamId(), "inlongStreamId should not be null");
        Preconditions.checkNotEmpty(transformRequest.getTransformName(), "transformName should not be null");

        Response<Boolean> response = executeHttpCall(
                streamTransformApi.deleteTransform(transformRequest.getInlongGroupId(),
                        transformRequest.getInlongStreamId(), transformRequest.getTransformName()));
        assertRespSuccess(response);
        return response.getData();
    }

    public Integer createSink(SinkRequest sinkRequest) {
        Response<Integer> response = executeHttpCall(streamSinkApi.createSink(sinkRequest));
        assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Delete stream sink info by ID.
     */
    public boolean deleteSink(int id) {
        Preconditions.checkTrue(id > 0, "sinkId is illegal");
        Response<Boolean> response = executeHttpCall(streamSinkApi.deleteSink(id));
        assertRespSuccess(response);
        return response.getData();
    }

    /**
     * List stream sinks by the given groupId and streamId.
     */
    public List<StreamSink> listSinks(String groupId, String streamId) {
        return listSinks(groupId, streamId, null);
    }

    /**
     * List stream sinks by the specified sink type.
     */
    public List<StreamSink> listSinks(String groupId, String streamId, String sinkType) {
        Response<PageInfo<StreamSink>> response = executeHttpCall(
                streamSinkApi.listSinks(groupId, streamId, sinkType));
        assertRespSuccess(response);
        return response.getData().getList();
    }

    /**
     * Update the stream sink info.
     */
    public Pair<Boolean, String> updateSink(SinkRequest sinkRequest) {
        Response<Boolean> responseBody = executeHttpCall(streamSinkApi.updateSink(sinkRequest));
        assertRespSuccess(responseBody);

        if (responseBody.getData() != null) {
            return Pair.of(responseBody.getData(), responseBody.getErrMsg());
        } else {
            return Pair.of(false, responseBody.getErrMsg());
        }
    }

    public WorkflowResult initInlongGroup(InlongGroupRequest groupInfo) {
        Response<WorkflowResult> responseBody = executeHttpCall(
                inlongGroupApi.initInlongGroup(groupInfo.getInlongGroupId()));
        assertRespSuccess(responseBody);
        return responseBody.getData();
    }

    public WorkflowResult startInlongGroup(int taskId, NewGroupProcessForm newGroupProcessForm) {
        ObjectNode workflowTaskOperation = objectMapper.createObjectNode();
        workflowTaskOperation.putPOJO("transferTo", Lists.newArrayList());
        workflowTaskOperation.put("remark", "approved by system");

        ObjectNode inlongGroupApproveForm = objectMapper.createObjectNode();
        inlongGroupApproveForm.putPOJO("groupApproveInfo", newGroupProcessForm.getGroupInfo());
        inlongGroupApproveForm.putPOJO("streamApproveInfoList", newGroupProcessForm.getStreamInfoList());
        inlongGroupApproveForm.put("formName", "InlongGroupApproveForm");
        workflowTaskOperation.set("form", inlongGroupApproveForm);

        log.info("startInlongGroup workflowTaskOperation: {}", inlongGroupApproveForm);

        Map<String, Object> requestMap = JsonUtils.OBJECT_MAPPER.convertValue(workflowTaskOperation,
                new TypeReference<Map<String, Object>>() {
                });
        Response<WorkflowResult> response = executeHttpCall(workflowApi.startInlongGroup(taskId, requestMap));
        assertRespSuccess(response);

        return response.getData();
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

        Response<String> responseBody = executeHttpCall(responseCall);

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
            Response<String> response = executeHttpCall(inlongGroupApi.deleteGroupAsync(groupId));
            assertRespSuccess(response);
            return groupId.equals(response.getData());
        } else {
            Response<Boolean> response = executeHttpCall(inlongGroupApi.deleteGroup(groupId));
            assertRespSuccess(response);
            return response.getData();
        }
    }

    /**
     * get inlong group error messages
     */
    public List<EventLogView> getInlongGroupError(String inlongGroupId) {
        Response<PageInfo<EventLogView>> response = executeHttpCall(workflowApi.getInlongGroupError(inlongGroupId, -1));
        assertRespSuccess(response);
        return response.getData().getList();
    }

    /**
     * get inlong group error messages
     */
    public List<InlongStreamConfigLogListResponse> getStreamLogs(String inlongGroupId, String inlongStreamId) {
        Response<PageInfo<InlongStreamConfigLogListResponse>> response = executeHttpCall(
                inlongStreamApi.getStreamLogs(inlongGroupId, inlongStreamId));
        assertRespSuccess(response);
        return response.getData().getList();
    }

    private <T> T executeHttpCall(Call<T> call) {
        Request request = call.request();
        String url = request.url().encodedPath();
        try {
            retrofit2.Response<T> response = call.execute();
            Preconditions.checkTrue(response.isSuccessful(),
                    String.format(REQUEST_FAILED_MSG, url, response.message()));
            return response.body();
        } catch (IOException e) {
            log.error(String.format(REQUEST_FAILED_MSG, url, e.getMessage()), e);
            throw new RuntimeException(String.format(REQUEST_FAILED_MSG, url, e.getMessage()), e);
        }
    }

    private void assertRespSuccess(Response<?> response) {
        Preconditions.checkTrue(response.isSuccess(), String.format(REQUEST_FAILED_MSG, response.getErrMsg(), null));
    }

}
