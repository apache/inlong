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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.RequestBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.enums.SimpleGroupStatus;
import org.apache.inlong.manager.client.api.util.InlongParser;
import org.apache.inlong.manager.common.auth.Authentication;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamConfigLogListResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamPageRequest;
import org.apache.inlong.manager.common.pojo.transform.TransformRequest;
import org.apache.inlong.manager.common.pojo.transform.TransformResponse;
import org.apache.inlong.manager.common.pojo.workflow.EventLogView;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.util.AssertUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import java.util.List;

/**
 * InnerInlongManagerClient is used to invoke http api of inlong manager.
 */
@Slf4j
public class InnerInlongManagerClient {

    protected static final String HTTP_PATH = "api/inlong/manager";
    private static final MediaType APPLICATION_JSON = MediaType.parse("application/json; charset=utf-8");

    protected final OkHttpClient httpClient;
    protected final String host;
    protected final int port;
    protected final String uname;
    protected final String passwd;

    public InnerInlongManagerClient(ClientConfiguration configuration) {
        this.host = configuration.getBindHost();
        this.port = configuration.getBindPort();
        Authentication authentication = configuration.getAuthentication();
        AssertUtils.notNull(authentication, "Inlong should be authenticated");
        AssertUtils.isTrue(authentication instanceof DefaultAuthentication,
                "Inlong only support default authentication");
        DefaultAuthentication defaultAuthentication = (DefaultAuthentication) authentication;
        this.uname = defaultAuthentication.getUserName();
        this.passwd = defaultAuthentication.getPassword();
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(configuration.getConnectTimeout(), configuration.getTimeUnit())
                .readTimeout(configuration.getReadTimeout(), configuration.getTimeUnit())
                .writeTimeout(configuration.getWriteTimeout(), configuration.getTimeUnit())
                .retryOnConnectionFailure(true)
                .build();
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
        } else {
            return null;
        }
    }

    /**
     * Check whether a group exists based on the group ID.
     */
    public Boolean isGroupExists(String inlongGroupId) {
        AssertUtils.notEmpty(inlongGroupId, "InlongGroupId should not be empty");

        String path = HTTP_PATH + "/group/exist/" + inlongGroupId;
        return this.getForDataEntity(formatUrl(path), new TypeReference<Response<Boolean>>() {
        });
    }

    /**
     * Get information of group.
     */
    public InlongGroupInfo getGroupInfo(String inlongGroupId) {
        AssertUtils.notEmpty(inlongGroupId, "InlongGroupId should not be empty");

        String path = HTTP_PATH + "/group/get/" + inlongGroupId;
        final String url = formatUrl(path);

        Response<InlongGroupInfo> responseBody = this.getForResEntity(url,
                new TypeReference<Response<InlongGroupInfo>>() {
                });
        if (responseBody.isSuccess()) {
            return responseBody.getData();
        } else {
            if (responseBody.getErrMsg().contains("Inlong group does not exist")) {
                return null;
            } else {
                throw new RuntimeException(responseBody.getErrMsg());
            }
        }
    }

    /**
     * Get information of groups.
     */
    public PageInfo<InlongGroupListResponse> listGroups(String keyword, int status, int pageNum, int pageSize) {
        if (pageNum <= 0) {
            pageNum = 1;
        }

        ObjectNode groupQuery = new ObjectMapper().createObjectNode();
        groupQuery.put("keyword", keyword);
        groupQuery.put("status", status);
        groupQuery.put("pageNum", pageNum);
        groupQuery.put("pageSize", pageSize);

        String path = HTTP_PATH + "/group/list";
        final String url = formatUrl(path);

        Response<PageInfo<InlongGroupListResponse>> pageInfoResponse = this.postJsonForResEntity(
                url,
                groupQuery.toString(),
                new TypeReference<Response<PageInfo<InlongGroupListResponse>>>() {
                });

        if (!pageInfoResponse.isSuccess()) {
            if (pageInfoResponse.getErrMsg().contains("Inlong group does not exist")) {
                return null;
            } else {
                throw new RuntimeException(pageInfoResponse.getErrMsg());
            }
        }

        return pageInfoResponse.getData();
    }

    /**
     * List inlong group by the page request
     *
     * @param pageRequest The pageRequest
     * @return Response encapsulate of inlong group list
     */
    public PageInfo<InlongGroupListResponse> listGroups(InlongGroupPageRequest pageRequest) {
        return this.postJsonForDataEntity(
                formatUrl(HTTP_PATH + "/group/list"),
                JsonUtils.toJsonString(pageRequest),
                new TypeReference<Response<PageInfo<InlongGroupListResponse>>>() {
                }
        );
    }

    /**
     * Create inlong group
     */
    public String createGroup(InlongGroupRequest groupInfo) {
        return this.postJsonForDataEntity(
                formatUrl(HTTP_PATH + "/group/save"),
                JsonUtils.toJsonString(groupInfo),
                String.class
        );
    }

    /**
     * Update inlong group info
     *
     * @return groupId && errMsg
     */
    public Pair<String, String> updateGroup(InlongGroupRequest groupRequest) {
        Response<String> updateGroupResp = this.postJsonForResEntity(
                formatUrl(HTTP_PATH + "/group/update"),
                JsonUtils.toJsonString(groupRequest),
                String.class
        );

        return Pair.of(updateGroupResp.getData(), updateGroupResp.getErrMsg());
    }

    /**
     * Create information of stream.
     */
    public Integer createStreamInfo(InlongStreamInfo streamInfo) {
        return this.postJsonForDataEntity(
                formatUrl(HTTP_PATH + "/stream/save"),
                JsonUtils.toJsonString(streamInfo),
                Integer.class
        );
    }

    public Boolean isStreamExists(InlongStreamInfo streamInfo) {
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        AssertUtils.notEmpty(groupId, "InlongGroupId should not be empty");
        AssertUtils.notEmpty(streamId, "InlongStreamId should not be empty");

        final String url = formatUrl(HTTP_PATH + "/stream/exist/" + groupId + "/" + streamId);
        return this.getForDataEntity(url, new TypeReference<Response<Boolean>>() {
        });
    }

    public Pair<Boolean, String> updateStreamInfo(InlongStreamInfo streamInfo) {
        streamInfo.setCreateTime(null);
        streamInfo.setModifyTime(null);

        final String url = formatUrl(HTTP_PATH + "/stream/update");
        Response<Boolean> resp = this.postJsonForResEntity(url, JsonUtils.toJsonString(streamInfo), Boolean.class);

        if (resp.getData() != null) {
            return Pair.of(resp.getData(), resp.getErrMsg());
        } else {
            return Pair.of(false, resp.getErrMsg());
        }
    }

    /**
     * Get inlong stream by the given groupId and streamId.
     */
    public InlongStreamInfo getStreamInfo(String inlongGroupId, String inlongStreamId) {
        String url = formatUrl(HTTP_PATH + "/stream/get");
        url += String.format("&groupId=%s&streamId=%s", inlongGroupId, inlongStreamId);

        Response<InlongStreamInfo> streamInfoResponse = this.getForResEntity(url,
                new TypeReference<Response<InlongStreamInfo>>() {
                });

        if (streamInfoResponse.isSuccess()) {
            return streamInfoResponse.getData();
        }

        if (streamInfoResponse.getErrMsg().contains("Inlong stream does not exist")) {
            return null;
        } else {
            throw new RuntimeException(streamInfoResponse.getErrMsg());
        }
    }

    /**
     * Get information of stream.
     */
    public List<FullStreamResponse> listStreamInfo(String inlongGroupId) {
        InlongStreamPageRequest pageRequest = new InlongStreamPageRequest();
        pageRequest.setInlongGroupId(inlongGroupId);

        return this.postJsonForDataEntity(
                        formatUrl(HTTP_PATH + "/stream/listAll"),
                        JsonUtils.toJsonString(pageRequest),
                        new TypeReference<Response<PageInfo<FullStreamResponse>>>() {
                        }
                )
                .getList();
    }

    /**
     * Create a data source.
     */
    public Integer createSource(SourceRequest sourceRequest) {
        return this.postJsonForDataEntity(
                formatUrl(HTTP_PATH + "/source/save"),
                JsonUtils.toJsonString(sourceRequest),
                Integer.class
        );
    }

    /**
     * Get information of sources.
     */
    public List<SourceListResponse> listSources(String groupId, String streamId) {
        return listSources(groupId, streamId, null);
    }

    /**
     * Get information of sources.
     */
    public List<SourceListResponse> listSources(String groupId, String streamId, String sourceType) {
        String url = formatUrl(HTTP_PATH + "/source/list");
        url = String.format("%s&inlongGroupId=%s&inlongStreamId=%s", url, groupId, streamId);
        if (StringUtils.isNotEmpty(sourceType)) {
            url = String.format("%s&sourceType=%s", url, sourceType);
        }

        return this.getForDataEntity(
                        url,
                        new TypeReference<Response<PageInfo<SourceListResponse>>>() {
                        }
                )
                .getList();
    }

    /**
     * Update data Source Information.
     */
    public Pair<Boolean, String> updateSource(SourceRequest sourceRequest) {
        Response<Boolean> resEntity = postJsonForResEntity(
                formatUrl(HTTP_PATH + "/source/update"),
                JsonUtils.toJsonString(sourceRequest),
                Boolean.class
        );

        if (resEntity.getData() != null) {
            return Pair.of(resEntity.getData(), resEntity.getErrMsg());
        } else {
            return Pair.of(false, resEntity.getErrMsg());
        }
    }

    /**
     * Delete data source information by id.
     */
    public boolean deleteSource(int id) {
        AssertUtils.isTrue(id > 0, "sourceId is illegal");
        return this.deleteJsonForDataEntity(
                formatUrl(HTTP_PATH + "/source/delete/" + id),
                null,
                Boolean.class
        );
    }

    /**
     * Create a conversion function information.
     */
    public Integer createTransform(TransformRequest transformRequest) {
        return this.postJsonForDataEntity(
                formatUrl(HTTP_PATH + "/transform/save"),
                JsonUtils.toJsonString(transformRequest),
                Integer.class
        );
    }

    /**
     * Get all conversion function information.
     */
    public List<TransformResponse> listTransform(String groupId, String streamId) {
        String url = formatUrl(HTTP_PATH + "/transform/list");
        url = String.format("%s&inlongGroupId=%s&inlongStreamId=%s", url, groupId, streamId);
        return this.getForDataEntity(
                url,
                new TypeReference<Response<List<TransformResponse>>>() {
                }
        );
    }

    /**
     * Update conversion function information.
     */
    public Pair<Boolean, String> updateTransform(TransformRequest transformRequest) {
        Response<Boolean> responseBody = this.postJsonForResEntity(
                formatUrl(HTTP_PATH + "/transform/update"),
                JsonUtils.toJsonString(transformRequest),
                Boolean.class
        );

        if (responseBody.getData() != null) {
            return Pair.of(responseBody.getData(), responseBody.getErrMsg());
        } else {
            return Pair.of(false, responseBody.getErrMsg());
        }
    }

    /**
     * Delete conversion function information.
     */
    public boolean deleteTransform(TransformRequest transformRequest) {
        AssertUtils.notEmpty(transformRequest.getInlongGroupId(), "inlongGroupId should not be null");
        AssertUtils.notEmpty(transformRequest.getInlongStreamId(), "inlongStreamId should not be null");
        AssertUtils.notEmpty(transformRequest.getTransformName(), "transformName should not be null");

        String url = formatUrl(HTTP_PATH + "/transform/delete");
        url = String.format("%s&inlongGroupId=%s&inlongStreamId=%s&transformName=%s", url,
                transformRequest.getInlongGroupId(),
                transformRequest.getInlongStreamId(),
                transformRequest.getTransformName());

        return this.deleteJsonForDataEntity(url, null, Boolean.class);
    }

    public Integer createSink(SinkRequest sinkRequest) {
        return this.postJsonForDataEntity(
                formatUrl(HTTP_PATH + "/sink/save"),
                JsonUtils.toJsonString(sinkRequest),
                Integer.class
        );
    }

    /**
     * Delete information of data sink by ID.
     */
    public boolean deleteSink(int id) {
        AssertUtils.isTrue(id > 0, "sinkId is illegal");

        return this.deleteJsonForDataEntity(
                formatUrl(HTTP_PATH + "/sink/delete/" + id),
                null,
                Boolean.class
        );
    }

    /**
     * Get information of data sinks.
     */
    public List<SinkListResponse> listSinks(String groupId, String streamId) {
        return listSinks(groupId, streamId, null);
    }

    /**
     * Get information of data sinks.
     */
    public List<SinkListResponse> listSinks(String groupId, String streamId, String sinkType) {
        String url = formatUrl(HTTP_PATH + "/sink/list");
        url = String.format("%s&inlongGroupId=%s&inlongStreamId=%s", url, groupId, streamId);
        if (StringUtils.isNotEmpty(sinkType)) {
            url = String.format("%s&sinkType=%s", url, sinkType);
        }

        PageInfo<SinkListResponse> sinkListResponsePageInfo = this.getForDataEntity(
                url,
                new TypeReference<Response<PageInfo<SinkListResponse>>>() {
                }
        );

        return sinkListResponsePageInfo.getList();
    }

    /**
     * Update information of data sink.
     */
    public Pair<Boolean, String> updateSink(SinkRequest sinkRequest) {
        Response<Boolean> responseBody = this.postJsonForResEntity(
                formatUrl(HTTP_PATH + "/sink/update"),
                JsonUtils.toJsonString(sinkRequest),
                Boolean.class
        );

        if (responseBody.getData() != null) {
            return Pair.of(responseBody.getData(), responseBody.getErrMsg());
        } else {
            return Pair.of(false, responseBody.getErrMsg());
        }
    }

    public WorkflowResult initInlongGroup(InlongGroupRequest groupInfo) {
        return this.postJsonForDataEntity(
                formatUrl(HTTP_PATH + "/group/startProcess/" + groupInfo.getInlongGroupId()),
                null,
                WorkflowResult.class
        );
    }

    public WorkflowResult startInlongGroup(int taskId,
            Pair<InlongGroupApproveRequest, List<InlongStreamApproveRequest>> initMsg) {

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode workflowTaskOperation = objectMapper.createObjectNode();
        workflowTaskOperation.putPOJO("transferTo", Lists.newArrayList());
        workflowTaskOperation.put("remark", "approved by system");

        ObjectNode inlongGroupApproveForm = objectMapper.createObjectNode();
        inlongGroupApproveForm.putPOJO("groupApproveInfo", initMsg.getKey());
        inlongGroupApproveForm.putPOJO("streamApproveInfoList", initMsg.getValue());
        inlongGroupApproveForm.put("formName", "InlongGroupApproveForm");
        workflowTaskOperation.set("form", inlongGroupApproveForm);

        String operationData = workflowTaskOperation.toString();
        log.info("startInlongGroup workflowTaskOperation:{}", operationData);

        return this.postJsonForDataEntity(
                formatUrl(HTTP_PATH + "/workflow/approve/" + taskId),
                operationData,
                WorkflowResult.class
        );
    }

    public boolean operateInlongGroup(String groupId, SimpleGroupStatus status) {
        return operateInlongGroup(groupId, status, false);
    }

    public boolean operateInlongGroup(String groupId, SimpleGroupStatus status, boolean async) {
        String path = HTTP_PATH;
        if (status == SimpleGroupStatus.STOPPED) {
            if (async) {
                path += "/group/suspendProcessAsync/";
            } else {
                path += "/group/suspendProcess/";
            }
        } else if (status == SimpleGroupStatus.STARTED) {
            if (async) {
                path += "/group/restartProcessAsync/";
            } else {
                path += "/group/restartProcess/";
            }
        } else {
            throw new IllegalArgumentException(String.format("Unsupported state: %s", status));
        }

        path += groupId;
        Response<String> responseBody = this.postJsonForResEntity(
                formatUrl(path),
                null,
                String.class
        );

        String errMsg = responseBody.getErrMsg();
        return responseBody.isSuccess()
                || errMsg == null
                || !errMsg.contains("current status was not allowed");
    }

    public boolean deleteInlongGroup(String groupId) {
        return deleteInlongGroup(groupId, false);
    }

    public boolean deleteInlongGroup(String groupId, boolean async) {
        String path = HTTP_PATH;
        if (async) {
            path += "/group/deleteAsync/" + groupId;
        } else {
            path += "/group/delete/" + groupId;
        }

        return this.deleteJsonForDataEntity(
                formatUrl(path),
                null,
                Boolean.class
        );
    }

    /**
     * get inlong group error messages
     */
    public List<EventLogView> getInlongGroupError(String inlongGroupId) {
        String url = formatUrl(HTTP_PATH + "/workflow/event/list");
        url = url + "&inlongGroupId=" + inlongGroupId + "&status=-1";

        return this.getForDataEntity(url, new TypeReference<Response<PageInfo<EventLogView>>>() {
                })
                .getList();
    }

    /**
     * get inlong group error messages
     */
    public List<InlongStreamConfigLogListResponse> getStreamLogs(String inlongGroupId, String inlongStreamId) {
        String url = formatUrl(HTTP_PATH + "/stream/config/log/list");
        url = url + "&inlongGroupId=" + inlongGroupId + "&inlongStreamId=" + inlongStreamId;

        return this.getForDataEntity(
                        url,
                        new TypeReference<Response<PageInfo<InlongStreamConfigLogListResponse>>>() {
                        }
                )
                .getList();
    }

    protected String formatUrl(String path) {
        return String.format("http://%s:%s/%s?username=%s&password=%s", host, port, path, uname, passwd);
    }

    public <T> Response<T> deleteJsonForResEntity(String url, String content, Class<T> clazz) {
        return this.requestJsonForResEntity("DELETE", url, content, clazz);
    }

    public <T> Response<T> postJsonForResEntity(String url, String content, Class<T> clazz) {
        return this.requestJsonForResEntity("POST", url, content, clazz);
    }

    public <T> Response<T> postJsonForResEntity(String url, String content, TypeReference<Response<T>> typeReference) {
        return this.requestJsonForResEntity("POST", url, content, typeReference);
    }

    public <T> T postJsonForDataEntity(String url, String content, Class<T> clazz) {
        return this.requestJsonForDataEntity("POST", url, content, clazz);
    }

    public <T> T postJsonForDataEntity(String url, String content, TypeReference<Response<T>> typeReference) {
        return this.requestJsonForDataEntity("POST", url, content, typeReference);
    }

    public <T> T requestJsonForDataEntity(String method, String url, String content, Class<T> clazz) {
        Response<T> responseBody = this.requestJsonForResEntity(method, url, content, clazz);
        Preconditions.checkState(responseBody.isSuccess(), "Inlong request failed: %s", responseBody.getErrMsg());

        return responseBody.getData();
    }

    public <T> T requestJsonForDataEntity(String method, String url, String content,
            TypeReference<Response<T>> typeReference) {
        Response<T> responseBody = this.requestJsonForResEntity(method, url, content, typeReference);
        Preconditions.checkState(responseBody.isSuccess(), "Inlong request failed: %s", responseBody.getErrMsg());

        return responseBody.getData();
    }

    public <T> T deleteJsonForDataEntity(String url, String content, Class<T> clazz) {
        return this.requestJsonForDataEntity("DELETE", url, content, clazz);
    }

    public <T> Response<T> getForResEntity(String url, TypeReference<Response<T>> typeReference) {
        return this.requestJsonForResEntity("GET", url, null, typeReference);
    }

    public <T> T getForDataEntity(String url, TypeReference<Response<T>> typeReference) {
        return this.requestJsonForDataEntity("GET", url, null, typeReference);
    }

    public <T> Response<T> requestJsonForResEntity(String method, String url, String content,
            TypeReference<Response<T>> typeReference) {
        Builder reqBuilder = new Builder()
                .url(url);
        if (StringUtils.isBlank(content)) {
            reqBuilder.method(method, null);
        } else {
            reqBuilder.method(method, RequestBody.create(APPLICATION_JSON, content));
        }

        return this.execHttpRequest(reqBuilder.build(), typeReference);
    }

    public <T> Response<T> requestJsonForResEntity(String method, String url, String content, Class<T> clazz) {
        Builder reqBuilder = new Builder()
                .url(url);
        if (StringUtils.isBlank(content)) {
            reqBuilder.method(method, null);
        } else {
            reqBuilder.method(method, RequestBody.create(APPLICATION_JSON, content));
        }

        return this.execHttpRequest(reqBuilder.build(), clazz);
    }

    public <T> Response<T> execHttpRequest(Request request, Class<T> clazz) {
        String body = execHttpRequestGetBodyStr(request);
        return InlongParser.parseResponse(body, clazz);
    }

    public <T> Response<T> execHttpRequest(Request request, TypeReference<Response<T>> typeReference) {
        String body = execHttpRequestGetBodyStr(request);
        return JsonUtils.parseObject(body, typeReference);
    }

    private String execHttpRequestGetBodyStr(Request request) {
        String urlPath = request.url().encodedPath();

        try (okhttp3.Response response = httpClient.newCall(request).execute()) {
            String body = response.body().string();
            Preconditions.checkState(response.isSuccessful(), "Inlong request=%s failed: %s", urlPath, body);

            return body;
        } catch (Exception e) {
            log.error("Inlong request failed: {}, url:{}", e.getMessage(), urlPath, e);
            throw new RuntimeException(
                    String.format("Inlong request failed: %s,url: %s", e.getMessage(), urlPath), e);
        }
    }

}
