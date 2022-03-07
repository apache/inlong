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

import com.alibaba.fastjson.JSONObject;
import com.github.pagehelper.PageInfo;
import com.google.common.collect.Lists;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.InlongGroupContext.InlongGroupState;
import org.apache.inlong.manager.client.api.auth.Authentication;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.api.util.AssertUtil;
import org.apache.inlong.manager.client.api.util.GsonUtil;
import org.apache.inlong.manager.client.api.util.InlongParser;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamConfigLogListResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.EventLogView;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;

/**
 * InnerInlongManagerClient is used to invoke http api of inlong manager.
 */
@Slf4j
public class InnerInlongManagerClient {

    private static final String HTTP_PATH = "api/inlong/manager";

    private OkHttpClient httpClient;

    private String host;

    private int port;

    private String uname;

    private String passwd;

    public InnerInlongManagerClient(InlongClientImpl inlongClient) {
        ClientConfiguration configuration = inlongClient.getConfiguration();
        this.host = configuration.getBindHost();
        this.port = configuration.getBindPort();
        Authentication authentication = configuration.getAuthentication();
        AssertUtil.notNull(authentication, "Inlong should be authenticated");
        AssertUtil.isTrue(authentication instanceof DefaultAuthentication,
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

    public Pair<Boolean, InlongGroupResponse> isGroupExists(InlongGroupRequest groupInfo) {
        String inlongGroupId = groupInfo.getInlongGroupId();
        if (StringUtils.isEmpty(inlongGroupId)) {
            inlongGroupId = "b_" + groupInfo.getName();
        }
        InlongGroupResponse currentBizInfo = getGroupInfo(inlongGroupId);
        if (currentBizInfo != null) {
            return Pair.of(true, currentBizInfo);
        } else {
            return Pair.of(false, null);
        }
    }

    public InlongGroupResponse getGroupInfo(String inlongGroupId) {
        if (StringUtils.isEmpty(inlongGroupId)) {
            throw new IllegalArgumentException("InlongGroupId should not be empty");
        }
        String path = HTTP_PATH + "/group/get/" + inlongGroupId;
        final String url = formatUrl(path);
        Request request = new Request.Builder().get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            if (responseBody.getErrMsg() != null) {
                if (responseBody.getErrMsg().contains("Inlong group does not exist")) {
                    return null;
                } else {
                    throw new RuntimeException(responseBody.getErrMsg());
                }
            } else {
                return InlongParser.parseGroupInfo(responseBody);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong group get failed: %s", e.getMessage()), e);
        }
    }

    public PageInfo<InlongGroupListResponse> listGroups(String keyword, int status, int pageNum, int pageSize) {
        if (pageNum <= 0) {
            pageNum = 1;
        }
        JSONObject groupQuery = new JSONObject();
        groupQuery.put("keyWord", pageNum);
        groupQuery.put("status", status);
        groupQuery.put("pageNum", pageNum);
        groupQuery.put("pageSize", pageSize);
        String operationData = GsonUtil.toJson(groupQuery);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), operationData);
        String path = HTTP_PATH + "/group/list";
        final String url = formatUrl(path);
        Request request = new Request.Builder().get()
                .url(url)
                .method("POST", requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            if (responseBody.getErrMsg() != null) {
                if (responseBody.getErrMsg().contains("Inlong group does not exist")) {
                    return null;
                } else {
                    throw new RuntimeException(responseBody.getErrMsg());
                }
            } else {
                return InlongParser.parseGroupList(responseBody);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong group get failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Create inlong group
     */
    public String createGroup(InlongGroupRequest groupInfo) {
        String path = HTTP_PATH + "/group/save";
        final String biz = GsonUtil.toJson(groupInfo);
        final RequestBody bizBody = RequestBody.create(MediaType.parse("application/json"), biz);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .method("POST", bizBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData().toString();
        } catch (Exception e) {
            throw new RuntimeException(String.format("inlong group save failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Update inlong group info
     *
     * @return groupId && errMsg
     */
    public Pair<String, String> updateGroup(InlongGroupRequest groupInfo) {
        groupInfo.setCreateTime(null);
        groupInfo.setModifyTime(null);
        String path = HTTP_PATH + "/group/update";
        final String biz = GsonUtil.toJson(groupInfo);
        final RequestBody bizBody = RequestBody.create(MediaType.parse("application/json"), biz);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .method("POST", bizBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            return Pair.of(responseBody.getData().toString(), responseBody.getErrMsg());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong group update failed: %s", e.getMessage()), e);
        }
    }

    public String createStreamInfo(InlongStreamInfo streamInfo) {
        String path = HTTP_PATH + "/stream/save";
        final String stream = GsonUtil.toJson(streamInfo);
        final RequestBody streamBody = RequestBody.create(MediaType.parse("application/json"), stream);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .method("POST", streamBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData().toString();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong stream save failed: %s", e.getMessage()), e);
        }
    }

    public Pair<Boolean, InlongStreamInfo> isStreamExists(InlongStreamInfo streamInfo) {
        InlongStreamInfo currentStreamInfo = getStreamInfo(streamInfo);
        if (currentStreamInfo != null) {
            return Pair.of(true, currentStreamInfo);
        } else {
            return Pair.of(false, null);
        }
    }

    public Pair<Boolean, String> updateStreamInfo(InlongStreamInfo streamInfo) {
        streamInfo.setCreateTime(null);
        streamInfo.setModifyTime(null);
        final String path = HTTP_PATH + "/stream/update";
        final String url = formatUrl(path);
        final String stream = GsonUtil.toJson(streamInfo);
        RequestBody bizBody = RequestBody.create(MediaType.parse("application/json"), stream);
        Request request = new Request.Builder()
                .method("POST", bizBody)
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            if (responseBody.getData() != null) {
                return Pair.of(Boolean.valueOf(responseBody.getData().toString()), responseBody.getErrMsg());
            } else {
                return Pair.of(false, responseBody.getErrMsg());
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong stream update failed with ex:%s", e.getMessage()), e);
        }
    }

    public InlongStreamInfo getStreamInfo(InlongStreamInfo streamInfo) {
        String path = HTTP_PATH + "/stream/get";
        String url = formatUrl(path);
        url += String.format("&groupId=%s&streamId=%s", streamInfo.getInlongGroupId(), streamInfo.getInlongStreamId());
        Request request = new Request.Builder().get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            if (responseBody.getErrMsg() != null) {
                if (responseBody.getErrMsg().contains("Inlong stream does not exist")) {
                    return null;
                } else {
                    throw new RuntimeException(responseBody.getErrMsg());
                }
            } else {
                return InlongParser.parseStreamInfo(responseBody);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong stream get failed with ex:%s", e.getMessage()), e);
        }
    }

    public List<FullStreamResponse> listStreamInfo(String inlongGroupId) {
        final String path = HTTP_PATH + "/stream/listAll";
        String url = formatUrl(path);
        url = url + "&inlongGroupId=" + inlongGroupId;
        Request request = new Request.Builder().get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return InlongParser.parseStreamList(responseBody);
        } catch (Exception e) {
            throw new RuntimeException(String.format("List inlong streams failed: %s", e.getMessage()), e);
        }
    }

    public String createSource(SourceRequest sourceRequest) {
        String path = HTTP_PATH + "/source/save";
        final String source = GsonUtil.toJson(sourceRequest);
        final RequestBody sourceBody = RequestBody.create(MediaType.parse("application/json"), source);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .method("POST", sourceBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData().toString();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong source save failed: %s", e.getMessage()), e);
        }
    }

    public List<SourceListResponse> listSources(String groupId, String streamId) {
        return listSources(groupId, streamId, null);
    }

    public List<SourceListResponse> listSources(String groupId, String streamId, String sourceType) {
        final String path = HTTP_PATH + "/source/list";
        String url = formatUrl(path);
        url = String.format("%s&inlongGroupId=%s&inlongStreamId=%s", url, groupId, streamId);
        if (StringUtils.isNotEmpty(sourceType)) {
            url = String.format("%s&sourceType=%s", url, sourceType);
        }
        Request request = new Request.Builder().get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed:%s", responseBody.getErrMsg()));
            PageInfo<SourceListResponse> sourceListResponsePageInfo = InlongParser.parseSourceList(
                    responseBody);
            return sourceListResponsePageInfo.getList();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong source list failed with ex:%s", e.getMessage()), e);
        }
    }

    public Pair<Boolean, String> updateSource(SourceRequest sourceRequest) {
        final String path = HTTP_PATH + "/source/update";
        final String url = formatUrl(path);
        final String storage = GsonUtil.toJson(sourceRequest);
        final RequestBody storageBody = RequestBody.create(MediaType.parse("application/json"), storage);
        Request request = new Request.Builder()
                .method("POST", storageBody)
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            if (responseBody.getData() != null) {
                return Pair.of(Boolean.valueOf(responseBody.getData().toString()), responseBody.getErrMsg());
            } else {
                return Pair.of(false, responseBody.getErrMsg());
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong source update failed with ex:%s", e.getMessage()), e);
        }
    }

    public String createSink(SinkRequest sinkRequest) {
        String path = HTTP_PATH + "/sink/save";
        final String sink = GsonUtil.toJson(sinkRequest);
        final RequestBody sinkBody = RequestBody.create(MediaType.parse("application/json"), sink);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .method("POST", sinkBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData().toString();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong sink save failed: %s", e.getMessage()), e);
        }
    }

    public List<SinkListResponse> listSinks(String groupId, String streamId) {
        return listSinks(groupId, streamId, null);
    }

    public List<SinkListResponse> listSinks(String groupId, String streamId, String sinkType) {
        final String path = HTTP_PATH + "/sink/list";
        String url = formatUrl(path);
        url = String.format("%s&inlongGroupId=%s&inlongStreamId=%s", url, groupId, streamId);
        if (StringUtils.isNotEmpty(sinkType)) {
            url = String.format("%s&sinkType=%s", url, sinkType);
        }
        Request request = new Request.Builder().get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed:%s", responseBody.getErrMsg()));
            PageInfo<SinkListResponse> sinkListResponsePageInfo = InlongParser.parseSinkList(
                    responseBody);
            return sinkListResponsePageInfo.getList();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong storage list failed with ex:%s", e.getMessage()), e);
        }
    }

    public Pair<Boolean, String> updateSink(SinkRequest sinkRequest) {
        final String path = HTTP_PATH + "/sink/update";
        final String url = formatUrl(path);
        final String storage = GsonUtil.toJson(sinkRequest);
        final RequestBody storageBody = RequestBody.create(MediaType.parse("application/json"), storage);
        Request request = new Request.Builder()
                .method("POST", storageBody)
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            if (responseBody.getData() != null) {
                return Pair.of(Boolean.valueOf(responseBody.getData().toString()), responseBody.getErrMsg());
            } else {
                return Pair.of(false, responseBody.getErrMsg());
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong sink update failed with ex:%s", e.getMessage()), e);
        }
    }

    public WorkflowResult initInlongGroup(InlongGroupRequest groupInfo) {
        final String groupId = groupInfo.getInlongGroupId();
        String path = HTTP_PATH + "/group/startProcess/" + groupId;
        final String url = formatUrl(path);

        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), "");
        Request request = new Request.Builder()
                .url(url)
                .method("POST", requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return InlongParser.parseWorkflowResult(responseBody);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong group init failed: %s", e.getMessage()),
                    e);
        }
    }

    public WorkflowResult startInlongGroup(int taskId,
            Pair<InlongGroupApproveRequest, List<InlongStreamApproveRequest>> initMsg) {

        JSONObject workflowTaskOperation = new JSONObject();
        workflowTaskOperation.put("transferTo", Lists.newArrayList());
        workflowTaskOperation.put("remark", "approved by system");
        JSONObject inlongGroupApproveForm = new JSONObject();
        inlongGroupApproveForm.put("groupApproveInfo", initMsg.getKey());
        inlongGroupApproveForm.put("streamApproveInfoList", initMsg.getValue());
        inlongGroupApproveForm.put("formName", "InlongGroupApproveForm");
        workflowTaskOperation.put("form", inlongGroupApproveForm);
        String operationData = GsonUtil.toJson(workflowTaskOperation);
        final String path = HTTP_PATH + "/workflow/approve/" + taskId;
        final String url = formatUrl(path);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), operationData);
        Request request = new Request.Builder()
                .url(url)
                .method("POST", requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return InlongParser.parseWorkflowResult(responseBody);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong group start failed: %s", e.getMessage()),
                    e);
        }
    }

    public boolean operateInlongGroup(String groupId, InlongGroupState status) {
        String path = HTTP_PATH;
        if (status == InlongGroupState.STOPPED) {
            path += "/group/suspendProcess/";
        } else if (status == InlongGroupState.STARTED) {
            path += "/group/restartProcess/";
        } else {
            throw new IllegalArgumentException(String.format("Unsupported state: %s", status));
        }
        path += groupId;
        final String url = formatUrl(path);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), "");
        Request request = new Request.Builder()
                .url(url)
                .method("POST", requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            String errMsg = responseBody.getErrMsg();
            return errMsg == null || !errMsg.contains("current status was not allowed");
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Inlong group operate: %s failed with ex: %s", status, e.getMessage()), e);
        }
    }

    public boolean deleteInlongGroup(String groupId) {
        final String path = HTTP_PATH + "/group/delete/" + groupId;
        final String url = formatUrl(path);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), "");
        Request request = new Request.Builder()
                .url(url)
                .method("DELETE", requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return Boolean.parseBoolean(responseBody.getData().toString());
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Inlong group delete failed: %s", e.getMessage()), e);
        }
    }

    /**
     * get inlong group error messages
     *
     * @param inlongGroupId
     * @return
     */
    public List<EventLogView> getInlongGroupError(String inlongGroupId) {
        final String path = HTTP_PATH + "/workflow/event/list";
        String url = formatUrl(path);
        url = url + "&inlongGroupId=" + inlongGroupId + "&status=-1";
        Request request = new Request.Builder()
                .get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            PageInfo<EventLogView> pageInfo = InlongParser.parseEventLogViewList(responseBody);
            return pageInfo.getList();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Get inlong group error messages failed: %s", e.getMessage()), e);
        }
    }

    /**
     * get inlong group error messages
     *
     * @param inlongGroupId
     * @return
     */
    public List<InlongStreamConfigLogListResponse> getStreamLogs(String inlongGroupId, String inlongStreamId) {
        final String path = HTTP_PATH + "/stream/config/log/list";
        String url = formatUrl(path);
        url = url + "&inlongGroupId=" + inlongGroupId + "&inlongStreamId=" + inlongStreamId;
        Request request = new Request.Builder()
                .get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            assert response.body() != null;
            String body = response.body().string();
            AssertUtil.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            AssertUtil.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            PageInfo<InlongStreamConfigLogListResponse> pageInfo = InlongParser.parseStreamLogList(responseBody);
            return pageInfo.getList();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Get inlong stream log failed: %s", e.getMessage()), e);
        }
    }

    private String formatUrl(String path) {
        return String.format("http://%s:%s/%s?username=%s&password=%s", host, port, path, uname, passwd);
    }

}
