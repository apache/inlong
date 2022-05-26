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
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.InlongGroupContext.InlongGroupStatus;
import org.apache.inlong.manager.client.api.util.GsonUtils;
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

import java.util.List;

/**
 * InnerInlongManagerClient is used to invoke http api of inlong manager.
 */
@Slf4j
public class InnerInlongManagerClient {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final String HTTP_PATH = "api/inlong/manager";

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
    public boolean isGroupExists(String inlongGroupId) {
        AssertUtils.notEmpty(inlongGroupId, "InlongGroupId should not be empty");

        String path = HTTP_PATH + "/group/exist/" + inlongGroupId;
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);

            Response<Boolean> responseBody = InlongParser.parseResponse(Boolean.class, body);
            if (!responseBody.isSuccess()) {
                throw new RuntimeException(responseBody.getErrMsg());
            }

            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong group check exists failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Get information of group.
     */
    public InlongGroupInfo getGroupInfo(String inlongGroupId) {
        if (StringUtils.isEmpty(inlongGroupId)) {
            throw new IllegalArgumentException("InlongGroupId should not be empty");
        }
        String path = HTTP_PATH + "/group/get/" + inlongGroupId;
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response responseBody = InlongParser.parseResponse(body);
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

    /**
     * Get information of groups.
     */
    public PageInfo<InlongGroupListResponse> listGroups(String keyword, int status, int pageNum, int pageSize) {
        if (pageNum <= 0) {
            pageNum = 1;
        }

        ObjectNode groupQuery = OBJECT_MAPPER.createObjectNode();
        groupQuery.put("keyword", keyword);
        groupQuery.put("status", status);
        groupQuery.put("pageNum", pageNum);
        groupQuery.put("pageSize", pageSize);
        String operationData = groupQuery.toString();

        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), operationData);
        String path = HTTP_PATH + "/group/list";
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response responseBody = InlongParser.parseResponse(body);
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
     * List inlong group by the page request
     *
     * @param pageRequest The pageRequest
     * @return Response encapsulate of inlong group list
     */
    public Response<PageInfo<InlongGroupListResponse>> listGroups(InlongGroupPageRequest pageRequest) throws Exception {
        String requestParams = GsonUtils.toJson(pageRequest);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), requestParams);
        String path = HTTP_PATH + "/group/list";
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();
        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);

            return OBJECT_MAPPER.readValue(body, new TypeReference<Response<PageInfo<InlongGroupListResponse>>>() {
            });
        }
    }

    /**
     * Create inlong group
     */
    public String createGroup(InlongGroupRequest groupInfo) {
        String path = HTTP_PATH + "/group/save";
        final String biz = GsonUtils.toJson(groupInfo);
        final RequestBody bizBody = RequestBody.create(MediaType.parse("application/json"), biz);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .post(bizBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<String> responseBody = InlongParser.parseResponse(String.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(String.format("inlong group save failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Update inlong group info
     *
     * @return groupId && errMsg
     */
    public Pair<String, String> updateGroup(InlongGroupRequest groupRequest) {
        String path = HTTP_PATH + "/group/update";
        final String group = GsonUtils.toJson(groupRequest);
        final RequestBody groupBody = RequestBody.create(MediaType.parse("application/json"), group);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .post(groupBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<String> responseBody = InlongParser.parseResponse(String.class, body);
            return Pair.of(responseBody.getData(), responseBody.getErrMsg());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong group update failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Create information of stream.
     */
    public Double createStreamInfo(InlongStreamInfo streamInfo) {
        String path = HTTP_PATH + "/stream/save";
        final String stream = GsonUtils.toJson(streamInfo);
        final RequestBody streamBody = RequestBody.create(MediaType.parse("application/json"), stream);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .post(streamBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Double> responseBody = InlongParser.parseResponse(Double.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong stream save failed: %s", e.getMessage()), e);
        }
    }

    public Boolean isStreamExists(InlongStreamInfo streamInfo) {
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        AssertUtils.notEmpty(groupId, "InlongGroupId should not be empty");
        AssertUtils.notEmpty(streamId, "InlongStreamId should not be empty");
        String path = HTTP_PATH + "/stream/exist/" + groupId + "/" + streamId;
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .get()
                .url(url)
                .build();

        try (okhttp3.Response response = httpClient.newCall(request).execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Boolean> responseBody = InlongParser.parseResponse(Boolean.class, body);
            if (!responseBody.isSuccess()) {
                throw new RuntimeException(responseBody.getErrMsg());
            }

            return responseBody.getData();
        } catch (Exception e) {
            log.error("Inlong stream check exists failed", e);
            throw new RuntimeException(String.format("Inlong stream check exists failed: %s", e.getMessage()), e);
        }
    }

    public Pair<Boolean, String> updateStreamInfo(InlongStreamInfo streamInfo) {
        streamInfo.setCreateTime(null);
        streamInfo.setModifyTime(null);
        final String path = HTTP_PATH + "/stream/update";
        final String url = formatUrl(path);
        final String stream = GsonUtils.toJson(streamInfo);
        RequestBody bizBody = RequestBody.create(MediaType.parse("application/json"), stream);
        Request request = new Request.Builder()
                .post(bizBody)
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Boolean> responseBody = InlongParser.parseResponse(Boolean.class, body);
            if (responseBody.getData() != null) {
                return Pair.of(responseBody.getData(), responseBody.getErrMsg());
            } else {
                return Pair.of(false, responseBody.getErrMsg());
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong stream update failed with ex:%s", e.getMessage()), e);
        }
    }

    /**
     * Get information through information of  Inlong's stream.
     */
    public InlongStreamInfo getStreamInfo(InlongStreamInfo streamInfo) {
        String path = HTTP_PATH + "/stream/get";
        String url = formatUrl(path);
        url += String.format("&groupId=%s&streamId=%s", streamInfo.getInlongGroupId(), streamInfo.getInlongStreamId());
        Request request = new Request.Builder().get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response responseBody = InlongParser.parseResponse(body);
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

    /**
     * Get information of stream.
     */
    public List<FullStreamResponse> listStreamInfo(String inlongGroupId) {
        InlongStreamPageRequest pageRequest = new InlongStreamPageRequest();
        pageRequest.setInlongGroupId(inlongGroupId);
        String requestParams = GsonUtils.toJson(pageRequest);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), requestParams);
        final String path = HTTP_PATH + "/stream/listAll";
        final String url = formatUrl(path);
        Request request = new Request.Builder().get()
                .url(url)
                .post(requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response responseBody = InlongParser.parseResponse(body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return InlongParser.parseStreamList(responseBody);
        } catch (Exception e) {
            throw new RuntimeException(String.format("List inlong streams failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Create a data source.
     */
    public Double createSource(SourceRequest sourceRequest) {
        String path = HTTP_PATH + "/source/save";
        final String source = GsonUtils.toJson(sourceRequest);
        final RequestBody sourceBody = RequestBody.create(MediaType.parse("application/json"), source);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .post(sourceBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            AssertUtils.isTrue(response.isSuccessful(), String.format("Inlong request failed: %s", body));
            Response<Double> responseBody = InlongParser.parseResponse(Double.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong source save failed: %s", e.getMessage()), e);
        }
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
        try (okhttp3.Response response = call.execute()) {
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response responseBody = InlongParser.parseResponse(body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed:%s", responseBody.getErrMsg()));
            PageInfo<SourceListResponse> sourceListResponsePageInfo = InlongParser.parseSourceList(
                    responseBody);
            return sourceListResponsePageInfo.getList();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong source list failed with ex:%s", e.getMessage()), e);
        }
    }

    /**
     * Update data Source Information.
     */
    public Pair<Boolean, String> updateSource(SourceRequest sourceRequest) {
        final String path = HTTP_PATH + "/source/update";
        final String url = formatUrl(path);
        final String storage = GsonUtils.toJson(sourceRequest);
        final RequestBody storageBody = RequestBody.create(MediaType.parse("application/json"), storage);
        Request request = new Request.Builder()
                .post(storageBody)
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Boolean> responseBody = InlongParser.parseResponse(Boolean.class, body);
            if (responseBody.getData() != null) {
                return Pair.of(responseBody.getData(), responseBody.getErrMsg());
            } else {
                return Pair.of(false, responseBody.getErrMsg());
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong source update failed with ex:%s", e.getMessage()), e);
        }
    }

    /**
     * Delete data source information by id.
     */
    public boolean deleteSource(int id) {
        AssertUtils.isTrue(id > 0, "sourceId is illegal");
        final String path = HTTP_PATH + "/source/delete/" + id;
        String url = formatUrl(path);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), "");
        Request request = new Request.Builder()
                .url(url)
                .delete(requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Boolean> responseBody = InlongParser.parseResponse(Boolean.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Inlong source delete failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Create a conversion function information.
     */
    public Double createTransform(TransformRequest transformRequest) {
        String path = HTTP_PATH + "/transform/save";
        final String sink = GsonUtils.toJson(transformRequest);
        final RequestBody transformBody = RequestBody.create(MediaType.parse("application/json"), sink);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .post(transformBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Double> responseBody = InlongParser.parseResponse(Double.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong transform save failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Get all conversion function information.
     */
    public List<TransformResponse> listTransform(String groupId, String streamId) {
        final String path = HTTP_PATH + "/transform/list";
        String url = formatUrl(path);
        url = String.format("%s&inlongGroupId=%s&inlongStreamId=%s", url, groupId, streamId);
        Request request = new Request.Builder().get()
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try {
            okhttp3.Response response = call.execute();
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response responseBody = InlongParser.parseResponse(body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed:%s", responseBody.getErrMsg()));
            return InlongParser.parseTransformList(responseBody);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong transform list failed with ex:%s", e.getMessage()), e);
        }
    }

    /**
     * Update conversion function information.
     */
    public Pair<Boolean, String> updateTransform(TransformRequest transformRequest) {
        final String path = HTTP_PATH + "/transform/update";
        final String url = formatUrl(path);
        final String transform = GsonUtils.toJson(transformRequest);
        final RequestBody storageBody = RequestBody.create(MediaType.parse("application/json"), transform);
        Request request = new Request.Builder()
                .method("POST", storageBody)
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Boolean> responseBody = InlongParser.parseResponse(Boolean.class, body);
            if (responseBody.getData() != null) {
                return Pair.of(responseBody.getData(), responseBody.getErrMsg());
            } else {
                return Pair.of(false, responseBody.getErrMsg());
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong transform update failed with ex:%s", e.getMessage()), e);
        }
    }

    /**
     * Delete conversion function information.
     */
    public boolean deleteTransform(TransformRequest transformRequest) {
        AssertUtils.notEmpty(transformRequest.getInlongGroupId(), "inlongGroupId should not be null");
        AssertUtils.notEmpty(transformRequest.getInlongStreamId(), "inlongStreamId should not be null");
        AssertUtils.notEmpty(transformRequest.getTransformName(), "transformName should not be null");
        final String path = HTTP_PATH + "/transform/delete";
        String url = formatUrl(path);
        url = String.format("%s&inlongGroupId=%s&inlongStreamId=%s&transformName=%s", url,
                transformRequest.getInlongGroupId(),
                transformRequest.getInlongStreamId(),
                transformRequest.getTransformName());
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), "");
        Request request = new Request.Builder()
                .url(url)
                .delete(requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Boolean> responseBody = InlongParser.parseResponse(Boolean.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Inlong transform delete failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Create information of data sink.
     */
    public Double createSink(SinkRequest sinkRequest) {
        String path = HTTP_PATH + "/sink/save";
        final String sink = GsonUtils.toJson(sinkRequest);
        final RequestBody sinkBody = RequestBody.create(MediaType.parse("application/json"), sink);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .post(sinkBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Double> responseBody = InlongParser.parseResponse(Double.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong sink save failed: %s", e.getMessage()), e);
        }
    }

    /**
     * Delete information of data sink by ID.
     */
    public boolean deleteSink(int id) {
        AssertUtils.isTrue(id > 0, "sinkId is illegal");
        final String path = HTTP_PATH + "/sink/delete/" + id;
        String url = formatUrl(path);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), "");
        Request request = new Request.Builder()
                .url(url)
                .delete(requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Boolean> responseBody = InlongParser.parseResponse(Boolean.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Inlong sink delete failed: %s", e.getMessage()), e);
        }
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
        try (okhttp3.Response response = call.execute()) {
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response responseBody = InlongParser.parseResponse(body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed:%s", responseBody.getErrMsg()));
            PageInfo<SinkListResponse> sinkListResponsePageInfo = InlongParser.parseSinkList(
                    responseBody);
            return sinkListResponsePageInfo.getList();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong storage list failed with ex:%s", e.getMessage()), e);
        }
    }

    /**
     * Update information of data sink.
     */
    public Pair<Boolean, String> updateSink(SinkRequest sinkRequest) {
        final String path = HTTP_PATH + "/sink/update";
        final String url = formatUrl(path);
        final String storage = GsonUtils.toJson(sinkRequest);
        final RequestBody storageBody = RequestBody.create(MediaType.parse("application/json"), storage);
        Request request = new Request.Builder()
                .post(storageBody)
                .url(url)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Boolean> responseBody = InlongParser.parseResponse(Boolean.class, body);
            if (responseBody.getData() != null) {
                return Pair.of(responseBody.getData(), responseBody.getErrMsg());
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
                .post(requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<WorkflowResult> responseBody = InlongParser.parseResponse(WorkflowResult.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong group init failed: %s", e.getMessage()),
                    e);
        }
    }

    public WorkflowResult startInlongGroup(int taskId,
            Pair<InlongGroupApproveRequest, List<InlongStreamApproveRequest>> initMsg) {

        ObjectNode workflowTaskOperation = OBJECT_MAPPER.createObjectNode();
        workflowTaskOperation.putPOJO("transferTo", Lists.newArrayList());
        workflowTaskOperation.put("remark", "approved by system");

        ObjectNode inlongGroupApproveForm = OBJECT_MAPPER.createObjectNode();
        inlongGroupApproveForm.putPOJO("groupApproveInfo", initMsg.getKey());
        inlongGroupApproveForm.putPOJO("streamApproveInfoList", initMsg.getValue());
        inlongGroupApproveForm.put("formName", "InlongGroupApproveForm");
        workflowTaskOperation.set("form", inlongGroupApproveForm);

        String operationData = workflowTaskOperation.toString();

        final String path = HTTP_PATH + "/workflow/approve/" + taskId;
        final String url = formatUrl(path);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), operationData);
        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<WorkflowResult> responseBody = InlongParser.parseResponse(WorkflowResult.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong group start failed: %s", e.getMessage()),
                    e);
        }
    }

    public boolean operateInlongGroup(String groupId, InlongGroupStatus status) {
        return operateInlongGroup(groupId, status, false);
    }

    public boolean operateInlongGroup(String groupId, InlongGroupStatus status, boolean async) {
        String path = HTTP_PATH;
        if (status == InlongGroupStatus.STOPPED) {
            if (async) {
                path += "/group/suspendProcessAsync/";
            } else {
                path += "/group/suspendProcess/";
            }
        } else if (status == InlongGroupStatus.STARTED) {
            if (async) {
                path += "/group/restartProcessAsync/";
            } else {
                path += "/group/restartProcess/";
            }
        } else {
            throw new IllegalArgumentException(String.format("Unsupported state: %s", status));
        }
        path += groupId;
        final String url = formatUrl(path);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), "");
        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response responseBody = InlongParser.parseResponse(body);
            String errMsg = responseBody.getErrMsg();
            return errMsg == null || !errMsg.contains("current status was not allowed");
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Inlong group operate: %s failed with ex: %s", status, e.getMessage()), e);
        }
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
        final String url = formatUrl(path);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), "");
        Request request = new Request.Builder()
                .url(url)
                .delete(requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response<Boolean> responseBody = InlongParser.parseResponse(Boolean.class, body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            return responseBody.getData();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Inlong group delete failed: %s", e.getMessage()), e);
        }
    }

    /**
     * get inlong group error messages
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
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response responseBody = InlongParser.parseResponse(body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            PageInfo<EventLogView> pageInfo = InlongParser.parseEventLogViewList(responseBody);
            return pageInfo.getList();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Get inlong group error messages failed: %s", e.getMessage()), e);
        }
    }

    /**
     * get inlong group error messages
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
        try (okhttp3.Response response = call.execute()) {
            assert response.body() != null;
            String body = response.body().string();
            assertHttpSuccess(response, body, path);
            Response responseBody = InlongParser.parseResponse(body);
            AssertUtils.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed: %s", responseBody.getErrMsg()));
            PageInfo<InlongStreamConfigLogListResponse> pageInfo = InlongParser.parseStreamLogList(responseBody);
            return pageInfo.getList();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Get inlong stream log failed: %s", e.getMessage()), e);
        }
    }

    protected void assertHttpSuccess(okhttp3.Response response, String body, String path) {
        AssertUtils.isTrue(response.isSuccessful(), String.format("Inlong request=%s failed: %s", path, body));
    }

    protected String formatUrl(String path) {
        return String.format("http://%s:%s/%s?username=%s&password=%s", host, port, path, uname, passwd);
    }

}
