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
import com.google.common.collect.Lists;
import java.util.List;
import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.auth.Authentication;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.api.util.GsonUtil;
import org.apache.inlong.manager.client.api.util.InlongParser;
import org.apache.inlong.manager.common.pojo.business.BusinessApproveInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageRequest;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamApproveInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.shiro.util.Assert;

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
        Assert.notNull(authentication, "Inlong should be authenticated");
        Assert.isTrue(authentication instanceof DefaultAuthentication,
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

    public boolean isBusinessExists(BusinessInfo businessInfo) {
        String inlongGroupId = businessInfo.getInlongGroupId();
        if (StringUtils.isEmpty(inlongGroupId)) {
            inlongGroupId = "b_" + businessInfo.getName();
        }
        BusinessInfo businessSnapshot = getBusinessInfo(inlongGroupId);
        return businessSnapshot != null;
    }

    public BusinessInfo getBusinessInfo(String inlongGroupId) {
        if (StringUtils.isEmpty(inlongGroupId)) {
            throw new IllegalArgumentException("InlongGroupId should not be empty");
        }
        String path = HTTP_PATH + "/business/get/" + inlongGroupId;
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), "");
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .method("POST", requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            Assert.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            if (responseBody.getErrMsg() != null && responseBody.getErrMsg().contains("Business does not exist")) {
                return null;
            } else {
                return InlongParser.parseBusinessInfo(responseBody.getData());
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong business get failed with ex:%s", e.getMessage()), e);
        }
    }

    public String createBusinessInfo(BusinessInfo businessInfo) {
        String path = HTTP_PATH + "/business/save";
        final String biz = JsonUtils.toJson(businessInfo);
        final RequestBody bizBody = RequestBody.create(MediaType.parse("application/json"), biz);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .method("POST", bizBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            Assert.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            Assert.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed:%s", responseBody.getErrMsg()));
            return responseBody.getData().toString();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong stream group save failed with ex:%s", e.getMessage()), e);
        }
    }

    public String createStreamInfo(DataStreamInfo streamInfo) {
        String path = HTTP_PATH + "/datastream/save";
        final String stream = JsonUtils.toJson(streamInfo);
        final RequestBody streamBody = RequestBody.create(MediaType.parse("application/json"), stream);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .method("POST", streamBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            Assert.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            Assert.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed:%s", responseBody.getErrMsg()));
            return responseBody.getData().toString();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong stream save failed with ex:%s", e.getMessage()), e);
        }
    }

    public String createStorage(StorageRequest storageRequest) {
        String path = HTTP_PATH + "/storage/save";
        final String storage = JsonUtils.toJson(storageRequest);
        final RequestBody storageBody = RequestBody.create(MediaType.parse("application/json"), storage);
        final String url = formatUrl(path);
        Request request = new Request.Builder()
                .url(url)
                .method("POST", storageBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            Assert.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            Assert.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed:%s", responseBody.getErrMsg()));
            return responseBody.getData().toString();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong storage save failed with ex:%s", e.getMessage()), e);
        }
    }

    public WorkflowResult initBusinessInfo(BusinessInfo businessInfo) {
        final String groupId = businessInfo.getInlongGroupId();
        String path = HTTP_PATH + "/business/startProcess/" + groupId;
        final String url = formatUrl(path);

        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), "");
        Request request = new Request.Builder()
                .url(url)
                .method("POST", requestBody)
                .build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            String body = response.body().string();
            Assert.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            Assert.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed:%s", responseBody.getErrMsg()));
            WorkflowResult workflowResult = InlongParser.parseWorkflowResult(responseBody.getData());
            return workflowResult;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Inlong business init failed with ex:%s", e.getMessage()), e);
        }
    }

    public WorkflowResult startBusinessInfo(int taskId,
            Pair<BusinessApproveInfo, List<DataStreamApproveInfo>> initMsg) {

        JSONObject workflowTaskOperation = new JSONObject();
        workflowTaskOperation.put("transferTo", Lists.newArrayList());
        workflowTaskOperation.put("remark", "approve by wedata");
        JSONObject businessAdminApproveForm = new JSONObject();
        businessAdminApproveForm.put("businessApproveInfo", initMsg.getKey());
        businessAdminApproveForm.put("streamApproveInfoList", initMsg.getValue());
        businessAdminApproveForm.put("formName", "BusinessAdminApproveForm");
        workflowTaskOperation.put("form", businessAdminApproveForm);
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
            String body = response.body().string();
            Assert.isTrue(response.isSuccessful(), String.format("Inlong request failed:%s", body));
            org.apache.inlong.manager.common.beans.Response responseBody = InlongParser.parseResponse(body);
            Assert.isTrue(responseBody.getErrMsg() == null,
                    String.format("Inlong request failed:%s", responseBody.getErrMsg()));
            WorkflowResult workflowResult = InlongParser.parseWorkflowResult(responseBody.getData());
            return workflowResult;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Finish inlong business init failed with ex:%s", e.getMessage()),
                    e);
        }
    }

    private String formatUrl(String path) {
        return String.format("http://%s:%s/%s?username=%s&password=%s", host, port, path, uname, passwd);
    }

}
