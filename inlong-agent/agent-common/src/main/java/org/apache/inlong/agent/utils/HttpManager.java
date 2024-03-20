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

package org.apache.inlong.agent.utils;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.common.util.BasicAuth;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_HTTP_APPLICATION_JSON;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_HTTP_SUCCESS_CODE;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_ADDR;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_AUTH_SECRET_ID;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_AUTH_SECRET_KEY;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_REQUEST_TIMEOUT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_VIP_HTTP_PREFIX_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_REQUEST_TIMEOUT;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH;

/**
 * Perform http operation
 */
public class HttpManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpManager.class);
    private static final Gson gson;

    static {
        final GsonBuilder gsonBuilder = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss");
        gson = gsonBuilder.create();
    }

    private final CloseableHttpClient httpClient;
    private final String baseUrl;
    private final String secretId;
    private final String secretKey;
    private static boolean enableHttps;

    public HttpManager(AgentConfiguration conf) {
        this(conf.get(AGENT_MANAGER_ADDR),
                conf.get(AGENT_MANAGER_VIP_HTTP_PREFIX_PATH, DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH),
                conf.getInt(AGENT_MANAGER_REQUEST_TIMEOUT,
                        DEFAULT_AGENT_MANAGER_REQUEST_TIMEOUT),
                conf.get(AGENT_MANAGER_AUTH_SECRET_ID),
                conf.get(AGENT_MANAGER_AUTH_SECRET_KEY));
    }

    public HttpManager(String managerAddr, String managerHttpPrefixPath, int timeout, String secretId,
            String secretKey) {
        baseUrl = managerAddr + managerHttpPrefixPath;
        enableHttps = StringUtils.startsWith(managerAddr, "https");
        if (enableHttps) {
            httpClient = constructHttpsClient(timeout);
        } else {
            httpClient = constructHttpClient(timeout);
        }
        this.secretId = secretId;
        this.secretKey = secretKey;
    }

    /**
     * build base url for manager according to config
     *
     * example(http)  - http://127.0.0.1:8080/inlong/manager/openapi
     * example(https) - https://127.0.0.1:8080/inlong/manager/openapi
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    /**
     * construct http client
     *
     * @param timeout timeout setting
     * @return closeable timeout
     */
    private synchronized CloseableHttpClient constructHttpClient(int timeout) {
        if (httpClient != null) {
            return httpClient;
        }
        long timeoutInMs = TimeUnit.SECONDS.toMillis(timeout);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout((int) timeoutInMs)
                .setSocketTimeout((int) timeoutInMs).build();
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setDefaultRequestConfig(requestConfig);
        return httpClientBuilder.build();
    }

    /**
     * construct https client
     *
     * @param timeout timeout setting
     * @return closeable timeout
     */
    private static CloseableHttpClient constructHttpsClient(int timeout) {
        long timeoutInMs = TimeUnit.SECONDS.toMillis(timeout);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout((int) timeoutInMs)
                .setSocketTimeout((int) timeoutInMs).build();
        SSLContext sslContext = null;
        try {
            sslContext = SSLContexts.custom().build();
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("constructHttpsClient error ", e);
        } catch (KeyManagementException e) {
            LOGGER.error("constructHttpsClient error ", e);
        }
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
                new String[]{"TLSv1.2"}, null,
                SSLConnectionSocketFactory.getDefaultHostnameVerifier());

        return HttpClients.custom().setDefaultRequestConfig(requestConfig).setSSLSocketFactory(sslsf).build();
    }

    /**
     * doPost
     *
     * @param dto content body needed to post
     * @return response
     */
    public String doSentPost(String url, Object dto) {
        try {
            HttpPost post = getHttpPost(url);
            Map<String, String> authHeader = getAuthHeader();
            authHeader.forEach((k, v) -> {
                post.addHeader(k, v);
            });
            StringEntity stringEntity = new StringEntity(toJsonStr(dto), Charset.forName("UTF-8"));
            stringEntity.setContentType(AGENT_HTTP_APPLICATION_JSON);
            post.setEntity(stringEntity);
            CloseableHttpResponse response = httpClient.execute(post);
            String returnStr = EntityUtils.toString(response.getEntity());
            if (returnStr != null && !returnStr.isEmpty()
                    && response.getStatusLine().getStatusCode() == 200) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("request url {}, dto: {}, return str {}", url, dto, returnStr);
                }
                return returnStr;
            }
        } catch (Exception e) {
            LOGGER.error("request url {}, request dto {} error :" + e.getMessage(), url, dto);
            return null;
        }
        return null;
    }

    public String toJsonStr(Object obj) {
        return gson.toJson(obj);
    }

    /**
     * doGet
     *
     * @return response
     */
    public String doSendPost(String url) {
        try {
            HttpPost post = getHttpPost(url);
            Map<String, String> authHeader = getAuthHeader();
            authHeader.forEach((k, v) -> {
                post.addHeader(k, v);
            });
            CloseableHttpResponse response = httpClient.execute(post);
            String returnStr = EntityUtils.toString(response.getEntity());
            if (returnStr != null && !returnStr.isEmpty()
                    && response.getStatusLine().getStatusCode() == AGENT_HTTP_SUCCESS_CODE) {
                return returnStr;
            }
        } catch (Exception e) {
            LOGGER.error("request url {} error :" + e.getMessage(), url);
            return null;
        }
        return null;
    }

    /**
     * get http post, the tauth params should be passed
     */
    private HttpPost getHttpPost(String url) {
        return new HttpPost(url);
    }

    /**
     * get http get, the tauth params should be passed
     */
    private HttpGet getHttpGet(String url) {
        return new HttpGet(url);
    }

    public Map<String, String> getAuthHeader() {
        Map<String, String> header = new HashMap<>();
        try {
            header.put(BasicAuth.BASIC_AUTH_HEADER, BasicAuth.genBasicAuthCredential(secretId, secretKey));
        } catch (Exception e) {
            LOGGER.error("Get auth header error", e);
        }
        return header;
    }
}
