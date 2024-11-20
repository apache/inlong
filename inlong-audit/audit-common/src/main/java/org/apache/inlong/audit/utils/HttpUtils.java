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

package org.apache.inlong.audit.utils;

import org.apache.inlong.common.util.BasicAuth;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.util.HashMap;
import java.util.Map;

public class HttpUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtils.class);
    private static final String PARAM_COMPONENT = "component";
    private static HttpClient httpClient = null;

    static {
        try {
            SSLContext sslContext = SSLContextBuilder.create()
                    .loadTrustMaterial(new TrustSelfSignedStrategy())
                    .build();

            httpClient = HttpClientBuilder.create()
                    .setSSLContext(sslContext)
                    .build();
        } catch (Exception e) {
            LOGGER.error("Error initializing SSL context or HTTP client", e);
        }
    }

    public static Map<String, String> getAuthHeader(String secretId, String secretKey) {
        Map<String, String> header = new HashMap<>();
        try {
            header.put(BasicAuth.BASIC_AUTH_HEADER,
                    BasicAuth.genBasicAuthCredential(secretId, secretKey));
        } catch (Exception e) {
            LOGGER.error("Get auth header error", e);
        }
        return header;
    }

    public static String httpGet(String component, String url, String secretId, String secretKey, int timeoutMs) {
        if (httpClient == null) {
            LOGGER.error("httpClient is null");
            return null;
        }
        try {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(timeoutMs)
                    .setConnectionRequestTimeout(timeoutMs)
                    .setSocketTimeout(timeoutMs)
                    .build();
            URIBuilder uriBuilder = new URIBuilder(url);
            uriBuilder.addParameter(PARAM_COMPONENT, component);
            String finalUrl = uriBuilder.build().toString();

            HttpGet request = new HttpGet(finalUrl);
            request.setConfig(requestConfig);

            Map<String, String> authHeaders = getAuthHeader(secretId, secretKey);
            for (Map.Entry<String, String> entry : authHeaders.entrySet()) {
                request.addHeader(entry.getKey(), entry.getValue());
            }

            try (CloseableHttpResponse response = (CloseableHttpResponse) httpClient.execute(request)) {
                String responseStr = EntityUtils.toString(response.getEntity());
                LOGGER.info("Http response: {}", responseStr);
                if (responseStr != null && !responseStr.isEmpty()
                        && response.getStatusLine().getStatusCode() == 200) {
                    return responseStr;
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Http request url = {}, secretId = {}, secretKey = {}, component = {} has exception!", url,
                    secretId, secretKey, component, e);
        }
        return null;
    }
}
