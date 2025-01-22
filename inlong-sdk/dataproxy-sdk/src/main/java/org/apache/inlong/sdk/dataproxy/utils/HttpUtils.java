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

package org.apache.inlong.sdk.dataproxy.utils;

import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

/**
 * Http(s) Utils class
 *
 * Used to place public processing functions related to HTTP(s)
 */
public class HttpUtils {

    private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);
    private static final LogCounter exceptCnt = new LogCounter(10, 200000, 60 * 1000L);

    public static boolean constructHttpClient(boolean rptByHttps,
            int socketTimeoutMs, int conTimeoutMs, String tlsVer, ProcessResult procResult) {
        CloseableHttpClient httpClient;
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(socketTimeoutMs)
                .setConnectTimeout(conTimeoutMs).build();
        try {
            if (rptByHttps) {
                SSLContext sslContext = SSLContexts.custom().build();
                SSLConnectionSocketFactory sslSf = new SSLConnectionSocketFactory(sslContext,
                        new String[]{tlsVer}, null,
                        SSLConnectionSocketFactory.getDefaultHostnameVerifier());
                httpClient = HttpClients.custom()
                        .setDefaultRequestConfig(requestConfig)
                        .setSSLSocketFactory(sslSf).build();
            } else {
                httpClient = HttpClientBuilder.create()
                        .setDefaultRequestConfig(requestConfig).build();
            }
            return procResult.setSuccess(httpClient);
        } catch (Throwable ex) {
            if (exceptCnt.shouldPrint()) {
                logger.error("Build http client exception", ex);
            }
            return procResult.setFailResult(ErrorCode.HTTP_BUILD_CLIENT_EXCEPTION, ex.getMessage());
        }
    }
}
