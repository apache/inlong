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

package org.apache.inlong.sort.standalone.sink.http;

import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import org.apache.flume.lifecycle.LifecycleState;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.slf4j.Logger;

import java.io.IOException;

public class HttpOutputChannel extends Thread {

    public static final Logger LOG = InlongLoggerFactory.getLogger(HttpOutputChannel.class);

    private LifecycleState status;
    private HttpSinkContext context;
    private CloseableHttpAsyncClient httpClient;

    public HttpOutputChannel(HttpSinkContext context) {
        super(context.getTaskName());
        this.context = context;
        this.status = LifecycleState.IDLE;
    }

    public void init() {
        initHttpClient();
    }

    private boolean initHttpClient() {
        try {
            if (httpClient == null) {
                String userName = context.getUsername();
                String password = context.getPassword();
                LOG.info("initHttpAsyncClient:url:{}", context.getBaseUrl());

                HttpAsyncClientBuilder builder = HttpAsyncClients.custom();
                final CredentialsProvider provider = new BasicCredentialsProvider();
                if (context.getEnableCredential()) {
                    provider.setCredentials(AuthScope.ANY,
                            new UsernamePasswordCredentials(userName, password));
                    builder.setDefaultCredentialsProvider(provider);
                }

                RequestConfig requestConfig = RequestConfig.custom()
                        .setConnectionRequestTimeout(context.getConnectionRequestTimeout())
                        .setSocketTimeout(context.getSocketTimeout())
                        .setMaxRedirects(context.getMaxRedirects())
                        .setConnectTimeout(120 * 1000)
                        .build();

                builder.setDefaultRequestConfig(requestConfig)
                        .setMaxConnTotal(context.getMaxConnect())
                        .setMaxConnPerRoute(context.getMaxConnectPerRoute());

                httpClient = HttpSinkFactory.createHttpAsyncClient(builder);
                httpClient.start();
            }
        } catch (Exception e) {
            LOG.error("init httpclient failed.", e);
            httpClient = null;
            return false;
        }
        return true;
    }

    public void close() {
        status = LifecycleState.STOP;
        try {
            httpClient.close();
        } catch (IOException e) {
            LOG.error(String.format("close HttpClient:%s", e.getMessage()), e);
        }
    }

    @Override
    public void run() {
        status = LifecycleState.START;
        LOG.info("Starting HttpOutputChannel:{},status:{}", context.getTaskName(), status);
        while (status == LifecycleState.START) {
            try {
                send();
            } catch (Throwable t) {
                LOG.error("Error occurred while starting HttpOutputChannel:{},status:{}", context.getTaskName(), status,
                        t);
            }
        }
    }

    public void send() throws InterruptedException {
        HttpRequest httpRequest = null;
        try {
            // get httpRequest
            httpRequest = context.takeDispatchQueue();
            if (httpRequest == null) {
                Thread.sleep(context.getProcessInterval());
                return;
            }
            // get id config
            String uid = httpRequest.getEvent().getUid();
            if (context.getIdConfig(uid) == null) {
                context.addSendResultMetric(httpRequest.getEvent(), context.getTaskName(), false,
                        httpRequest.getSendTime());
                return;
            }
            // send
            httpClient.execute(httpRequest.getRequest(), new HttpCallback(context, httpRequest));
            context.addSendMetric(httpRequest.getEvent(), context.getTaskName());
        } catch (Throwable e) {
            LOG.error("Failed to send HttpRequest '{}': {}", httpRequest, e.getMessage(), e);
            if (httpRequest != null) {
                context.backDispatchQueue(httpRequest);
                context.addSendResultMetric(httpRequest.getEvent(), context.getTaskName(), false,
                        httpRequest.getSendTime());
            }
            try {
                Thread.sleep(context.getProcessInterval());
            } catch (InterruptedException e1) {
                LOG.error("Thread interrupted while sleeping, error: {}", e1.getMessage(), e1);
            }
        }
    }
}
