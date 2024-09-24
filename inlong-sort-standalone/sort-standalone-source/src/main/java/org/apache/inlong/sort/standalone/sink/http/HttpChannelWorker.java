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

import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.dispatch.DispatchProfile;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public class HttpChannelWorker extends Thread {

    public static final Logger LOG = LoggerFactory.getLogger(HttpChannelWorker.class);

    private final HttpSinkContext context;
    private final int workerIndex;

    private LifecycleState status;
    private IEvent2HttpRequestHandler handler;
    private CloseableHttpAsyncClient httpClient;

    public HttpChannelWorker(HttpSinkContext context, int workerIndex) {
        this.context = context;
        this.workerIndex = workerIndex;
        this.status = LifecycleState.IDLE;
        this.handler = context.createHttpRequestHandler();
    }

    @Override
    public void run() {
        status = LifecycleState.START;
        LOG.info("Starting HttpChannelWorker:{},status:{},index:{}", context.getTaskName(), status, workerIndex);
        this.initHttpClient();
        while (status == LifecycleState.START) {
            try {
                this.doRun();
            } catch (Throwable t) {
                LOG.error("Error occurred while starting HttpChannelWorker:{},status:{},index:{}",
                        context.getTaskName(), status, workerIndex, t);
            }
        }
    }

    public void doRun() throws InterruptedException, JsonProcessingException, URISyntaxException {
        DispatchProfile dispatchProfile = context.takeDispatchQueue();
        if (dispatchProfile == null) {
            Thread.sleep(context.getProcessInterval());
            return;
        }
        // check id config
        String uid = dispatchProfile.getUid();
        if (context.getIdConfig(uid) == null) {
            for (ProfileEvent profileEvent : dispatchProfile.getEvents()) {
                context.addSendResultMetric(profileEvent, context.getTaskName(), false, System.currentTimeMillis());
                profileEvent.ack();
            }
            return;
        }
        // send
        try {
            // parse request
            List<HttpRequest> requests = handler.parse(context, dispatchProfile);
            // check request
            if (requests == null) {
                for (ProfileEvent profileEvent : dispatchProfile.getEvents()) {
                    context.addSendResultMetric(profileEvent, context.getTaskName(), false, System.currentTimeMillis());
                    context.releaseDispatchQueue(dispatchProfile);
                    profileEvent.ack();
                }
            }
            for (HttpRequest request : requests) {
                httpClient.execute(request.getRequest(), new HttpCallback(context, request));
                for (ProfileEvent profileEvent : dispatchProfile.getEvents()) {
                    context.addSendMetric(profileEvent, context.getTaskName());
                }
            }
        } catch (Throwable e) {
            LOG.error("Failed to send HttpRequest uid:{},error:{}", dispatchProfile.getUid(), e.getMessage(), e);
            context.backDispatchQueue(dispatchProfile);
            this.initHttpClient();
            Thread.sleep(context.getProcessInterval());
        }
    }

    public void close() {
        this.status = LifecycleState.STOP;
    }

    private void initHttpClient() {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOG.error(String.format("close HttpClient:%s", e.getMessage()), e);
            }
            httpClient = null;
        }
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
            LOG.error("init httpclient failed,error:{}", e.getMessage(), e);
            httpClient = null;
        }
    }
}
