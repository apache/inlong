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
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;

public class HttpCallback implements FutureCallback<HttpResponse> {

    public static final Logger LOG = InlongLoggerFactory.getLogger(HttpCallback.class);

    private HttpSinkContext context;
    private HttpRequest requestItem;

    public HttpCallback(HttpSinkContext context, HttpRequest requestItem) {
        this.context = context;
        this.requestItem = requestItem;
    }

    @Override
    public void completed(HttpResponse httpResponse) {
        int statusCode = httpResponse.getStatusLine().getStatusCode();
        ProfileEvent event = requestItem.getEvent();
        long sendTime = requestItem.getSendTime();

        // is fail
        if (statusCode != 200) {
            handleFailedRequest(event, sendTime);
        } else {
            context.addSendResultMetric(event, context.getTaskName(), true, sendTime);
            context.releaseDispatchQueue(requestItem);
            event.ack();
        }
    }

    @Override
    public void failed(Exception e) {
        LOG.error("Http request failed,errorMsg:{}", e.getMessage(), e);
        ProfileEvent event = requestItem.getEvent();
        long sendTime = requestItem.getSendTime();
        handleFailedRequest(event, sendTime);
    }

    @Override
    public void cancelled() {
        LOG.info("Request cancelled");
    }

    private void handleFailedRequest(ProfileEvent event, long sendTime) {
        int remainRetryTimes = requestItem.getRemainRetryTimes();
        context.addSendResultMetric(event, context.getTaskName(), false, sendTime);
        // if reach the max retry times, release the request
        if (remainRetryTimes == 1) {
            context.releaseDispatchQueue(requestItem);
            return;
        } else if (remainRetryTimes > 1) {
            requestItem.setRemainRetryTimes(remainRetryTimes - 1);
        }
        context.backDispatchQueue(requestItem);
    }
}
