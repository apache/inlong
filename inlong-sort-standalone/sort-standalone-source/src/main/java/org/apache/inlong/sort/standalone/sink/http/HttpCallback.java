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
import org.apache.inlong.sort.standalone.dispatch.DispatchManager;
import org.apache.inlong.sort.standalone.dispatch.DispatchProfile;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

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
        long sendTime = requestItem.getSendTime();

        // is fail
        if (statusCode != 200) {
            HttpEntity entity = httpResponse.getEntity();
            try (InputStream is = entity.getContent()) {
                int len = (int) entity.getContentLength();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] readed = new byte[len];
                is.read(readed);
                baos.write(readed);
                String content = new String(readed);
                LOG.error("Fail to send http,statusCode:{},content:{}",
                        statusCode, content);
            } catch (Throwable t) {
                LOG.error(t.getMessage(), t);
            }
            handleFailedRequest(sendTime);
        } else {
            if (requestItem.getProfileEvent() != null) {
                ProfileEvent profileEvent = requestItem.getProfileEvent();
                context.addSendResultMetric(profileEvent, context.getTaskName(), true, sendTime);
                context.releaseDispatchQueue(profileEvent);
                profileEvent.ack();
            }
            if (requestItem.getDispatchProfile() != null) {
                DispatchProfile dispatchProfile = requestItem.getDispatchProfile();
                for (ProfileEvent profileEvent : dispatchProfile.getEvents()) {
                    context.addSendResultMetric(profileEvent, context.getTaskName(), true, sendTime);
                    context.releaseDispatchQueue(profileEvent);
                    profileEvent.ack();
                }
            }
        }
    }

    @Override
    public void failed(Exception e) {
        LOG.error("Http request failed,errorMsg:{}", e.getMessage(), e);
        long sendTime = requestItem.getSendTime();
        handleFailedRequest(sendTime);
    }

    @Override
    public void cancelled() {
        LOG.info("Request cancelled");
    }

    private void handleFailedRequest(long sendTime) {
        int remainRetryTimes = requestItem.getRemainRetryTimes();
        if (requestItem.getProfileEvent() != null) {
            ProfileEvent profileEvent = requestItem.getProfileEvent();
            context.addSendResultMetric(profileEvent, context.getTaskName(), false, sendTime);
            // if reach the max retry times, release the request
            if (remainRetryTimes == 1) {
                context.releaseDispatchQueue(profileEvent);
                return;
            } else if (remainRetryTimes > 1) {
                requestItem.setRemainRetryTimes(remainRetryTimes - 1);
            }
            long dispatchTime = profileEvent.getRawLogTime() - profileEvent.getRawLogTime() % DispatchManager.MINUTE_MS;
            DispatchProfile dispatchProfile = new DispatchProfile(profileEvent.getUid(),
                    profileEvent.getInlongGroupId(), profileEvent.getInlongStreamId(),
                    dispatchTime);
            dispatchProfile.addEvent(profileEvent, DispatchManager.DEFAULT_DISPATCH_MAX_PACKCOUNT, Integer.MAX_VALUE);
            context.backDispatchQueue(dispatchProfile);
        }
        if (requestItem.getDispatchProfile() != null) {
            DispatchProfile dispatchProfile = requestItem.getDispatchProfile();
            dispatchProfile.getEvents()
                    .forEach(v -> context.addSendResultMetric(v, context.getTaskName(), false, sendTime));
            context.backDispatchQueue(dispatchProfile);
        }
    }
}
