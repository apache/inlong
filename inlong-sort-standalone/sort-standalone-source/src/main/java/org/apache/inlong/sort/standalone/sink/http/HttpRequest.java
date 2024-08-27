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

import org.apache.http.client.methods.HttpUriRequest;

public class HttpRequest {

    private final HttpUriRequest request;
    private final ProfileEvent event;
    private final long sendTime;
    private int remainRetryTimes;

    public HttpRequest(HttpUriRequest request, ProfileEvent event, int remainRetryTimes) {
        this.request = request;
        this.event = event;
        this.sendTime = System.currentTimeMillis();
        this.remainRetryTimes = remainRetryTimes;
    }

    public HttpUriRequest getRequest() {
        return request;
    }

    public ProfileEvent getEvent() {
        return event;
    }

    public long getSendTime() {
        return sendTime;
    }

    public int getRemainRetryTimes() {
        return remainRetryTimes;
    }

    public void setRemainRetryTimes(int remainRetryTimes) {
        this.remainRetryTimes = remainRetryTimes;
    }
}
