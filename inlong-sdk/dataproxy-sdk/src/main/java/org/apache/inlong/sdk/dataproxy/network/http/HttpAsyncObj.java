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

package org.apache.inlong.sdk.dataproxy.network.http;

import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpEventInfo;

/**
 * HTTP Asynchronously Object class
 *
 * Used to carry the reported message content
 */
public class HttpAsyncObj {

    private final HttpEventInfo httpEvent;
    private final MsgSendCallback callback;
    private final long rptMs;

    public HttpAsyncObj(HttpEventInfo httpEvent, MsgSendCallback callback) {
        this.httpEvent = httpEvent;
        this.callback = callback;
        this.rptMs = System.currentTimeMillis();
    }

    public HttpEventInfo getHttpEvent() {
        return httpEvent;
    }

    public MsgSendCallback getCallback() {
        return callback;
    }

    public long getRptMs() {
        return rptMs;
    }
}
