/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.network;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.inlong.dataproxy.SendMessageCallback;

/**
 * http message for cache.
 */
public class HttpMessage {
    private final String bid;
    private final String tid;
    private final List<String> bodies;
    private final SendMessageCallback callback;
    private final long dt;
    private final long timeout;
    private final TimeUnit timeUnit;

    public HttpMessage(List<String> bodies, String bid, String tid, long dt,
                       long timeout, TimeUnit timeUnit, SendMessageCallback callback) {
        this.bid = bid;
        this.tid = tid;
        this.bodies = bodies;
        this.callback = callback;
        this.dt = dt;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    public String getBid() {
        return bid;
    }

    public String getTid() {
        return tid;
    }

    public List<String> getBodies() {
        return bodies;
    }

    public SendMessageCallback getCallback() {
        return callback;
    }

    public long getDt() {
        return dt;
    }

    public long getTimeout() {
        return timeout;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }
}
