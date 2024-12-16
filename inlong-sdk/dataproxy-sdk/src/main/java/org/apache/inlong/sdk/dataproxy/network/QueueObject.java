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

package org.apache.inlong.sdk.dataproxy.network;

import org.apache.inlong.sdk.dataproxy.common.SendMessageCallback;

import java.util.concurrent.atomic.AtomicBoolean;

public class QueueObject {

    private final NettyClient client;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final long sendTimeInMillis;
    private final SendMessageCallback callback;
    private final long timeoutInMillis;
    private final int size;

    public QueueObject(NettyClient client, long sendTimeInMillis,
            SendMessageCallback callback, int size, long timeoutMs) {
        this.client = client;
        this.sendTimeInMillis = sendTimeInMillis;
        this.callback = callback;
        this.timeoutInMillis = timeoutMs;
        this.size = size;
    }

    public void done() {
        if (client != null && done.compareAndSet(false, true)) {
            client.decMsgInFlight();
        }
    }

    public long getSendTimeInMillis() {
        return sendTimeInMillis;
    }

    public SendMessageCallback getCallback() {
        return callback;
    }

    public long getTimeoutInMillis() {
        return timeoutInMillis;
    }

    public int getSize() {
        return size;
    }
}
