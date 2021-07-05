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

package org.apache.inlong.dataproxy.metric;

/**
 * message uuid
 */
public class MessageRecord {
    private final String msgUUID;
    private final int msgCount;
    private final long startTime;
    private final long dt;

    private final String bid;
    private final String tid;
    private final String localIp;
    private final long packTime;

    public MessageRecord(String bid, String tid, String localIp, String msgId, long dt, long packTime, int msgCount) {
        this.bid = bid;
        this.tid = tid;
        this.localIp = localIp;
        this.msgUUID = msgId;
        this.msgCount = msgCount;
        this.packTime = packTime;
        this.dt = dt;
        startTime = System.currentTimeMillis();
    }

    public String getMsgUUID() {
        return msgUUID;
    }

    public int getMsgCount() {
        return msgCount;
    }

    public long getMessageTime() {
        return System.currentTimeMillis() - startTime;
    }

    public long getPackTime() {
        return packTime;
    }

    public long getDt() {
        return dt;
    }

    public String getBid() {
        return bid;
    }

    public String getTid() {
        return tid;
    }

    public String getLocalIp() {
        return localIp;
    }
}

