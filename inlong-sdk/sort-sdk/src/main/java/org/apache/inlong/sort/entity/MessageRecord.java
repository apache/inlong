/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.sort.entity;

import java.util.Map;

public class MessageRecord {

    private final String msgKey;

    private final byte[] message;
    private final Map<String, String> msgHeader;

    private final String offset;
    private final long recTime;

    public MessageRecord(String msgKey, byte[] message, Map<String, String> msgHeader, String offset, long recTime) {
        this.msgKey = msgKey;
        this.message = message;
        this.msgHeader = msgHeader;
        this.offset = offset;
        this.recTime = recTime;
    }

    public String getMsgKey() {
        return msgKey;
    }

    public byte[] getMessage() {
        return message;
    }

    public Map<String, String> getMsgHeader() {
        return msgHeader;
    }

    public String getOffset() {
        return offset;
    }

    public long getRecTime() {
        return recTime;
    }

    @Override
    public String toString() {
        return "MessageRecord{"
                + "msgKey='" + msgKey
                + ", message=" + new String(message)
                + ", msgHeader=" + msgHeader
                + ", offset='" + offset
                + ", recTime=" + recTime
                + '}';
    }
}
