/**
 * Tencent is pleased to support the open source community by making Tars available.
 * <p>
 * Copyright (C) 2015,2016 THL A29 Limited, a Tencent company. All rights reserved.
 * <p>
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * https://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.inlong.audit.util;

import org.apache.inlong.audit.protocol.AuditApi;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class AuditData {
    public static int HEAD_LENGTH = 4;
    private final long sdkTime;
    private final AuditApi.AuditRequest content;
    private final Long requestId;
    private AtomicInteger resendTimes = new AtomicInteger(0);

    /**
     * Constructor
     *
     * @param sdkTime
     * @param content
     * @param requestId
     */
    public AuditData(long sdkTime, AuditApi.AuditRequest content, Long requestId) {
        this.sdkTime = sdkTime;
        this.content = content;
        this.requestId = requestId;
    }

    /**
     * set resendTimes
     */
    public int increaseResendTimes() {
        return this.resendTimes.incrementAndGet();
    }

    public byte[] getDataByte() {
        return addBytes(ByteBuffer.allocate(HEAD_LENGTH).putInt(content.toByteArray().length).array(),
                content.toByteArray());
    }

    /**
     * Concatenated byte array
     * @param data1
     * @param data2
     * @return data1 and  data2 combined package result
     */
    public byte[] addBytes(byte[] data1, byte[] data2) {
        byte[] data3 = new byte[data1.length + data2.length];
        System.arraycopy(data1, 0, data3, 0, data1.length);
        System.arraycopy(data2, 0, data3, data1.length, data2.length);
        return data3;
    }
}
