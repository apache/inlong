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

package org.apache.inlong.audit.util;

import org.apache.inlong.audit.protocol.AuditApi;
import org.junit.Test;


public class AuditDataTest {
    @Test
    public void increaseResendTimes() {
        AuditApi.AuditRequest content = null;
        AuditData test = new AuditData(System.currentTimeMillis(), content, 1L);
        int resendTimes = test.increaseResendTimes();
        System.out.println(resendTimes);
        resendTimes = test.increaseResendTimes();
        System.out.println(resendTimes);
        resendTimes = test.increaseResendTimes();
        System.out.println(resendTimes);
    }

    @Test
    public void getDataByte() {
        AuditApi.AuditMessageHeader header = AuditApi.AuditMessageHeader.newBuilder().setIp("127.0.0.1").build();
        AuditApi.AuditMessageBody body = AuditApi.AuditMessageBody.newBuilder().setAuditId("1").build();
        AuditApi.AuditRequest content = AuditApi.AuditRequest.newBuilder().setMsgHeader(header).addMsgBody(body).build();
        AuditData test = new AuditData(System.currentTimeMillis(), content, 1L);
        byte data[] = test.getDataByte();
        System.out.println(data[0]);
        System.out.println(data);
    }

    @Test
    public void addBytes() {
    }
}