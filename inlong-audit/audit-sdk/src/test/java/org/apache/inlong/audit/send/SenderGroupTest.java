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

package org.apache.inlong.audit.send;

import org.apache.inlong.audit.protocol.AuditApi;
import org.apache.inlong.audit.util.AuditData;
import org.apache.inlong.audit.util.Config;
import org.apache.inlong.audit.util.Decoder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

import java.util.LinkedHashSet;
import java.util.Set;

public class SenderGroupTest {
    Config testConfig = new Config();
    SenderManager testManager = new SenderManager(testConfig);
    SenderHandler clientHandler = new org.apache.inlong.audit.send.SenderHandler(testManager);
    SenderGroup sender = new org.apache.inlong.audit.send.SenderGroup(10, new Decoder(), clientHandler);

    @Test
    public void send() {
        AuditApi.AuditMessageHeader header = AuditApi.AuditMessageHeader.newBuilder().setIp("127.0.0.1").build();
        AuditApi.AuditMessageBody body = AuditApi.AuditMessageBody.newBuilder().setAuditId("1").build();
        AuditApi.AuditRequest content = AuditApi.AuditRequest.newBuilder().setMsgHeader(header)
                                        .addMsgBody(body).build();
        AuditData testData = new AuditData(System.currentTimeMillis(), content, 1L);
        ChannelBuffer dataBuf = ChannelBuffers.wrappedBuffer(testData.getDataByte());
        sender.send(dataBuf);
    }

    @Test
    public void release() {
        sender.release("127.0.9.1:80");
    }

    @Test
    public void updateConfig() {
        Set<String> ipLists = new LinkedHashSet<>();
        ipLists.add("127.0.9.1:80");
        ipLists.add("127.0.9.1:81");
        ipLists.add("127.0.9.1:82");
        sender.updateConfig(ipLists);
    }

    @Test
    public void isHasSendError() {
        sender.setHasSendError(false);
        boolean isError = sender.isHasSendError();
        System.out.println(isError);

        sender.setHasSendError(true);
        isError = sender.isHasSendError();
        System.out.println(isError);
    }

    @Test
    public void setHasSendError() {
        sender.setHasSendError(false);
        boolean isError = sender.isHasSendError();
        System.out.println(isError);

        sender.setHasSendError(true);
        isError = sender.isHasSendError();
        System.out.println(isError);
    }
}