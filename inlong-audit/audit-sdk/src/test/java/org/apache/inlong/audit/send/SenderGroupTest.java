package org.apache.inlong.audit.send;

import org.apache.inlong.audit.protocol.AuditApi;
import org.apache.inlong.audit.util.AuditData;
import org.apache.inlong.audit.util.Config;
import org.apache.inlong.audit.util.Decoder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

class SenderGroupTest {
    Config testConfig = new Config();
    SenderManager testManager = new SenderManager(testConfig);
    SenderHandler clientHandler = new org.apache.inlong.audit.send.SenderHandler(testManager);
    SenderGroup sender = new org.apache.inlong.audit.send.SenderGroup(10, new Decoder(), clientHandler);

    @Test
    void send() {
        AuditApi.AuditMessageHeader header = AuditApi.AuditMessageHeader.newBuilder().setIp("127.0.0.1").build();
        AuditApi.AuditMessageBody body = AuditApi.AuditMessageBody.newBuilder().setAuditId("1").build();
        AuditApi.AuditRequest content = AuditApi.AuditRequest.newBuilder().setMsgHeader(header).addMsgBody(body).build();
        AuditData testData = new AuditData(System.currentTimeMillis(), content, 1L);
        ChannelBuffer dataBuf = ChannelBuffers.wrappedBuffer(testData.getDataByte());
        sender.send(dataBuf);
    }

    @Test
    void release() {
        sender.release("127.0.9.1:80");
    }

    @Test
    void updateConfig() {
        Set<String> ipLists = new LinkedHashSet<>();
        ipLists.add("127.0.9.1:80");
        ipLists.add("127.0.9.1:81");
        ipLists.add("127.0.9.1:82");
        sender.updateConfig(ipLists);
    }

    @Test
    void isHasSendError() {
        sender.setHasSendError(false);
        boolean isError = sender.isHasSendError();
        System.out.println(isError);

        sender.setHasSendError(true);
        isError = sender.isHasSendError();
        System.out.println(isError);
    }

    @Test
    void setHasSendError() {
        sender.setHasSendError(false);
        boolean isError = sender.isHasSendError();
        System.out.println(isError);

        sender.setHasSendError(true);
        isError = sender.isHasSendError();
        System.out.println(isError);
    }
}