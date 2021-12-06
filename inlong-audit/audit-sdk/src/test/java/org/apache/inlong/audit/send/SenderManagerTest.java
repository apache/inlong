package org.apache.inlong.audit.send;

import org.apache.inlong.audit.protocol.AuditApi;
import org.apache.inlong.audit.util.Config;
import org.junit.jupiter.api.Test;

class SenderManagerTest {
    private Config testConfig = new Config();

    @Test
    void reload() {
        testConfig.init();
        SenderManager testManager = new SenderManager(testConfig);
        testManager.reload("testFile");
    }

    @Test
    void nextRequestId() {
        SenderManager testManager = new SenderManager(testConfig);
        Long RequestId = testManager.nextRequestId();
        System.out.println(RequestId);

        RequestId = testManager.nextRequestId();
        System.out.println(RequestId);

        RequestId = testManager.nextRequestId();
        System.out.println(RequestId);
    }

    @Test
    void send() {
        AuditApi.AuditMessageHeader header=AuditApi.AuditMessageHeader.newBuilder().setIp("127.0.0.1").build();
        AuditApi.AuditMessageBody body=AuditApi.AuditMessageBody.newBuilder().setAuditId("1").build();
        AuditApi.AuditRequest content=AuditApi.AuditRequest.newBuilder().setMsgHeader(header).addMsgBody(body).build();
        SenderManager testManager = new SenderManager(testConfig);
        testManager.send(System.currentTimeMillis(),content);
    }

    @Test
    void clearBuffer() {
        SenderManager testManager = new SenderManager(testConfig);
        testManager.clearBuffer();
        int dataMapSize=testManager.getDataMapSize();
        System.out.println(dataMapSize);
    }
}