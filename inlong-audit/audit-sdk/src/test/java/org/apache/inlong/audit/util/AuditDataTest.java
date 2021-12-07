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