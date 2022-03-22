package org.apache.inlong.manager.client.api.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

@Slf4j
public class InnerInlongManagerClientTest {

    @Test
    public void testListStreamInfo() {
        String serviceUrl = "127.0.0.1:8083";
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setAuthentication(new DefaultAuthentication("admin", "inlong"));
        InlongClientImpl inlongClient = new InlongClientImpl(serviceUrl, configuration);
        InnerInlongManagerClient innerInlongManagerClient = new InnerInlongManagerClient(inlongClient);
        List<FullStreamResponse> fullStreamResponseList = null;
        try {
            innerInlongManagerClient.listStreamInfo("test");
        } catch (RuntimeException e) {
            log.error("exception:", e);
            Assert.assertNull(fullStreamResponseList);
        }
    }

}
