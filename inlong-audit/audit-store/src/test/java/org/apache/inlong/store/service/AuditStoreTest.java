/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.store.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.shade.io.netty.channel.EventLoop;
import org.apache.pulsar.shade.io.netty.util.Timer;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AuditStoreTest {

    private ConsumerConfigurationData consumerConf;

    private AuditMsgConsumerServer auditMsgConsumerServer;

    private PulsarClientImpl client;

    static <T> PulsarClientImpl createPulsarClientMock() {
        PulsarClientImpl clientMock = mock(PulsarClientImpl.class, Mockito.RETURNS_DEEP_STUBS);

        ClientConfigurationData clientConf = new ClientConfigurationData();
        when(clientMock.getConfiguration()).thenReturn(clientConf);
        when(clientMock.timer()).thenReturn(mock(Timer.class));

        when(clientMock.externalExecutorProvider()).thenReturn(mock(ExecutorProvider.class));
        when(clientMock.eventLoopGroup().next()).thenReturn(mock(EventLoop.class));

        return clientMock;
    }

    @BeforeMethod
    public void setUp() {
        consumerConf = new ConsumerConfigurationData<>();
        PulsarClientImpl client = createPulsarClientMock();
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        CompletableFuture<Consumer<ConsumerImpl>> subscribeFuture = new CompletableFuture<>();
        consumerConf.setSubscriptionName("test-sub");
        auditMsgConsumerServer = new AuditMsgConsumerServer();
    }

    @Test
    public void testConsumer() {
        String topic = "non-persistent://public/default/audit-test";
        auditMsgConsumerServer.createConsumer(client, topic);
    }
}
