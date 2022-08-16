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

package org.apache.inlong.sdk.sort.impl.pulsar;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.sdk.sort.api.ClientContext;
import org.apache.inlong.sdk.sort.api.InlongTopicFetcher;
import org.apache.inlong.sdk.sort.api.SortClientConfig;
import org.apache.inlong.sdk.sort.entity.CacheZoneCluster;
import org.apache.inlong.sdk.sort.entity.InlongTopic;
import org.apache.inlong.sdk.sort.impl.ClientContextImpl;
import org.apache.inlong.sdk.sort.stat.SortClientStateCounter;
import org.apache.inlong.sdk.sort.stat.StatManager;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest({ClientContext.class})
public class InlongPulsarFetcherImplTest {

    private ClientContext clientContext;
    private InlongTopic inlongTopic;
    private SortClientConfig sortClientConfig;
    private StatManager statManager;

    /**
     * setUp
     */
    @Before
    public void setUp() throws Exception {
        System.setProperty("log4j2.disable.jmx", Boolean.TRUE.toString());

        inlongTopic = new InlongTopic();
        inlongTopic.setTopic("testTopic");
        inlongTopic.setPartitionId(0);
        inlongTopic.setTopicType("pulsar");
        inlongTopic.setProperties(new HashMap<>());

        CacheZoneCluster cacheZoneCluster = new CacheZoneCluster("clusterId", "bootstraps", "token");
        inlongTopic.setInlongCluster(cacheZoneCluster);
        clientContext = PowerMockito.mock(ClientContextImpl.class);

        sortClientConfig = PowerMockito.mock(SortClientConfig.class);
        statManager = PowerMockito.mock(StatManager.class);

        when(clientContext.getConfig()).thenReturn(sortClientConfig);
        when(clientContext.getStatManager()).thenReturn(statManager);
        SortClientStateCounter sortClientStateCounter = new SortClientStateCounter("sortTaskId",
                cacheZoneCluster.getClusterId(),
                inlongTopic.getTopic(), 0);
        when(statManager.getStatistics(anyString(), anyString(), anyString())).thenReturn(sortClientStateCounter);
        when(sortClientConfig.getSortTaskId()).thenReturn("sortTaskId");

    }

    @Test
    public void stopConsume() {
        InlongTopicFetcher inlongTopicFetcher = new InlongPulsarFetcherImpl(inlongTopic, clientContext);
        boolean consumeStop = inlongTopicFetcher.isConsumeStop();
        Assert.assertFalse(consumeStop);
        inlongTopicFetcher.stopConsume(true);
        consumeStop = inlongTopicFetcher.isConsumeStop();
        Assert.assertTrue(consumeStop);
    }

    @Test
    public void getInlongTopic() {
        InlongTopicFetcher inlongTopicFetcher = new InlongPulsarFetcherImpl(inlongTopic, clientContext);
        InlongTopic inlongTopic = inlongTopicFetcher.getInlongTopic();
        Assert.assertEquals(inlongTopic.getInlongCluster(), inlongTopic.getInlongCluster());
    }

    @Test
    public void getConsumedDataSize() {
        InlongTopicFetcher inlongTopicFetcher = new InlongPulsarFetcherImpl(inlongTopic, clientContext);
        long consumedDataSize = inlongTopicFetcher.getConsumedDataSize();
        Assert.assertEquals(0L, consumedDataSize);
    }

    @Test
    public void getAckedOffset() {
        InlongTopicFetcher inlongTopicFetcher = new InlongPulsarFetcherImpl(inlongTopic, clientContext);
        long ackedOffset = inlongTopicFetcher.getAckedOffset();
        Assert.assertEquals(0L, ackedOffset);
    }

    @Test
    public void ack() {
        InlongTopicFetcher inlongTopicFetcher = new InlongPulsarFetcherImpl(inlongTopic, clientContext);
        MessageId messageId = PowerMockito.mock(MessageId.class);
        ConcurrentHashMap<String, MessageId> offsetCache = new ConcurrentHashMap<>();
        offsetCache.put("test", messageId);

        Whitebox.setInternalState(inlongTopicFetcher, "offsetCache", offsetCache);

        try {
            inlongTopicFetcher.ack("test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void init() {
        InlongTopicFetcher inlongTopicFetcher = new InlongPulsarFetcherImpl(inlongTopic, clientContext);
        PulsarClient pulsarClient = PowerMockito.mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = PowerMockito.mock(ConsumerBuilder.class);

        try {
            when(pulsarClient.newConsumer(any())).thenReturn(consumerBuilder);
            when(consumerBuilder.topic(anyString())).thenReturn(consumerBuilder);
            when(consumerBuilder.subscriptionName(anyString())).thenReturn(consumerBuilder);
            when(consumerBuilder.subscriptionType(any())).thenReturn(consumerBuilder);
            when(consumerBuilder.startMessageIdInclusive()).thenReturn(consumerBuilder);
            when(consumerBuilder.ackTimeout(anyLong(), any())).thenReturn(consumerBuilder);
            when(consumerBuilder.subscriptionInitialPosition(any())).thenReturn(consumerBuilder);

            when(consumerBuilder.receiverQueueSize(anyInt())).thenReturn(consumerBuilder);
            when(consumerBuilder.messageListener(any())).thenReturn(consumerBuilder);

            Consumer consumer = PowerMockito.mock(Consumer.class);
            when(consumerBuilder.subscribe()).thenReturn(consumer);
            doNothing().when(consumer).close();
            boolean init = inlongTopicFetcher.init(pulsarClient);
            inlongTopicFetcher.close();
            Assert.assertTrue(init);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void pause() {
        InlongTopicFetcher inlongTopicFetcher = new InlongPulsarFetcherImpl(inlongTopic, clientContext);
        inlongTopicFetcher.pause();
    }

    @Test
    public void resume() {
        InlongTopicFetcher inlongTopicFetcher = new InlongPulsarFetcherImpl(inlongTopic, clientContext);
        inlongTopicFetcher.resume();
    }

    @Test
    public void close() {
        InlongTopicFetcher inlongTopicFetcher = new InlongPulsarFetcherImpl(inlongTopic, clientContext);
        boolean close = inlongTopicFetcher.close();
        Assert.assertTrue(close);

        boolean closed = inlongTopicFetcher.isClosed();
        Assert.assertTrue(closed);
    }
}