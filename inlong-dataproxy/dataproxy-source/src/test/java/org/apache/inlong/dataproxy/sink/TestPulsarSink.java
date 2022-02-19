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

package org.apache.inlong.dataproxy.sink;

import com.google.common.base.Charsets;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.holder.PropertiesConfigHolder;
import org.apache.inlong.dataproxy.config.holder.PulsarConfigHolder;
import org.apache.inlong.dataproxy.config.pojo.PulsarConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.testng.PowerMockTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import java.util.Map;

import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ConfigManager.class})
@PowerMockIgnore({"javax.net.ssl.*", "javax.management.*"})
public class TestPulsarSink extends PowerMockTestCase {
    private static final Logger logger = LoggerFactory
            .getLogger(TestPulsarSink.class);
    private static final String hostname = "127.0.0.1";
    private static final Integer port = 1234;
    private String zkStr = "127.0.0.1:2181";
    private String zkRoot = "/meta";
    private int batchSize = 1;

    private PulsarSink sink;
    private Channel channel;
    private PulsarConfig pulsarConfig = new PulsarConfig();
    private Map<String, String> url2token;

    @Mock
    private static ConfigManager configManager;

    public void setUp() {
        sink = new PulsarSink();
        channel = new MemoryChannel();
        url2token = ConfigManager.getInstance().getPulsarUrl2Token();
        pulsarConfig.setUrl2token(url2token);

        configManager = PowerMockito.mock(ConfigManager.class);
        PowerMockito.mockStatic(ConfigManager.class);
        PowerMockito.when(ConfigManager.getInstance()).thenReturn(configManager);
        when(configManager.getPulsarConfig()).thenReturn(pulsarConfig);
        when(configManager.getTopicConfig()).thenReturn(new PropertiesConfigHolder("topics.properties"));
        when(configManager.getPulsarCluster()).thenReturn(new PulsarConfigHolder("pulsar_config.properties"));

        Context context = new Context();
        context.put("type", "org.apache.inlong.dataproxy.sink.PulsarSink");
        sink.setChannel(channel);

        Configurables.configure(sink, context);
        Configurables.configure(channel, context);
    }

    @Test
    public void testProcess() throws InterruptedException, EventDeliveryException,
            InstantiationException, IllegalAccessException {
        setUp();
        Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);
        sink.start();
        Assert.assertTrue(LifecycleController.waitForOneOf(sink,
                LifecycleState.START_OR_ERROR, 5000));

        Transaction transaction = channel.getTransaction();

        transaction.begin();
        for (int i = 0; i < 10; i++) {
            channel.put(event);
        }
        transaction.commit();
        transaction.close();

        for (int i = 0; i < 5; i++) {
            Sink.Status status = sink.process();
            Assert.assertEquals(Sink.Status.READY, status);
        }

        sink.stop();
        Assert.assertTrue(LifecycleController.waitForOneOf(sink,
                LifecycleState.STOP_OR_ERROR, 5000));
    }
}
