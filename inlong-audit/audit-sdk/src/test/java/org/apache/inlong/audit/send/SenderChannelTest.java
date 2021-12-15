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

import org.apache.inlong.audit.util.Encoder;
import org.apache.inlong.audit.util.IpPort;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SenderChannelTest {
    private static final Logger logger = LoggerFactory.getLogger(SenderChannelTest.class);
    private ClientBootstrap client = new ClientBootstrap();
    private IpPort ipPortObj = new IpPort("0.0.0.0", 54041);
    private ChannelFuture future;
    SenderChannel senderChannel;

    /**
     * test  SenderChannel
     */
    public SenderChannelTest() {
        try {
            client.setFactory(new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool(),
                    10));

            client.setPipelineFactory(() -> {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("encoder", new Encoder());
                return pipeline;
            });
            client.setOption("tcpNoDelay", true);
            client.setOption("child.tcpNoDelay", true);
            client.setOption("keepAlive", true);
            client.setOption("child.keepAlive", true);
            client.setOption("reuseAddr", true);

            future = client.connect(ipPortObj.addr).await();
            senderChannel = new SenderChannel(future.getChannel(), ipPortObj, 10);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

    @Test
    public void tryAcquire() {
        boolean ret = senderChannel.tryAcquire();
        assertTrue(ret);
    }

    @Test
    public void release() {
        senderChannel.release();
    }

    @Test
    public void testToString() {
        IpPort ipPort = senderChannel.getIpPort();
        assertEquals(ipPort, ipPortObj);
    }

    @Test
    public void getIpPort() {
        String toString = senderChannel.toString();
        assertEquals(toString, "0.0.0.0:54041");
    }
}