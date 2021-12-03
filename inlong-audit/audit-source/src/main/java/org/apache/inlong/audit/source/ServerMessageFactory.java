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

package org.apache.inlong.audit.source;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.source.AbstractSource;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerMessageFactory implements ChannelPipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ServerMessageFactory.class);
    private static final int DEFAULT_READ_IDLE_TIME = 70 * 60 * 1000;
    private AbstractSource source;
    private ChannelProcessor processor;
    private ChannelGroup allChannels;
    private ServiceDecoder serviceDecoder;
    private String messageHandlerName;
    private int maxConnections = Integer.MAX_VALUE;
    private int maxMsgLength;
    private String name;
    private Timer timer = new HashedWheelTimer();

    /**
     * get server factory
     *
     * @param source
     * @param allChannels
     * @param serviceDecoder
     * @param messageHandlerName
     * @param maxMsgLength
     * @param maxCons
     * @param name
     */
    public ServerMessageFactory(AbstractSource source,
            ChannelGroup allChannels, ServiceDecoder serviceDecoder,
            String messageHandlerName, Integer maxMsgLength, Integer maxCons, String name) {
        this.source = source;
        this.processor = source.getChannelProcessor();
        this.allChannels = allChannels;
        this.serviceDecoder = serviceDecoder;
        this.messageHandlerName = messageHandlerName;
        this.name = name;
        this.maxConnections = maxCons;
        this.maxMsgLength = maxMsgLength;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline cp = Channels.pipeline();
        return addMessageHandlersTo(cp);
    }

    /**
     * get message handlers
     * @param cp
     * @return
     */
    public ChannelPipeline addMessageHandlersTo(ChannelPipeline cp) {

        cp.addLast("messageDecoder", new LengthFieldBasedFrameDecoder(
                this.maxMsgLength, 0, 4, 0, 0, true));
        cp.addLast("readTimeoutHandler", new ReadTimeoutHandler(timer,
                DEFAULT_READ_IDLE_TIME, TimeUnit.MILLISECONDS));

        if (processor != null) {
            try {
                Class<? extends SimpleChannelHandler> clazz = (Class<? extends SimpleChannelHandler>) Class
                        .forName(messageHandlerName);

                Constructor<?> ctor = clazz.getConstructor(
                        AbstractSource.class, ServiceDecoder.class, ChannelGroup.class, Integer.class);

                SimpleChannelHandler messageHandler = (SimpleChannelHandler) ctor
                        .newInstance(source, serviceDecoder, allChannels, maxConnections);

                cp.addLast("messageHandler", messageHandler);
            } catch (Exception e) {
                LOG.info("SimpleChannelHandler.newInstance  has error:" + name, e);
            }
        }
        return cp;
    }
}
