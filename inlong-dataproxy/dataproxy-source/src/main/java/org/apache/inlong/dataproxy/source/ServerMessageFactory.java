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

package org.apache.inlong.dataproxy.source;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.source.AbstractSource;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
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
    private ExecutionHandler executionHandler;
    private String protocolType;
    private ServiceDecoder serviceProcessor;
    private String messageHandlerName;
    private int maxConnections = Integer.MAX_VALUE;
    private int maxMsgLength;
    private boolean isCompressed;
    private String name;
    private String topic;
    private String attr;
    private boolean filterEmptyMsg;
    private Timer timer = new HashedWheelTimer();

    /**
     * get server factory
     *
     * @param processor
     * @param allChannels
     * @param protocol
     * @param serProcessor
     * @param messageHandlerName
     * @param maxMsgLength
     * @param topic
     * @param attr
     * @param filterEmptyMsg
     * @param maxCons
     * @param isCompressed
     * @param name
     */
    public ServerMessageFactory(AbstractSource source,
                                ChannelGroup allChannels, String protocol, ServiceDecoder serProcessor,
                                String messageHandlerName, Integer maxMsgLength,
                                String topic, String attr, Boolean filterEmptyMsg, Integer maxCons,
                                Boolean isCompressed, String name) {
        this.source = source;
        this.processor = source.getChannelProcessor();
        this.allChannels = allChannels;
        this.topic = topic;
        this.attr = attr;
        this.filterEmptyMsg = filterEmptyMsg;
        int cores = Runtime.getRuntime().availableProcessors();
        this.protocolType = protocol;
        this.serviceProcessor = serProcessor;
        this.messageHandlerName = messageHandlerName;
        this.name = name;
        this.maxConnections = maxCons;
        this.maxMsgLength = maxMsgLength;
        this.isCompressed = isCompressed;

        if (protocolType.equalsIgnoreCase(ConfigConstants.UDP_PROTOCOL)) {
            this.executionHandler = new ExecutionHandler(
                    new OrderedMemoryAwareThreadPoolExecutor(cores * 2,
                            1024 * 1024, 1024 * 1024));
        }
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

        if (this.protocolType
                .equalsIgnoreCase(ConfigConstants.TCP_PROTOCOL)) {
            cp.addLast("messageDecoder", new LengthFieldBasedFrameDecoder(
                    this.maxMsgLength, 0, 4, 0, 0, true));
            cp.addLast("readTimeoutHandler", new ReadTimeoutHandler(timer,
                    DEFAULT_READ_IDLE_TIME, TimeUnit.MILLISECONDS));
        }

        if (processor != null) {
            try {
                Class<? extends SimpleChannelHandler> clazz = (Class<? extends SimpleChannelHandler>) Class
                        .forName(messageHandlerName);

                Constructor<?> ctor = clazz.getConstructor(
                        AbstractSource.class, ServiceDecoder.class, ChannelGroup.class,
                        String.class, String.class, Boolean.class, Integer.class,
                        Integer.class, Boolean.class, String.class);

                SimpleChannelHandler messageHandler = (SimpleChannelHandler) ctor
                        .newInstance(source, serviceProcessor, allChannels, topic, attr,
                                filterEmptyMsg, maxMsgLength, maxConnections, isCompressed, protocolType
                        );

                cp.addLast("messageHandler", messageHandler);
            } catch (Exception e) {
                LOG.info("SimpleChannelHandler.newInstance  has error:" + name, e);
            }
        }

        if (this.protocolType.equalsIgnoreCase(ConfigConstants.UDP_PROTOCOL)) {
            cp.addLast("execution", executionHandler);
        }

        return cp;
    }
}
