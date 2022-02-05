/**
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

package org.apache.inlong.dataproxy.source.tcp;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.source.SourceContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * InlongTcpChannelPipelineFactory
 */
public class InlongTcpChannelPipelineFactory implements ChannelPipelineFactory, Configurable {

    public static final Logger LOG = LoggerFactory.getLogger(InlongTcpChannelPipelineFactory.class);
    private static final int DEFAULT_READ_IDLE_TIME = 70 * 60 * 1000;
    private SourceContext sourceContext;
    private String messageHandlerName;
    private Timer timer = new HashedWheelTimer();

    /**
     * get server factory
     *
     * @param sourceContext
     */
    public InlongTcpChannelPipelineFactory(SourceContext sourceContext) {
        this.sourceContext = sourceContext;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline cp = Channels.pipeline();
        return addMessageHandlersTo(cp);
    }

    /**
     * get message handlers
     * 
     * @param  cp
     * @return
     */
    @SuppressWarnings("unchecked")
    public ChannelPipeline addMessageHandlersTo(ChannelPipeline cp) {
        cp.addLast("messageDecoder", new LengthFieldBasedFrameDecoder(
                sourceContext.getMaxMsgLength(), 0, 4, -4, 0, true));
        cp.addLast("readTimeoutHandler", new ReadTimeoutHandler(timer,
                DEFAULT_READ_IDLE_TIME, TimeUnit.MILLISECONDS));

        if (sourceContext.getSource().getChannelProcessor() != null) {
            try {
                Class<? extends SimpleChannelHandler> clazz = (Class<? extends SimpleChannelHandler>) Class
                        .forName(messageHandlerName);

                Constructor<?> ctor = clazz.getConstructor(SourceContext.class);

                SimpleChannelHandler messageHandler = (SimpleChannelHandler) ctor
                        .newInstance(sourceContext);

                cp.addLast("messageHandler", messageHandler);
            } catch (Exception e) {
                LOG.error("SimpleChannelHandler.newInstance  has error:" + sourceContext.getSource().getName(), e);
            }
        }

        return cp;
    }

    @Override
    public void configure(Context context) {
        LOG.info("context is {}", context);
        messageHandlerName = context.getString(ConfigConstants.MESSAGE_HANDLER_NAME,
                InlongTcpChannelHandler.class.getName());
        messageHandlerName = messageHandlerName.trim();
        Preconditions.checkArgument(StringUtils.isNotBlank(messageHandlerName),
                "messageHandlerName is empty");
    }
}
