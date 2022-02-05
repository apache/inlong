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
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.inlong.dataproxy.base.NamedThreadFactory;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.source.SourceContext;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Inlong tcp source
 */
public class InlongTcpSource extends AbstractSource implements Configurable, EventDrivenSource {

    public static final Logger LOG = LoggerFactory.getLogger(InlongTcpSource.class);

    protected ChannelGroup allChannels;
    protected SourceContext sourceContext;

    protected ServerBootstrap serverBootstrap = null;
    protected int port;
    protected String host = null;
    protected String msgFactoryName;
    protected String messageHandlerName;

    private boolean tcpNoDelay = true;
    private boolean keepAlive = true;
    private int receiveBufferSize;
    private int highWaterMark;
    private int sendBufferSize;
    private int trafficClass;

    private Channel nettyChannel = null;

    private Configurable pipelineFactoryConfigurable = null;

    /**
     * Constructor
     */
    public InlongTcpSource() {
        super();
        allChannels = new DefaultChannelGroup();
    }

    /**
     * start
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public synchronized void start() {
        try {
            LOG.info("start " + this.getName());

            ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);
            ChannelFactory factory = new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(
                            new NamedThreadFactory("tcpSource-nettyBoss-threadGroup")),
                    1,
                    Executors.newCachedThreadPool(
                            new NamedThreadFactory("tcpSource-nettyWorker-threadGroup")),
                    this.sourceContext.getMaxThreads());
            LOG.info("Set max workers : {} ;", this.sourceContext.getMaxThreads());
            ChannelPipelineFactory pipelineFactory = null;

            serverBootstrap = new ServerBootstrap(factory);
            serverBootstrap.setOption("child.tcpNoDelay", tcpNoDelay);
            serverBootstrap.setOption("child.keepAlive", keepAlive);
            serverBootstrap.setOption("child.receiveBufferSize", receiveBufferSize);
            serverBootstrap.setOption("child.sendBufferSize", sendBufferSize);
            serverBootstrap.setOption("child.trafficClass", trafficClass);
            serverBootstrap.setOption("child.writeBufferHighWaterMark", highWaterMark);
            LOG.info("load msgFactory=" + msgFactoryName
                    + " and messageHandlerName=" + messageHandlerName);
            try {
                Class<? extends ChannelPipelineFactory> clazz = (Class<? extends ChannelPipelineFactory>) Class
                        .forName(msgFactoryName);

                Constructor ctor = clazz.getConstructor(SourceContext.class);

                LOG.info("Using channel processor:{}", getChannelProcessor().getClass().getName());
                pipelineFactory = (ChannelPipelineFactory) ctor.newInstance(sourceContext);
                if (pipelineFactory instanceof Configurable) {
                    this.pipelineFactoryConfigurable = ((Configurable) pipelineFactory);
                    this.pipelineFactoryConfigurable.configure(sourceContext.getParentContext());
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error(
                        "Inlong Tcp Source start error, fail to construct ChannelPipelineFactory with name {}, ex {}",
                        msgFactoryName, e);
                stop();
                throw new FlumeException(e.getMessage(), e);
            }

            serverBootstrap.setPipelineFactory(pipelineFactory);

            try {
                if (host == null) {
                    nettyChannel = serverBootstrap.bind(new InetSocketAddress(port));
                } else {
                    nettyChannel = serverBootstrap.bind(new InetSocketAddress(host, port));
                }
            } catch (Exception e) {
                LOG.error("Inlong TCP Source error bind host {} port {},program will exit!", host,
                        port);
                System.exit(-1);
            }

            allChannels.add(nettyChannel);

            LOG.info("Inlong TCP Source started at host {}, port {}", host, port);
            super.start();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    /**
     * stop
     */
    @Override
    public synchronized void stop() {
        LOG.info("[STOP SOURCE]{} stopping...", super.toString());
        if (allChannels != null && !allChannels.isEmpty()) {
            try {
                allChannels.unbind().awaitUninterruptibly();
                allChannels.close().awaitUninterruptibly();
            } catch (Exception e) {
                LOG.warn("Inlong TCP Source netty server stop ex", e);
            } finally {
                allChannels.clear();
            }
        }

        if (serverBootstrap != null) {
            try {
                serverBootstrap.releaseExternalResources();
            } catch (Exception e) {
                LOG.warn("Inlong TCP Source serverBootstrap stop ex ", e);
            } finally {
                serverBootstrap = null;
            }
        }

//        super.stop();
        LOG.info("[STOP SOURCE]{} stopped", super.getName());
    }

    /**
     * configure
     * 
     * @param context
     */
    @Override
    public void configure(Context context) {
        try {
            LOG.info("context is {}", context);
            this.sourceContext = new SourceContext(this, allChannels, context);
            // start
            this.sourceContext.start();

            tcpNoDelay = context.getBoolean(ConfigConstants.TCP_NO_DELAY, true);
            keepAlive = context.getBoolean(ConfigConstants.KEEP_ALIVE, true);
            highWaterMark = context.getInteger(ConfigConstants.HIGH_WATER_MARK, 64 * 1024);
            receiveBufferSize = context.getInteger(ConfigConstants.RECEIVE_BUFFER_SIZE, 1024 * 64);
            if (receiveBufferSize > 16 * 1024 * 1024) {
                receiveBufferSize = 16 * 1024 * 1024;
            }
            Preconditions.checkArgument(receiveBufferSize > 0, "receiveBufferSize must be > 0");

            sendBufferSize = context.getInteger(ConfigConstants.SEND_BUFFER_SIZE, 1024 * 64);
            if (sendBufferSize > 16 * 1024 * 1024) {
                sendBufferSize = 16 * 1024 * 1024;
            }
            Preconditions.checkArgument(sendBufferSize > 0, "sendBufferSize must be > 0");

            trafficClass = context.getInteger(ConfigConstants.TRAFFIC_CLASS, 0);
            Preconditions.checkArgument((trafficClass == 0 || trafficClass == 96),
                    "trafficClass must be == 0 or == 96");

            msgFactoryName = context.getString(ConfigConstants.MSG_FACTORY_NAME, InlongTcpChannelPipelineFactory.class.getName());
            msgFactoryName = msgFactoryName.trim();
            Preconditions.checkArgument(StringUtils.isNotBlank(msgFactoryName),
                    "msgFactoryName is empty");

            messageHandlerName = context.getString(ConfigConstants.MESSAGE_HANDLER_NAME,
                    InlongTcpChannelHandler.class.getName());
            messageHandlerName = messageHandlerName.trim();
            Preconditions.checkArgument(StringUtils.isNotBlank(messageHandlerName),
                    "messageHandlerName is empty");

            if (this.pipelineFactoryConfigurable != null) {
                this.pipelineFactoryConfigurable.configure(context);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
