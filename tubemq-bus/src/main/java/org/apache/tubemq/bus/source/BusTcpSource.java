/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.bus.source;

import static org.apache.tubemq.bus.source.SourceConfigConstants.CONFIG_HOST;
import static org.apache.tubemq.bus.source.SourceConfigConstants.CONFIG_PORT;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_BOSS_NUM;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_CONFIG_HOST;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_HIGH_WATER_MARK;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_KEEP_ALIVE;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_MAX_THREADS;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_MESSAGE_HANDLER_NAME;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_MSG_FACTORY_NAME;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_RECEIVE_BUFFER_SIZE;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_SEND_BUFFER_SIZE;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_SERVICE_PROCESSOR_NAME;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_TCP_NO_DELAY;
import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_TRAFFIC_CLASS;
import static org.apache.tubemq.bus.source.SourceConfigConstants.HIGH_WATER_MARK;
import static org.apache.tubemq.bus.source.SourceConfigConstants.KEEP_ALIVE;
import static org.apache.tubemq.bus.source.SourceConfigConstants.MAX_THREADS;
import static org.apache.tubemq.bus.source.SourceConfigConstants.MESSAGE_HANDLER_NAME;
import static org.apache.tubemq.bus.source.SourceConfigConstants.MSG_FACTORY_NAME;
import static org.apache.tubemq.bus.source.SourceConfigConstants.RECEIVE_BUFFER_SIZE;
import static org.apache.tubemq.bus.source.SourceConfigConstants.SEND_BUFFER_SIZE;
import static org.apache.tubemq.bus.source.SourceConfigConstants.SERVICE_PROCESSOR_NAME;
import static org.apache.tubemq.bus.source.SourceConfigConstants.TCP_NO_DELAY;
import static org.apache.tubemq.bus.source.SourceConfigConstants.TRAFFIC_CLASS;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.SslContextAwareAbstractSource;
import org.apache.tubemq.bus.BusThreadFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bus source based on tcp source.
 */

public class BusTcpSource extends SslContextAwareAbstractSource implements EventDrivenSource,
    Configurable {

    private static final Logger logger = LoggerFactory.getLogger(BusTcpSource.class);

    private static final String BUS_BOSS_THREAD_NAME = "tcpSource-nettyBoss-threadGroup";
    private static final String BUS_WORKER_THREAD_NAME = "tcpSource-nettyWorker-threadGroup";

    private int maxThreads;
    private int port;
    private String host;
    private String msgFactoryName;
    private boolean tcpNoDelay = true;
    private boolean keepAlive = true;
    private int receiveBufferSize;
    private int highWaterMark;
    private int sendBufferSize;
    private int trafficClass;

    private String serviceDecoderName;
    private String messageHandlerName;
    private Channel nettyChannel = null;
    private ChannelGroup allChannels = new DefaultChannelGroup();
    private ServerBootstrap bootstrap = null;

    @Override
    public void configure(Context context) {
        Configurables.ensureRequiredNonNull(context, CONFIG_PORT);
        port = context.getInteger(CONFIG_PORT);
        host = context.getString(CONFIG_HOST, DEFAULT_CONFIG_HOST);
        msgFactoryName = context.getString(MSG_FACTORY_NAME, DEFAULT_MSG_FACTORY_NAME);
        maxThreads = context.getInteger(MAX_THREADS, DEFAULT_MAX_THREADS);
        tcpNoDelay = context.getBoolean(TCP_NO_DELAY, DEFAULT_TCP_NO_DELAY);

        keepAlive = context.getBoolean(KEEP_ALIVE, DEFAULT_KEEP_ALIVE);
        highWaterMark = context.getInteger(HIGH_WATER_MARK, DEFAULT_HIGH_WATER_MARK);
        receiveBufferSize = context.getInteger(RECEIVE_BUFFER_SIZE, DEFAULT_RECEIVE_BUFFER_SIZE);
        sendBufferSize = context.getInteger(SEND_BUFFER_SIZE, DEFAULT_SEND_BUFFER_SIZE);
        trafficClass = context.getInteger(TRAFFIC_CLASS, DEFAULT_TRAFFIC_CLASS);

        serviceDecoderName = context.getString(SERVICE_PROCESSOR_NAME, DEFAULT_SERVICE_PROCESSOR_NAME);
        messageHandlerName = context.getString(MESSAGE_HANDLER_NAME, DEFAULT_MESSAGE_HANDLER_NAME);


        configureSsl(context);
    }

    @Override
    public void start() {
        ChannelFactory factory =
            new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(new BusThreadFactory(BUS_BOSS_THREAD_NAME)), DEFAULT_BOSS_NUM,
                Executors.newCachedThreadPool(new BusThreadFactory(BUS_WORKER_THREAD_NAME)), maxThreads);
        bootstrap = new ServerBootstrap(factory);

        bootstrap.setOption("child.tcpNoDelay", tcpNoDelay);
        bootstrap.setOption("child.keepAlive", keepAlive);
        bootstrap.setOption("child.receiveBufferSize", receiveBufferSize);
        bootstrap.setOption("child.sendBufferSize", sendBufferSize);
        bootstrap.setOption("child.trafficClass", trafficClass);
        bootstrap.setOption("child.writeBufferHighWaterMark", highWaterMark);
        ChannelPipelineFactory fac = null;
        try {
            ServiceDecoder serviceDecoder = new DefaultServiceDecoder();
            fac = new ServerMessageFactory().setDecoder(serviceDecoder);
        } catch (Exception ex) {
            logger.error("Simple Tcp Source start error, fail to construct "
                + "ChannelPipelineFactory with name {}, ex {}", msgFactoryName, ex);
            stop();
            throw new FlumeException(ex.getMessage());
        }
        bootstrap.setPipelineFactory(fac);
        nettyChannel = bootstrap.bind(new InetSocketAddress(host, port));
        super.start();
    }

    @Override
    public void stop() {
        if (allChannels != null && !allChannels.isEmpty()) {
            try {
                allChannels.unbind().awaitUninterruptibly();
                allChannels.close().awaitUninterruptibly();
            } catch (Exception e) {
                logger.warn("Simple TCP Source netty server stop ex", e);
            } finally {
                allChannels.clear();
            }
        }

        if (bootstrap != null) {
            try {
                bootstrap.releaseExternalResources();
            } catch (Exception e) {
                logger.warn("Simple TCP Source serverBootstrap stop ex", e);
            } finally {
                bootstrap = null;
            }
        }

        super.stop();
    }

    @Override
    public String toString() {
        return "Bus source " + getName() + ": { bindAddress: " + host +
            ", port: " + port + " }";
    }
}
