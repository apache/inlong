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

package org.apache.inlong.dataproxy.source2;

import com.google.common.base.Preconditions;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.holder.ConfigUpdateCallback;
import org.apache.inlong.dataproxy.utils.AddressUtils;
import org.apache.inlong.dataproxy.utils.ConfStringUtils;
import org.apache.inlong.dataproxy.utils.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Iterator;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Simple tcp source
 */
public class SimpleTcpSource extends BaseSource implements Configurable, ConfigUpdateCallback {

    private static final Logger logger = LoggerFactory.getLogger(SimpleTcpSource.class);

    private ServerBootstrap bootstrap;
    private boolean tcpNoDelay;
    private boolean tcpKeepAlive;
    private int highWaterMark;
    private boolean enableBusyWait;

    public SimpleTcpSource() {
        super();
        ConfigManager.getInstance().regIPVisitConfigChgCallback(this);
    }

    @Override
    public void configure(Context context) {
        logger.info("Source {} context is {}", getName(), context);
        super.configure(context);
        // get tcp no-delay parameter
        this.tcpNoDelay = context.getBoolean(SourceConstants.SRCCXT_TCP_NO_DELAY,
                SourceConstants.VAL_DEF_TCP_NO_DELAY);
        // get tcp keep-alive parameter
        this.tcpKeepAlive = context.getBoolean(SourceConstants.SRCCXT_TCP_KEEP_ALIVE,
                SourceConstants.VAL_DEF_TCP_KEEP_ALIVE);
        // get tcp enable busy-wait
        this.enableBusyWait = context.getBoolean(SourceConstants.SRCCXT_TCP_ENABLE_BUSY_WAIT,
                SourceConstants.VAL_DEF_TCP_ENABLE_BUSY_WAIT);
        // get tcp high watermark
        this.highWaterMark = ConfStringUtils.getIntValue(context,
                SourceConstants.SRCCXT_TCP_HIGH_WATER_MARK, SourceConstants.VAL_DEF_TCP_HIGH_WATER_MARK);
        Preconditions.checkArgument((this.highWaterMark >= SourceConstants.VAL_MIN_TCP_HIGH_WATER_MARK),
                SourceConstants.VAL_DEF_TCP_HIGH_WATER_MARK + " must be >= "
                        + SourceConstants.VAL_MIN_TCP_HIGH_WATER_MARK);
    }

    @Override
    public synchronized void startSource() {
        logger.info("start " + this.getName());
        // build accept group
        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(maxAcceptThreads, enableBusyWait,
                new DefaultThreadFactory(this.getName() + "-boss-group"));
        // build worker group
        this.workerGroup = EventLoopUtil.newEventLoopGroup(maxWorkerThreads, enableBusyWait,
                new DefaultThreadFactory(this.getName() + "-worker-group"));
        // init boostrap
        bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, ByteBufAllocator.DEFAULT);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, tcpNoDelay);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, tcpKeepAlive);
        bootstrap.childOption(ChannelOption.SO_RCVBUF, maxRcvBufferSize);
        bootstrap.childOption(ChannelOption.SO_SNDBUF, maxSendBufferSize);
        bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, highWaterMark);
        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(bootstrap);
        bootstrap.group(acceptorGroup, workerGroup);
        bootstrap.childHandler(this.getChannelInitializerFactory());
        try {
            if (srcHost == null) {
                channelFuture = bootstrap.bind(new InetSocketAddress(srcPort)).sync();
            } else {
                channelFuture = bootstrap.bind(new InetSocketAddress(srcHost, srcPort)).sync();
            }
        } catch (Exception e) {
            logger.error("Source {} bind ({}:{}) error, program will exit! e = {}",
                    this.getName(), srcHost, srcPort, e);
            System.exit(-1);
        }
        ConfigManager.getInstance().addSourceReportInfo(
                srcHost, String.valueOf(srcPort), getProtocolName().toUpperCase());
        logger.info("Source {} started at ({}:{})!", this.getName(), srcHost, srcPort);
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }

    @Override
    public String getProtocolName() {
        return SourceConstants.SRC_PROTOCOL_TYPE_TCP;
    }

    @Override
    public void update() {
        // check current all links
        if (ConfigManager.getInstance().needChkIllegalIP()) {
            int cnt = 0;
            Channel channel;
            String strRemoteIP;
            long startTime = System.currentTimeMillis();
            Iterator<Channel> iterator = allChannels.iterator();
            while (iterator.hasNext()) {
                channel = iterator.next();
                strRemoteIP = AddressUtils.getChannelRemoteIP(channel);
                if (strRemoteIP == null) {
                    continue;
                }
                if (ConfigManager.getInstance().isIllegalIP(strRemoteIP)) {
                    channel.disconnect();
                    channel.close();
                    allChannels.remove(channel);
                    cnt++;
                    logger.error(strRemoteIP + " is Illegal IP, so disconnect it !");
                }
            }
            logger.info("Source {} channel check, disconnects {} Illegal channels, waist {} ms",
                    getName(), cnt, (System.currentTimeMillis() - startTime));
        }
    }

}
