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
import org.apache.inlong.audit.util.SenderResult;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class SenderGroup {
    private static final Logger logger = LoggerFactory.getLogger(SenderGroup.class);
    // maximum number of sending
    public static final int MAX_SEND_TIMES = 3;
    public static final int DEFAULT_WAIT_TIMES = 10000;
    public static final int WAIT_INTERVAL = 1;
    public static final int DEFAULT_SYNCH_REQUESTS = 1;

    private ClientBootstrap client = new ClientBootstrap();
    private List<LinkedBlockingQueue<SenderChannel>> channelGroups = new ArrayList<>();
    private int mIndex = 0;
    private List<SenderChannel> deleteChannels = new ArrayList<>();
    private ConcurrentHashMap<String, SenderChannel> totalChannels = new ConcurrentHashMap<>();

    private int senderThreadNum;
    private int waitChannelTimes = DEFAULT_WAIT_TIMES;
    private int waitChannelIntervalMs = WAIT_INTERVAL;
    private int maxSynchRequest = DEFAULT_SYNCH_REQUESTS;
    private boolean hasSendError = false;

    /**
     * constructor
     *
     * @param senderThreadNum
     * @param decoder
     * @param clientHandler
     */
    public SenderGroup(int senderThreadNum, ChannelUpstreamHandler decoder,
                       SimpleChannelHandler clientHandler) {
        this.senderThreadNum = senderThreadNum;

        client.setFactory(new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(),
                this.senderThreadNum));

        client.setPipelineFactory(() -> {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("decoder", decoder);
            pipeline.addLast("encoder", new Encoder());
            pipeline.addLast("handler", clientHandler);
            return pipeline;
        });
        client.setOption("tcpNoDelay", true);
        client.setOption("child.tcpNoDelay", true);
        client.setOption("keepAlive", true);
        client.setOption("child.keepAlive", true);
        client.setOption("reuseAddr", true);

        channelGroups.add(new LinkedBlockingQueue<>());
        channelGroups.add(new LinkedBlockingQueue<>());
    }

    /**
     * send data
     *
     * @param dataBuf
     * @return
     */
    public SenderResult send(ChannelBuffer dataBuf) {
        LinkedBlockingQueue<SenderChannel> channels = channelGroups.get(mIndex);
        SenderChannel channel = null;
        try {
            if (channels.size() <= 0) {
                logger.error("channels is empty");
                return new SenderResult("channels is empty", 0, false);
            }
            boolean isOk = false;
            for (int tryIndex = 0; tryIndex < waitChannelTimes; tryIndex++) {
                channels = channelGroups.get(mIndex);
                for (int i = 0; i < channels.size(); i++) {
                    channel = channels.poll();
                    boolean ret = channel.tryAcquire();
                    if (channel.tryAcquire()) {
                        isOk = true;
                        break;
                    }

                    if (ret) {
                        isOk = true;
                        break;
                    }
                    channels.offer(channel);
                    channel = null;
                }
                if (isOk) {
                    break;
                }
                try {
                    Thread.sleep(waitChannelIntervalMs);
                } catch (Throwable e) {
                    System.out.println(e.getMessage());
                }
            }
            if (channel == null) {
                logger.error("can not get a channel");
                return new SenderResult("can not get a channel", 0, false);
            }
            ChannelFuture t = null;
            if (channel.getChannel().isConnected()) {
                t = channel.getChannel().write(dataBuf).sync().await();
                if (!t.isSuccess()) {
                    if (!channel.getChannel().isConnected()) {
                        reconnect(channel);
                    }
                    t = channel.getChannel().write(dataBuf).sync().await();
                }
            } else {
                reconnect(channel);
                t = channel.getChannel().write(dataBuf).sync().await();
            }
            return new SenderResult(channel.getIpPort().ip, channel.getIpPort().port, t.isSuccess());
        } catch (Throwable ex) {
            logger.error(ex.getMessage());
            this.setHasSendError(true);
            return new SenderResult(ex.getMessage(), 0, false);
        } finally {
            if (channel != null) {
                channel.release();
                channels.offer(channel);
            }
        }
    }

    /**
     * release channel
     */
    public void release(String ipPort) {
        SenderChannel channel = this.totalChannels.get(ipPort);
        if (channel != null) {
            channel.release();
        }
    }

    /**
     * release channel
     */
    public void release(InetSocketAddress addr) {
        String destIp = addr.getHostName();
        int destPort = addr.getPort();
        String ipPort = IpPort.getIpPortKey(destIp, destPort);
        SenderChannel channel = this.totalChannels.get(ipPort);
        if (channel != null) {
            channel.release();
        }
    }

    /**
     * update config
     *
     * @param ipLists
     */
    public void updateConfig(Set<String> ipLists) {
        try {
            for (SenderChannel dc : deleteChannels) {
                dc.getChannel().disconnect();
                dc.getChannel().close();
            }
            deleteChannels.clear();
            int newIndex = mIndex ^ 0x01;
            LinkedBlockingQueue<SenderChannel> newChannels = this.channelGroups.get(newIndex);
            newChannels.clear();
            for (String ipPort : ipLists) {
                SenderChannel channel = totalChannels.get(ipPort);
                if (channel != null) {
                    newChannels.add(channel);
                    continue;
                }
                try {
                    IpPort ipPortObj = IpPort.parseIpPort(ipPort);
                    if (ipPortObj == null) {
                        continue;
                    }
                    ChannelFuture future = client.connect(ipPortObj.addr).await();
                    channel = new SenderChannel(future.getChannel(), ipPortObj, maxSynchRequest);
                    newChannels.add(channel);
                    totalChannels.put(ipPort, channel);
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }

            for (Entry<String, SenderChannel> entry : totalChannels.entrySet()) {
                if (!ipLists.contains(entry.getKey())) {
                    deleteChannels.add(entry.getValue());
                }
            }
            for (SenderChannel dc : deleteChannels) {
                totalChannels.remove(dc.getIpPort().key);
            }
            this.mIndex = newIndex;
        } catch (Throwable e) {
            logger.error("Update Sender Ip Failed." + e.getMessage());
        }
    }

    /**
     * reconnect
     *
     * @param channel
     */
    private void reconnect(SenderChannel channel) {
        try {
            synchronized (channel) {
                if (channel.getChannel().isOpen()) {
                    return;
                }

                Channel oldChannel = channel.getChannel();
                ChannelFuture future = client.connect(channel.getIpPort().addr).await();
                Channel newChannel = future.getChannel();
                channel.setChannel(newChannel);
                oldChannel.disconnect();
                oldChannel.close();
            }
        } catch (Throwable e) {
            logger.error("reconnect failed." + e.getMessage());
        }
    }

    /**
     * get hasSendError
     *
     * @return the hasSendError
     */
    public boolean isHasSendError() {
        return hasSendError;
    }

    /**
     * set hasSendError
     *
     * @param hasSendError the hasSendError to set
     */
    public void setHasSendError(boolean hasSendError) {
        this.hasSendError = hasSendError;
    }

}

