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

import org.apache.inlong.audit.util.SenderResult;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class SenderGroup {

    public static final Logger LOG = LoggerFactory.getLogger(SenderGroup.class);
    public static final AttributeKey<String> CHANNEL_KEY = AttributeKey.newInstance("channelKey");
    // maximum number of sending
    public static final int MAX_SEND_TIMES = 3;
    public static final int DEFAULT_WAIT_TIMES = 10000;
    public static final int WAIT_INTERVAL = 1;
    public static final int DEFAULT_SYNCH_REQUESTS = 1;
    public static final int RANDOM_MIN = 0;

    private List<LinkedBlockingQueue<SenderChannel>> channelGroups = new ArrayList<>();
    private int mIndex = 0;
    private List<SenderChannel> deleteChannels = new ArrayList<>();
    private ConcurrentHashMap<String, SenderChannel> totalChannels = new ConcurrentHashMap<>();

    private int waitChannelTimes = DEFAULT_WAIT_TIMES;
    private int waitChannelIntervalMs = WAIT_INTERVAL;
    private int maxSynchRequest = DEFAULT_SYNCH_REQUESTS;
    private boolean hasSendError = false;

    private SenderManager senderManager;

    private AtomicLong channelId = new AtomicLong(0);

    /**
     * constructor
     *
     * @param senderManager
     */
    public SenderGroup(SenderManager senderManager) {
        this.senderManager = senderManager;
        /*
         * init add two list for update config
         */
        channelGroups.add(new LinkedBlockingQueue<>());
        channelGroups.add(new LinkedBlockingQueue<>());
    }

    /**
     * send data
     *
     * @param dataBuf
     * @return
     */
    public SenderResult send(ByteBuf dataBuf) {
        LinkedBlockingQueue<SenderChannel> channels = channelGroups.get(mIndex);
        SenderChannel channel = null;
        boolean dataBufReleased = false;
        try {
            if (channels.size() <= 0) {
                LOG.error("channels is empty");
                dataBuf.release();
                dataBufReleased = true;
                return new SenderResult("channels is empty", 0, false);
            }
            boolean isOk = false;
            // tryAcquire
            for (int tryIndex = 0; tryIndex < MAX_SEND_TIMES; tryIndex++) {
                for (int i = 0; i < channels.size(); i++) {
                    channel = channels.poll();
                    if (channel.tryAcquire()) {
                        if (channel.connect()) {
                            isOk = true;
                            break;
                        }
                        channel.release();
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
                    LOG.error(e.getMessage());
                }
            }
            // acquire
            if (channel == null) {
                for (int i = 0; i < channels.size(); i++) {
                    channel = channels.poll();
                    if (!channel.connect()) {
                        channels.offer(channel);
                        channel = null;
                        continue;
                    }
                    if (channel.acquire()) {
                        break;
                    }
                }
            }
            // error
            if (channel == null) {
                LOG.error("can not get a channel");
                dataBuf.release();
                dataBufReleased = true;
                return new SenderResult("can not get a channel", 0, false);
            }

            ChannelFuture t = null;
            if (channel.getChannel().isWritable()) {
                t = channel.getChannel().writeAndFlush(dataBuf).sync().await();
                if (!t.isSuccess()) {
                    if (!channel.getChannel().isActive()) {
                        channel.connect();
                    }
                    t = channel.getChannel().writeAndFlush(dataBuf).sync().await();
                }
                dataBufReleased = true;
            } else {
                dataBuf.release();
                dataBufReleased = true;
            }
            return new SenderResult(channel.getAddr().getHostString(), channel.getAddr().getPort(), t.isSuccess());
        } catch (Throwable ex) {
            LOG.error(ex.getMessage(), ex);
            this.setHasSendError(true);
            return new SenderResult("127.0.0.1", 0, false);
        } finally {
            if (channel != null) {
                channel.release();
                channels.offer(channel);
            }
            if (!dataBufReleased && dataBuf != null) {
                dataBuf.release();
            }
        }
    }

    public void release(Channel channel) {
        Attribute<String> attr = channel.attr(CHANNEL_KEY);
        String key = attr.get();
        if (key == null) {
            return;
        }
        SenderChannel senderChannel = this.totalChannels.get(key);
        if (senderChannel != null) {
            senderChannel.release();
        }
    }

    /**
     * update config
     *
     * @param ipLists
     */
    public void updateConfig(List<String> ipLists) {
        try {
            for (SenderChannel dc : deleteChannels) {
                if (dc.getChannel() != null) {
                    try {
                        dc.getChannel().disconnect();
                        dc.getChannel().close();
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }
            deleteChannels.clear();
            int newIndex = mIndex ^ 0x01;
            LinkedBlockingQueue<SenderChannel> newChannels = this.channelGroups.get(newIndex);
            newChannels.clear();
            List<String> waitingDeleteChannelKey = new ArrayList<>(totalChannels.size());
            waitingDeleteChannelKey.addAll(totalChannels.keySet());
            for (String ipPort : ipLists) {
                try {
                    InetSocketAddress addr = parseAddress(ipPort);
                    if (addr == null) {
                        continue;
                    }
                    String key = String.valueOf(channelId.getAndIncrement());
                    SenderChannel channel = new SenderChannel(addr, maxSynchRequest, senderManager);
                    channel.setChannelKey(key);
                    newChannels.add(channel);
                    totalChannels.put(key, channel);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }

            waitingDeleteChannelKey.forEach(v -> deleteChannels.add(totalChannels.remove(v)));
            this.mIndex = newIndex;
        } catch (Throwable e) {
            LOG.error("Update Sender Ip Failed." + e.getMessage(), e);
        }
    }

    /**
     * parseAddress
     * 
     * @param  InetSocketAddress
     * @return
     */
    private static InetSocketAddress parseAddress(String ipPort) {
        String[] splits = ipPort.split(":");
        if (splits.length == 2) {
            String strIp = splits[0];
            String strPort = splits[1];
            int port = NumberUtils.toInt(strPort, 0);
            if (port > 0) {
                return new InetSocketAddress(strIp, port);
            }
        }
        return null;
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
