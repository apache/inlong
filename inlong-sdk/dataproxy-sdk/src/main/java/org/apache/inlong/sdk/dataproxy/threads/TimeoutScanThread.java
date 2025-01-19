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

package org.apache.inlong.sdk.dataproxy.threads;

import org.apache.inlong.sdk.dataproxy.TcpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.common.SendResult;
import org.apache.inlong.sdk.dataproxy.network.QueueObject;
import org.apache.inlong.sdk.dataproxy.network.Sender;
import org.apache.inlong.sdk.dataproxy.network.TimeScanObject;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Daemon threads to check timeout for asynchronous callback.
 */
public class TimeoutScanThread extends Thread {

    private static final int MAX_CHANNEL_TIMEOUT = 5 * 60 * 1000;
    private static final Logger logger = LoggerFactory.getLogger(TimeoutScanThread.class);
    private static final LogCounter exptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private volatile boolean bShutDown = false;
    private long printCount = 0;
    private final TcpMsgSenderConfig tcpConfig;
    private final Sender sender;
    private final ConcurrentHashMap<Channel, TimeScanObject> timeoutChannelStat = new ConcurrentHashMap<>();

    public TimeoutScanThread(Sender sender, TcpMsgSenderConfig tcpConfig) {
        this.bShutDown = false;
        this.tcpConfig = tcpConfig;
        this.sender = sender;
        this.setDaemon(true);
        this.setName("TimeoutScanThread-" + this.sender.getInstanceId());
        logger.info("TimeoutScanThread({}) started", this.sender.getInstanceId());
    }

    public void shutDown() {
        this.bShutDown = true;
        this.interrupt();
        logger.info("TimeoutScanThread({}) shutdown!", this.sender.getInstanceId());
    }

    /**
     * add timeout channel
     *
     * @param channel
     */
    public void addTimeoutChannel(Channel channel) {
        if (channel == null) {
            return;
        }
        TimeScanObject timeScanObject = timeoutChannelStat.get(channel);
        if (timeScanObject == null) {
            TimeScanObject tmpTimeObj = new TimeScanObject();
            timeScanObject = timeoutChannelStat.putIfAbsent(channel, tmpTimeObj);
            if (timeScanObject == null) {
                timeScanObject = tmpTimeObj;
            }
        }
        timeScanObject.incrementAndGet();
    }

    /**
     * reset channel timeout
     *
     * @param channel
     */
    public void resetTimeoutChannel(Channel channel) {
        if (channel == null) {
            return;
        }
        TimeScanObject timeScanObject = timeoutChannelStat.get(channel);
        if (timeScanObject != null) {
            timeScanObject.updateCountToZero();
        }
    }

    /**
     * check timeout
     */
    private void checkTimeoutChannel() {
        // if timeout >3,set channel busy
        for (Channel tmpChannel : timeoutChannelStat.keySet()) {
            TimeScanObject timeScanObject = tmpChannel != null ? timeoutChannelStat.get(tmpChannel) : null;
            if (timeScanObject == null) {
                continue;
            }

            // If the channel exists in the list for more than 5 minutes,
            // and does not reach the maximum number of timeouts, it will be removed
            if (System.currentTimeMillis() - timeScanObject.getTime() > MAX_CHANNEL_TIMEOUT) {
                timeoutChannelStat.remove(tmpChannel);
            } else {
                if (timeScanObject.getCurTimeoutCount() > tcpConfig.getMaxAllowedSyncMsgTimeoutCnt()) {
                    timeoutChannelStat.remove(tmpChannel);
                    if (tmpChannel.isOpen() && tmpChannel.isActive()) {
                        sender.getClientMgr().setConnectionBusy(tmpChannel);
                    }
                }
            }
        }
    }

    /**
     * check message id
     *
     * @param channel
     * @param messageIdCallbacks
     */
    private void checkMessageIdBasedCallbacks(Channel channel,
            ConcurrentHashMap<String, QueueObject> messageIdCallbacks) {
        for (String messageId : messageIdCallbacks.keySet()) {
            QueueObject queueObject = messageId != null ? messageIdCallbacks.get(messageId) : null;
            if (queueObject == null) {
                continue;
            }
            // if queueObject timeout
            if (System.currentTimeMillis() - queueObject.getSendTimeInMillis() >= queueObject.getTimeoutInMillis()) {
                // remove it before callback
                QueueObject queueObject1 = messageIdCallbacks.remove(messageId);
                if (queueObject1 != null) {
                    queueObject1.getCallback().onMessageAck(SendResult.TIMEOUT);
                    sender.getCurrentBufferSize().decrementAndGet();
                    queueObject.done();
                }
                addTimeoutChannel(channel);
            }
        }
    }

    @Override
    public void run() {
        logger.info("TimeoutScanThread({}) thread started!", sender.getInstanceId());
        while (!bShutDown) {
            try {
                for (Map.Entry<Channel, ConcurrentHashMap<String, QueueObject>> entry : sender.getCallbacks()
                        .entrySet()) {
                    if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                        continue;
                    }
                    checkMessageIdBasedCallbacks(entry.getKey(), entry.getValue());
                }
                checkTimeoutChannel();
            } catch (Throwable ex) {
                if (exptCnt.shouldPrint()) {
                    logger.warn("TimeoutScanThread({}) throw exception", sender.getInstanceId(), ex);
                }
            }
            if (printCount++ % 60 == 0) {
                logger.info("TimeoutScanThread({}) scan, currentBufferSize={}",
                        sender.getInstanceId(), sender.getCurrentBufferSize().get());
            }
            if (bShutDown) {
                break;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                //
            }
        }
        logger.info("TimeoutScanThread({}) thread existed !", sender.getInstanceId());
    }
}
