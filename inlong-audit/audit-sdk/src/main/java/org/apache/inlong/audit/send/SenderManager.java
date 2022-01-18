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

import org.apache.inlong.audit.protocol.AuditApi;
import org.apache.inlong.audit.util.AuditData;
import org.apache.inlong.audit.util.Config;
import org.apache.inlong.audit.util.Decoder;
import org.apache.inlong.audit.util.SenderResult;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * sender manager
 */
public class SenderManager {
    private static final Logger logger = LoggerFactory.getLogger(SenderManager.class);
    public static final int DEFAULT_SEND_THREADNUM = 2;
    public static final Long MAX_REQUEST_ID = 1000000000L;
    public static final int ALL_CONNECT_CHANNEL = -1;
    public static final int DEFAULT_CONNECT_CHANNEL = 2;

    private SenderGroup sender;
    private int maxConnectChannels = ALL_CONNECT_CHANNEL;
    private SecureRandom sRandom = new SecureRandom(Long.toString(System.currentTimeMillis()).getBytes());
    // IPList
    private HashSet<String> currentIpPorts = new HashSet<String>();
    private AtomicLong requestIdSeq = new AtomicLong(0L);
    private ConcurrentHashMap<Long, AuditData> dataMap = new ConcurrentHashMap<>();
    private Config config;

    /**
     * Constructor
     *
     * @param config
     */
    public SenderManager(Config config) {
        this(config, DEFAULT_CONNECT_CHANNEL);
    }

    /**
     * Constructor
     *
     * @param config
     * @param maxConnectChannels
     */
    public SenderManager(Config config, int maxConnectChannels) {
        try {
            this.config = config;
            this.maxConnectChannels = maxConnectChannels;
            SenderHandler clientHandler = new SenderHandler(this);
            this.sender = new SenderGroup(DEFAULT_SEND_THREADNUM, new Decoder(), clientHandler);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
    }

    /**
     * update config
     */
    public void setAuditProxy(HashSet<String> ipPortList) {
        if (ipPortList.equals(currentIpPorts) && !this.sender.isHasSendError()) {
            return;
        }
        this.sender.setHasSendError(false);
        this.currentIpPorts = ipPortList;
        int ipSize = ipPortList.size();
        int needNewSize = 0;
        if (this.maxConnectChannels == ALL_CONNECT_CHANNEL || this.maxConnectChannels >= ipSize) {
            needNewSize = ipSize;
        } else {
            needNewSize = maxConnectChannels;
        }
        HashSet<String> updateConfigIpLists = new HashSet<>();
        List<String> availableIpLists = new ArrayList<String>();
        availableIpLists.addAll(ipPortList);
        for (int i = 0; i < needNewSize; i++) {
            int availableIpSize = availableIpLists.size();
            int newIpPortIndex = this.sRandom.nextInt(availableIpSize);
            String ipPort = availableIpLists.remove(newIpPortIndex);
            updateConfigIpLists.add(ipPort);
        }
        if (updateConfigIpLists.size() > 0) {
            this.sender.updateConfig(updateConfigIpLists);
        }
    }

    /**
     * next requestid
     *
     * @return
     */
    public Long nextRequestId() {
        Long requestId = requestIdSeq.getAndIncrement();
        if (requestId > MAX_REQUEST_ID) {
            requestId = 0L;
            requestIdSeq.set(requestId);
        }
        return requestId;
    }

    /**
     * send data
     *
     * @param sdkTime
     * @param baseCommand
     */
    public void send(long sdkTime, AuditApi.BaseCommand baseCommand) {
        AuditData data = new AuditData(sdkTime, baseCommand);
        // Cache first
        this.dataMap.putIfAbsent(baseCommand.getAuditRequest().getRequestId(), data);
        this.sendData(data);
    }

    /**
     * send data
     *
     * @param data
     */
    private void sendData(AuditData data) {
        ChannelBuffer dataBuf = ChannelBuffers.wrappedBuffer(data.getDataByte());
        SenderResult result = this.sender.send(dataBuf);
        if (!result.result) {
            this.sender.setHasSendError(true);
        }
    }

    /**
     * Clean up the backlog of unsent message packets
     */
    public void clearBuffer() {
        for (AuditData data : this.dataMap.values()) {
            this.sendData(data);
        }
    }

    /**
     * get data map szie
     */
    public int getDataMapSize() {
        return this.dataMap.size();
    }

    /**
     * processing return package
     *
     * @param ctx
     * @param e
     */
    public void onMessageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        try {
            //Analyze abnormal events
            if (!(e.getMessage() instanceof ChannelBuffer)) {
                logger.error("onMessageReceived e.getMessage:" + e.getMessage());
                return;
            }
            ChannelBuffer readBuffer = (ChannelBuffer) e.getMessage();
            byte[] readBytes = readBuffer.toByteBuffer().array();
            AuditApi.BaseCommand baseCommand = AuditApi.BaseCommand.parseFrom(readBytes);
            // Parse request id
            Long requestId = baseCommand.getAuditReply().getRequestId();
            AuditData data = this.dataMap.get(requestId);
            if (data == null) {
                logger.error("can not find the requestid onMessageReceived:" + requestId);
                return;
            }
            if (AuditApi.AuditReply.RSP_CODE.SUCCESS.equals(baseCommand.getAuditReply().getRspCode())) {
                this.dataMap.remove(requestId);
                this.sender.notifyAll();
                return;
            }
            int resendTimes = data.increaseResendTimes();
            if (resendTimes < org.apache.inlong.audit.send.SenderGroup.MAX_SEND_TIMES) {
                this.sendData(data);
            }
            this.sender.notifyAll();
        } catch (Throwable ex) {
            logger.error(ex.getMessage());
            this.sender.setHasSendError(true);
        }
    }

    /**
     * Handle the packet return exception
     *
     * @param ctx
     * @param e
     */
    public void onExceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.error(e.getCause().getMessage());
        try {
            this.sender.setHasSendError(true);
        } catch (Throwable ex) {
            logger.error(ex.getMessage());
        }
    }
}
