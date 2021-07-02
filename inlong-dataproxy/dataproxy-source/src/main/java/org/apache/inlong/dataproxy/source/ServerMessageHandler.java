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

import static org.apache.inlong.dataproxy.consts.AttributeConstants.SEPARATOR;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.SLA_METRIC_BID;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.SLA_METRIC_DATA;
import static org.apache.inlong.dataproxy.source.SimpleTcpSource.blacklist;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.inlong.commons.msg.TDMsg1;
import org.apache.inlong.dataproxy.base.ProxyMessage;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.consts.AttributeConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.exception.ErrorCode;
import org.apache.inlong.dataproxy.exception.MessageIDException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server message handler
 *
 */
public class ServerMessageHandler extends SimpleChannelHandler {

    private static final Logger logger = LoggerFactory.getLogger(ServerMessageHandler.class);

    private static final String DEFAULT_REMOTE_IP_VALUE = "0.0.0.0";
    private static final String DEFAULT_REMOTE_IDC_VALUE = "0";
    private static final ConfigManager configManager = ConfigManager.getInstance();
    private static final Joiner.MapJoiner mapJoiner = Joiner.on(AttributeConstants.SEPARATOR)
            .withKeyValueSeparator(AttributeConstants.KEY_VALUE_SEPARATOR);
    private static final Splitter.MapSplitter mapSplitter = Splitter
            .on(AttributeConstants.SEPARATOR)
            .trimResults().withKeyValueSeparator(AttributeConstants.KEY_VALUE_SEPARATOR);

    private static final ThreadLocal<SimpleDateFormat> dateFormator =
            new ThreadLocal<SimpleDateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    return new SimpleDateFormat("yyyyMMddHHmm");
                }
            };
    private static final ThreadLocal<SimpleDateFormat> dateFormator4Transfer =
            new ThreadLocal<SimpleDateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    return new SimpleDateFormat("yyyyMMddHHmmss");
                }
            };

    private final ChannelGroup allChannels;
    private int maxConnections = Integer.MAX_VALUE;
    private boolean filterEmptyMsg = false;
    private final boolean isCompressed;
    private final ChannelProcessor processor;
    private final ServiceDecoder serviceProcessor;
    private final String defaultTopic;
    private String defaultMXAttr = "m=3";
    private final ChannelBuffer heartbeatBuffer;
    private final String protocolType;


    public ServerMessageHandler(ChannelProcessor processor, ServiceDecoder serProcessor,
                                ChannelGroup allChannels,
                                String topic, String attr, Boolean filterEmptyMsg, Integer maxMsgLength,
                                Integer maxCons,
                                Boolean isCompressed, String protocolType) {

        this.processor = processor;
        this.serviceProcessor = serProcessor;
        this.allChannels = allChannels;
        this.defaultTopic = topic;
        if (null != attr) {
            this.defaultMXAttr = attr;
        }

        this.filterEmptyMsg = filterEmptyMsg;
        this.isCompressed = isCompressed;
        this.heartbeatBuffer = ChannelBuffers.wrappedBuffer(new byte[]{0, 0, 0, 1, 1});
        this.maxConnections = maxCons;
        this.protocolType = protocolType;
    }

    private String getRemoteIp(Channel channel) {
        String strRemoteIp = DEFAULT_REMOTE_IP_VALUE;
        SocketAddress remoteSocketAddress = channel.getRemoteAddress();
        if (null != remoteSocketAddress) {
            strRemoteIp = remoteSocketAddress.toString();
            try {
                strRemoteIp = strRemoteIp.substring(1, strRemoteIp.indexOf(':'));
            } catch (Exception ee) {
                logger.warn("fail to get the remote IP, and strIP={},remoteSocketAddress={}",
                        strRemoteIp,
                        remoteSocketAddress);
            }
        }
        return strRemoteIp;
    }

    private byte[] newBinMsg(byte[] orgBinMsg, String extraAttr) {
        final int BIN_MSG_TOTALLEN_OFFSET = 0;
        final int BIN_MSG_TOTALLEN_SIZE = 4;
        final int BIN_MSG_BODYLEN_SIZE = 4;
        final int BIN_MSG_EXTEND_OFFSET = 9;
        final int BIN_MSG_BODYLEN_OFFSET = 21;
        final int BIN_MSG_BODY_OFFSET = BIN_MSG_BODYLEN_SIZE + BIN_MSG_BODYLEN_OFFSET;
        final int BIN_MSG_ATTRLEN_SIZE = 2;
        final int BIN_MSG_FORMAT_SIZE = 29;
        final int BIN_MSG_MAGIC_SIZE = 2;
        final int BIN_MSG_MAGIC = 0xEE01;

        ByteBuffer orgBuf = ByteBuffer.wrap(orgBinMsg);
        int totalLen = orgBuf.getInt(BIN_MSG_TOTALLEN_OFFSET);
        int dataLen = orgBuf.getInt(BIN_MSG_BODYLEN_OFFSET);
        int attrLen = orgBuf.getShort(BIN_MSG_BODY_OFFSET + dataLen);

        int newTotalLen = 0;
        String strAttr;
        if (attrLen != 0) {
            newTotalLen = totalLen + extraAttr.length() + "&".length();
            strAttr = "&" + extraAttr;
        } else {
            newTotalLen = totalLen + extraAttr.length();
            strAttr = extraAttr;
        }

        ByteBuffer dataBuf = ByteBuffer.allocate(newTotalLen + BIN_MSG_TOTALLEN_SIZE);
        dataBuf
                .put(orgBuf.array(), 0, dataLen + (BIN_MSG_FORMAT_SIZE - BIN_MSG_MAGIC_SIZE) + attrLen);
        dataBuf
                .putShort(dataLen + (BIN_MSG_FORMAT_SIZE - BIN_MSG_ATTRLEN_SIZE - BIN_MSG_MAGIC_SIZE),
                        (short) (strAttr.length() + attrLen));


        System.arraycopy(strAttr.getBytes(StandardCharsets.UTF_8), 0, dataBuf.array(),
                dataLen + (BIN_MSG_FORMAT_SIZE - BIN_MSG_MAGIC_SIZE) + attrLen,
                strAttr.length());
        int extendField = orgBuf.getShort(BIN_MSG_EXTEND_OFFSET);
        dataBuf.putShort(BIN_MSG_EXTEND_OFFSET, (short) (extendField | 0x4));
        dataBuf.putInt(0, newTotalLen);
        dataBuf.putShort(newTotalLen + BIN_MSG_TOTALLEN_SIZE - BIN_MSG_MAGIC_SIZE,
                (short) BIN_MSG_MAGIC);
        return dataBuf.array();
    }

    public boolean checkBlackIp(Channel channel) {
        String strRemoteIp = getRemoteIp(channel);
        if (strRemoteIp != null && blacklist != null && blacklist.contains(strRemoteIp)) {
            logger.error(strRemoteIp + " is in blacklist, so refuse it !");
            channel.disconnect();
            channel.close();
            allChannels.remove(channel);
            return true;
        } else {
            return false;
        }
    }


    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (allChannels.size() - 1 >= maxConnections) {
            logger.warn("refuse to connect , and connections=" + (allChannels.size() - 1)
                    + ", maxConnections="
                    + maxConnections + ",channel is " + e.getChannel());
            e.getChannel().disconnect();
            e.getChannel().close();
        }
        if (!checkBlackIp(e.getChannel())) {
            logger.info("connections={},maxConnections={}", allChannels.size() - 1, maxConnections);
            allChannels.add(e.getChannel());
            super.channelOpen(ctx, e);
        }
    }

    private void checkBidInfo(ProxyMessage message, Map<String, String> commonAttrMap,
        Map<String, String> attrMap, AtomicReference<String> topicInfo) {
        String bid = message.getBid();
        String tid;
        if (null != bid) {
            String from = commonAttrMap.get(AttributeConstants.FROM);
            if ("dc".equals(from)) {
                String dcInterfaceId = message.getTid();
                if (StringUtils.isNotEmpty(dcInterfaceId)
                    && configManager.getDcMappingProperties()
                    .containsKey(dcInterfaceId.trim())) {
                    bid = configManager.getDcMappingProperties()
                        .get(dcInterfaceId.trim()).trim();
                    message.setBid(bid);
                }
            }


            String value = configManager.getTopicProperties().get(bid);
            if (StringUtils.isNotEmpty(value)) {
                topicInfo.set(value.trim());
            }


            Map<String, String> mxValue = configManager.getMxPropertiesMaps().get(bid);
            if (mxValue != null && mxValue.size() != 0) {
                message.getAttributeMap().putAll(mxValue);
            } else {
                message.getAttributeMap().putAll(mapSplitter.split(this.defaultMXAttr));
            }
        } else {
            String num2name = commonAttrMap.get(AttributeConstants.NUM2NAME);
            String bidNum = commonAttrMap.get(AttributeConstants.BID_NUM);
            String tidNum = commonAttrMap.get(AttributeConstants.TID_NUM);

            if (configManager.getBidMappingProperties() != null
                && configManager.getTidMappingProperties() != null) {
                bid = configManager.getBidMappingProperties().get(bidNum);
                tid = (configManager.getTidMappingProperties().get(bidNum) == null)
                    ? null : configManager.getTidMappingProperties().get(bidNum).get(tidNum);
                if (bid != null && tid != null) {
                    String enableTrans =
                        (configManager.getBidEnableMappingProperties() == null)
                            ? null : configManager.getBidEnableMappingProperties().get(bidNum);
                    if (("TRUE".equalsIgnoreCase(enableTrans) && "TRUE"
                        .equalsIgnoreCase(num2name))) {
                        String extraAttr = "bid=" + bid + "&" + "tid=" + tid;
                        message.setData(newBinMsg(message.getData(), extraAttr));
                    }

                    attrMap.put(AttributeConstants.BUSINESS_ID, bid);
                    attrMap.put(AttributeConstants.INTERFACE_ID, tid);
                    message.setBid(bid);
                    message.setTid(tid);


                    String value = configManager.getTopicProperties().get(bid);
                    if (StringUtils.isNotEmpty(value)) {
                        topicInfo.set(value.trim());
                    }
                }
            }
        }
    }

    private void updateMsgList(List<ProxyMessage> msgList, Map<String, String> commonAttrMap,
        Map<String, HashMap<String, List<ProxyMessage>>> messageMap,
        String strRemoteIP, MsgType msgType) {
        for (ProxyMessage message : msgList) {
            Map<String, String> attrMap = message.getAttributeMap();

            String topic = this.defaultTopic;

            AtomicReference<String> topicInfo = new AtomicReference<>(topic);
            checkBidInfo(message, commonAttrMap, attrMap, topicInfo);
            topic = topicInfo.get();

//                if(bid==null)bid="b_test";//default bid

            message.setTopic(topic);
            commonAttrMap.put(AttributeConstants.NODE_IP, strRemoteIP);

            String bid = message.getBid();
            String tid = message.getTid();

            // whether sla
            if (SLA_METRIC_BID.equals(bid)) {
                commonAttrMap.put(SLA_METRIC_DATA, "true");
                message.setTopic(SLA_METRIC_DATA);
            }

            if (bid != null && tid != null) {
                String tubeSwtichKey = bid + SEPARATOR + tid;
                if (configManager.getTubeSwitchProperties().get(tubeSwtichKey) != null
                    && "false".equals(configManager.getTubeSwitchProperties()
                    .get(tubeSwtichKey).trim())) {
                    continue;
                }
            }

            if (!"pb".equals(attrMap.get(AttributeConstants.MESSAGE_TYPE))
                && !MsgType.MSG_MULTI_BODY.equals(msgType)
                && !MsgType.MSG_MULTI_BODY_ATTR.equals(msgType)) {
                byte[] data = message.getData();
                if (data[data.length - 1] == '\n') {
                    int tripDataLen = data.length - 1;
                    if (data[data.length - 2] == '\r') {
                        tripDataLen = data.length - 2;
                    }
                    byte[] tripData = new byte[tripDataLen];
                    System.arraycopy(data, 0, tripData, 0, tripDataLen);
                    message.setData(tripData);
                }
            }

            if (tid == null) {
                tid = "";
            }
            HashMap<String, List<ProxyMessage>> tidMsgMap = messageMap
                .computeIfAbsent(topic, k -> new HashMap<>());
            List<ProxyMessage> tidMsgList = tidMsgMap
                .computeIfAbsent(tid, k -> new ArrayList<>());
            tidMsgList.add(message);
        }
    }

    private void formatMessagesAndSend(Map<String, String> commonAttrMap,
        Map<String, HashMap<String, List<ProxyMessage>>> messageMap,
        String strRemoteIP, MsgType msgType) throws MessageIDException {

        int tdMsgVer = 1;
        if (MsgType.MSG_MULTI_BODY_ATTR.equals(msgType)) {
            tdMsgVer = 3;
        } else if (MsgType.MSG_BIN_MULTI_BODY.equals(msgType)) {
            tdMsgVer = 4;
        }

        for (Map.Entry<String, HashMap<String, List<ProxyMessage>>> topicEntry : messageMap.entrySet()) {
            for (Map.Entry<String, List<ProxyMessage>> tidEntry : topicEntry.getValue().entrySet()) {

                TDMsg1 tdMsg = TDMsg1.newTDMsg(this.isCompressed, tdMsgVer);
                Map<String, String> headers = new HashMap<String, String>();
                for (ProxyMessage message : tidEntry.getValue()) {
                    if (MsgType.MSG_MULTI_BODY_ATTR.equals(msgType) || MsgType.MSG_MULTI_BODY.equals(msgType)) {
                        message.getAttributeMap().put(AttributeConstants.MESSAGE_COUNT, String.valueOf(1));
                        tdMsg.addMsg(mapJoiner.join(message.getAttributeMap()), message.getData());
                    } else if (MsgType.MSG_BIN_MULTI_BODY.equals(msgType)) {
                        tdMsg.addMsg(message.getData());
                    } else {
                        tdMsg.addMsg(mapJoiner.join(message.getAttributeMap()), message.getData());
                    }
                }


                long pkgTimeInMillis = tdMsg.getCreatetime();
                String pkgTimeStr = dateFormator.get().format(pkgTimeInMillis);

                if (tdMsgVer == 4) {
                    if (commonAttrMap.containsKey(ConfigConstants.PKG_TIME_KEY)) {
                        pkgTimeStr = commonAttrMap.get(ConfigConstants.PKG_TIME_KEY);
                    } else {
                        pkgTimeStr = dateFormator.get().format(System.currentTimeMillis());
                    }
                }


                if (commonAttrMap.get(AttributeConstants.DATA_TIME) != null) {
                    headers.put(AttributeConstants.DATA_TIME, commonAttrMap.get(AttributeConstants.DATA_TIME));
                } else {
                    headers.put(AttributeConstants.DATA_TIME, String.valueOf(System.currentTimeMillis()));
                }

                headers.put(ConfigConstants.TOPIC_KEY, topicEntry.getKey());
                headers.put(AttributeConstants.INTERFACE_ID, tidEntry.getKey());
                headers.put(ConfigConstants.REMOTE_IP_KEY, strRemoteIP);
                headers.put(ConfigConstants.REMOTE_IDC_KEY, DEFAULT_REMOTE_IDC_VALUE);
                // every message share the same msg cnt? what if msgType = 5
                String proxyMetricMsgCnt = commonAttrMap.get(AttributeConstants.MESSAGE_COUNT);
                headers.put(ConfigConstants.MSG_COUNTER_KEY, proxyMetricMsgCnt);


                byte[] data = tdMsg.buildArray();
                headers.put(ConfigConstants.TOTAL_LEN, String.valueOf(data.length));

                String sequenceId = commonAttrMap.get(AttributeConstants.SEQUENCE_ID);
                if (StringUtils.isNotEmpty(sequenceId)) {


                    StringBuilder sidBuilder = new StringBuilder();
                    sidBuilder.append(topicEntry.getKey()).append(SEPARATOR).append(tidEntry.getKey())
                        .append(SEPARATOR).append(sequenceId);
                    headers.put(ConfigConstants.SEQUENCE_ID, sidBuilder.toString());
                }

                headers.put(ConfigConstants.PKG_TIME_KEY, pkgTimeStr);
                Event event = EventBuilder.withBody(data, headers);

                long dtten = 0;
                try {
                    dtten = Long.parseLong(headers.get(AttributeConstants.DATA_TIME));
                } catch (Exception e1) {
                    long uniqVal = Long.parseLong(commonAttrMap.get(AttributeConstants.UNIQ_ID));
                    throw new MessageIDException(uniqVal,
                        ErrorCode.DT_ERROR,
                        new Throwable("attribute dt=" + headers.get(AttributeConstants.DATA_TIME
                            + " has error, detail is: topic=" + topicEntry.getKey() + "&tid="
                            + tidEntry.getKey() + "&NodeIP=" + strRemoteIP), e1));
                }

                dtten = dtten / 1000 / 60 / 10;
                dtten = dtten * 1000 * 60 * 10;
                try {
                    processor.processEvent(event);
                } catch (Throwable ex) {
                    logger.error("Error writting to channel,data will discard.", ex);

                    throw new ChannelException("ProcessEvent error can't write event to channel.");
                }
            }
        }
    }

    private void responsePackage(Map<String, String> commonAttrMap,
        Map<String, Object> resultMap,
        Channel remoteChannel,
        SocketAddress remoteSocketAddress,
        MsgType msgType) throws Exception {
        if (!commonAttrMap.containsKey("isAck") || "true".equals(commonAttrMap.get("isAck"))) {
            if (MsgType.MSG_ACK_SERVICE.equals(msgType) || MsgType.MSG_ORIGINAL_RETURN
                .equals(msgType)
                || MsgType.MSG_MULTI_BODY.equals(msgType) || MsgType.MSG_MULTI_BODY_ATTR
                .equals(msgType)) {
                byte[] backAttr = mapJoiner.join(commonAttrMap).getBytes(StandardCharsets.UTF_8);
                byte[] backBody = null;

                if (backAttr != null && !new String(backAttr, StandardCharsets.UTF_8).isEmpty()) {
                    if (MsgType.MSG_ORIGINAL_RETURN.equals(msgType)) {

                        backBody = (byte[]) resultMap.get(ConfigConstants.DECODER_BODY);
                    } else {

                        backBody = new byte[]{50};
                    }
                    int backTotalLen = 1 + 4 + backBody.length + 4 + backAttr.length;
                    ChannelBuffer buffer = ChannelBuffers.buffer(4 + backTotalLen);
                    buffer.writeInt(backTotalLen);
                    buffer.writeByte(msgType.getValue());
                    buffer.writeInt(backBody.length);
                    buffer.writeBytes(backBody);
                    buffer.writeInt(backAttr.length);
                    buffer.writeBytes(backAttr);
                    if (remoteChannel.isWritable()) {
                        remoteChannel.write(buffer, remoteSocketAddress);
                    } else {
                        String backAttrStr = new String(backAttr, StandardCharsets.UTF_8);
                        logger.warn(
                            "the send buffer1 is full, so disconnect it!please check remote client"
                                + "; Connection info:"
                                + remoteChannel + ";attr is " + backAttrStr);
                        throw new Exception(new Throwable(
                            "the send buffer1 is full, so disconnect it!please check remote client"
                                +
                                "; Connection info:" + remoteChannel + ";attr is "
                                + backAttrStr));
                    }
                }
            } else if (MsgType.MSG_BIN_MULTI_BODY.equals(msgType)) {
                String backattrs = null;
                if (resultMap.containsKey(ConfigConstants.DECODER_ATTRS)) {
                    backattrs = (String) resultMap.get(ConfigConstants.DECODER_ATTRS);
                }

                int binTotalLen = 1 + 4 + 2 + 2;
                if (null != backattrs) {
                    binTotalLen += backattrs.length();
                }

                ChannelBuffer binBuffer = ChannelBuffers.buffer(4 + binTotalLen);
                binBuffer.writeInt(binTotalLen);
                binBuffer.writeByte(msgType.getValue());

                long uniqVal = Long.parseLong(commonAttrMap.get(AttributeConstants.UNIQ_ID));
                byte[] uniq = new byte[4];
                uniq[0] = (byte) ((uniqVal >> 24) & 0xFF);
                uniq[1] = (byte) ((uniqVal >> 16) & 0xFF);
                uniq[2] = (byte) ((uniqVal >> 8) & 0xFF);
                uniq[3] = (byte) (uniqVal & 0xFF);
                binBuffer.writeBytes(uniq);

                if (null != backattrs) {
                    binBuffer.writeShort(backattrs.length());
                    binBuffer.writeBytes(backattrs.getBytes(StandardCharsets.UTF_8));
                } else {
                    binBuffer.writeShort(0x0);
                }

                binBuffer.writeShort(0xee01);
                if (remoteChannel.isWritable()) {
                    remoteChannel.write(binBuffer, remoteSocketAddress);
                } else {
                    logger.warn(
                        "the send buffer2 is full, so disconnect it!please check remote client"
                            + "; Connection info:" + remoteChannel + ";attr is "
                            + backattrs);
                    throw new Exception(new Throwable(
                        "the send buffer2 is full,so disconnect it!please check remote client, Connection info:"
                            + remoteChannel + ";attr is " + backattrs));
                }
            }
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        logger.info("message received");
        if (e == null) {
            logger.error("get null messageevent, just skip");
            return;
        }
        ChannelBuffer cb = ((ChannelBuffer) e.getMessage());
        String strRemoteIP = getRemoteIp(e.getChannel());
        SocketAddress remoteSocketAddress = e.getRemoteAddress();
        int len = cb.readableBytes();
        if (len == 0 && this.filterEmptyMsg) {
            logger.warn("skip empty msg.");
            cb.clear();
            return;
        }

        Channel remoteChannel = e.getChannel();
        Map<String, Object> resultMap = null;
        try {
            resultMap = serviceProcessor.extractData(cb, remoteChannel);
        } catch (MessageIDException ex) {
            throw new IOException(ex.getCause());
        }

        if (resultMap == null) {
            logger.info("result is null");
            return;
        }

        MsgType msgType = (MsgType) resultMap.get(ConfigConstants.MSG_TYPE);
        if (MsgType.MSG_HEARTBEAT.equals(msgType)) {
            remoteChannel.write(heartbeatBuffer, remoteSocketAddress);
            return;
        }

        if (MsgType.MSG_BIN_HEARTBEAT.equals(msgType)) {
//            ChannelBuffer binBuffer = getBinHeart(resultMap,msgType);
//            remoteChannel.write(binBuffer, remoteSocketAddress);
            return;
        }

        Map<String, String> commonAttrMap =
                (Map<String, String>) resultMap.get(ConfigConstants.COMMON_ATTR_MAP);
        if (commonAttrMap == null) {
            commonAttrMap = new HashMap<String, String>();
        }

        List<ProxyMessage> msgList = (List<ProxyMessage>) resultMap.get(ConfigConstants.MSG_LIST);
        if (msgList != null
                && !commonAttrMap.containsKey(ConfigConstants.FILE_CHECK_DATA)
                && !commonAttrMap.containsKey(ConfigConstants.MINUTE_CHECK_DATA)) {
            Map<String, HashMap<String, List<ProxyMessage>>> messageMap =
                    new HashMap<String, HashMap<String, List<ProxyMessage>>>(
                    msgList.size());

            updateMsgList(msgList, commonAttrMap, messageMap, strRemoteIP, msgType);

            formatMessagesAndSend(commonAttrMap, messageMap, strRemoteIP, msgType);

        } else if (msgList != null && commonAttrMap.containsKey(ConfigConstants.FILE_CHECK_DATA)) {
//            logger.info("i am in FILE_CHECK_DATA ");
            Map<String, String> headers = new HashMap<String, String>();
            headers.put("msgtype", "filestatus");
            headers.put(ConfigConstants.FILE_CHECK_DATA,
                    "true");
            for (ProxyMessage message : msgList) {
                byte[] body = message.getData();
//                logger.info("data:"+new String(body));
                Event event = EventBuilder.withBody(body, headers);
                try {
                    processor.processEvent(event);
                } catch (Throwable ex) {
                    logger.error("Error writing to controller,data will discard.", ex);

                    throw new ChannelException(
                            "Process Controller Event error can't write event to channel.");
                }
            }
        } else if (msgList != null && commonAttrMap
                .containsKey(ConfigConstants.MINUTE_CHECK_DATA)) {
            logger.info("i am in MINUTE_CHECK_DATA");
            Map<String, String> headers = new HashMap<String, String>();
            headers.put("msgtype", "measure");
            headers.put(ConfigConstants.FILE_CHECK_DATA,
                    "true");
            for (ProxyMessage message : msgList) {
                byte[] body = message.getData();
//                logger.info("data:"+new String(body));
                Event event = EventBuilder.withBody(body, headers);
                try {
                    processor.processEvent(event);
                } catch (Throwable ex) {
                    logger.error("Error writing to controller,data will discard.", ex);


                    throw new ChannelException(
                            "Process Controller Event error can't write event to channel.");
                }
            }
        }
        responsePackage(commonAttrMap, resultMap, remoteChannel, remoteSocketAddress, msgType);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        logger.error("exception caught", e.getCause());
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        logger.error("channel closed {}", ctx.getChannel());
    }
}
