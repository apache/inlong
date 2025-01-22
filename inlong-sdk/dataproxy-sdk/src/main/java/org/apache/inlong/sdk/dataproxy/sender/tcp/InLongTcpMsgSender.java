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

package org.apache.inlong.sdk.dataproxy.sender.tcp;

import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.common.msg.MsgType;
import org.apache.inlong.sdk.dataproxy.MsgSenderFactory;
import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.config.EncryptConfigEntry;
import org.apache.inlong.sdk.dataproxy.network.tcp.SendQos;
import org.apache.inlong.sdk.dataproxy.network.tcp.TcpClientMgr;
import org.apache.inlong.sdk.dataproxy.network.tcp.TcpNettyClient;
import org.apache.inlong.sdk.dataproxy.network.tcp.codec.EncodeObject;
import org.apache.inlong.sdk.dataproxy.sender.BaseSender;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;
import org.apache.inlong.sdk.dataproxy.utils.EncryptUtil;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.xerial.snappy.Snappy;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 * TCP Message Sender class
 *
 * Used to define the TCP sender common methods
 */
public class InLongTcpMsgSender extends BaseSender implements TcpMsgSender {

    protected static final LogCounter tcpExceptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private final TcpMsgSenderConfig tcpConfig;
    private final TcpClientMgr tcpClientMgr;

    public InLongTcpMsgSender(TcpMsgSenderConfig configure) {
        this(configure, null, null, null);
    }

    public InLongTcpMsgSender(TcpMsgSenderConfig configure, ThreadFactory selfDefineFactory) {
        this(configure, selfDefineFactory, null, null);
    }

    public InLongTcpMsgSender(TcpMsgSenderConfig configure,
            ThreadFactory selfDefineFactory, MsgSenderFactory senderFactory, String clusterIdKey) {
        super(configure, senderFactory, clusterIdKey);
        this.tcpConfig = (TcpMsgSenderConfig) baseConfig;
        this.clientMgr = new TcpClientMgr(this.getSenderId(), this.tcpConfig, selfDefineFactory);
        this.tcpClientMgr = (TcpClientMgr) clientMgr;
    }

    @Override
    public boolean sendMessage(TcpEventInfo eventInfo, ProcessResult procResult) {
        if (eventInfo == null) {
            throw new NullPointerException("eventInfo is null");
        }
        if (procResult == null) {
            throw new NullPointerException("procResult is null");
        }
        if (!this.isStarted()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        return processEvent(SendQos.SOURCE_ACK, eventInfo, null, procResult);
    }

    @Override
    public boolean asyncSendMessage(
            TcpEventInfo eventInfo, MsgSendCallback callback, ProcessResult procResult) {
        if (eventInfo == null) {
            throw new NullPointerException("eventInfo is null");
        }
        if (callback == null) {
            throw new NullPointerException("callback is null");
        }
        if (procResult == null) {
            throw new NullPointerException("procResult is null");
        }
        if (!this.isStarted()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        return processEvent(SendQos.SOURCE_ACK, eventInfo, callback, procResult);
    }

    @Override
    public boolean sendMessageWithoutAck(TcpEventInfo eventInfo, ProcessResult procResult) {
        if (eventInfo == null) {
            throw new NullPointerException("eventInfo is null");
        }
        if (procResult == null) {
            throw new NullPointerException("procResult is null");
        }
        if (!this.isStarted()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        return processEvent(SendQos.NO_ACK, eventInfo, null, procResult);
    }

    @Override
    public boolean sendMsgWithSinkAck(TcpEventInfo eventInfo, ProcessResult procResult) {
        if (eventInfo == null) {
            throw new NullPointerException("eventInfo is null");
        }
        if (procResult == null) {
            throw new NullPointerException("procResult is null");
        }
        if (!this.isStarted()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        return processEvent(SendQos.SINK_ACK, eventInfo, null, procResult);
    }

    @Override
    public boolean asyncSendMsgWithSinkAck(
            TcpEventInfo eventInfo, MsgSendCallback callback, ProcessResult procResult) {
        if (eventInfo == null) {
            throw new NullPointerException("eventInfo is null");
        }
        if (callback == null) {
            throw new NullPointerException("callback is null");
        }
        if (procResult == null) {
            throw new NullPointerException("procResult is null");
        }
        if (!this.isStarted()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        return processEvent(SendQos.SINK_ACK, eventInfo, callback, procResult);
    }

    @Override
    public int getActiveNodeCnt() {
        return tcpClientMgr.getActiveNodeCnt();
    }

    @Override
    public int getInflightMsgCnt() {
        return tcpClientMgr.getInflightMsgCnt();
    }

    private boolean processEvent(SendQos sendQos,
            TcpEventInfo eventInfo, MsgSendCallback callback, ProcessResult procResult) {
        if (this.isMetaInfoUnReady()) {
            return procResult.setFailResult(ErrorCode.NO_NODE_META_INFOS);
        }
        EncodeObject encObject = new EncodeObject(eventInfo.getGroupId(),
                eventInfo.getStreamId(), tcpConfig.getSdkMsgType(), eventInfo.getDtMs());
        // pre-process attributes
        processEventAttrsInfo(sendQos, eventInfo, encObject);
        // check package length
        if (!isValidPkgLength(encObject.getAttrDataLength(),
                eventInfo.getBodySize(), this.getAllowedPkgLength(), procResult)) {
            return false;
        }
        // process body
        if (!procEventBodyInfo(eventInfo, procResult, encObject)) {
            return false;
        }
        // get client object
        if (!tcpClientMgr.getClientByRoundRobin(procResult)) {
            return false;
        }
        TcpNettyClient client = (TcpNettyClient) procResult.getRetData();
        encObject.setMessageIdInfo(tcpClientMgr.getNextMsgId());
        try {
            return tcpClientMgr.reportEvent(sendQos, client, encObject, callback, procResult);
        } finally {
            client.decClientUsingCnt();
        }
    }

    private boolean isValidPkgLength(
            int attrLength, int bodyLength, int allowedLen, ProcessResult procResult) {
        // Not valid if the maximum limit is less than or equal to 0
        if (allowedLen < 0) {
            return true;
        }
        // Reserve space for attribute
        if (bodyLength + attrLength > allowedLen - SdkConsts.RESERVED_ATTRIBUTE_LENGTH) {
            String errMsg = String.format("OverMaxLen: attrLen(%d) + bodyLen(%d) > allowedLen(%d) - fixedLen(%d)",
                    attrLength, bodyLength, allowedLen, SdkConsts.RESERVED_ATTRIBUTE_LENGTH);
            if (tcpExceptCnt.shouldPrint()) {
                logger.warn(errMsg);
            }
            return procResult.setFailResult(ErrorCode.REPORT_INFO_EXCEED_MAX_LEN, errMsg);
        }
        return true;
    }

    private void processEventAttrsInfo(
            SendQos sendQos, TcpEventInfo eventInfo, EncodeObject encodeObject) {
        // get msgType
        int intMsgType = encodeObject.getMsgType().getValue();
        boolean enableDataComp = tcpConfig.isEnableDataCompress()
                && eventInfo.getBodySize() >= tcpConfig.getMinCompEnableLength();
        // add fixed attributes
        Map<String, String> newAttrs = new HashMap<>(eventInfo.getAttrs());
        newAttrs.put(AttributeConstants.MSG_RPT_TIME, String.valueOf(encodeObject.getRtms()));
        newAttrs.put(AttributeConstants.PROXY_SDK_VERSION, ProxyUtils.getJarVersion());

        if (sendQos == SendQos.NO_ACK) {
            newAttrs.put(AttributeConstants.MESSAGE_IS_ACK, String.valueOf(true));
        } else if (sendQos == SendQos.SINK_ACK) {
            newAttrs.put(AttributeConstants.MESSAGE_PROXY_SEND, String.valueOf(true));
        }
        if (tcpConfig.getSdkMsgType() == MsgType.MSG_ACK_SERVICE
                || tcpConfig.getSdkMsgType() == MsgType.MSG_MULTI_BODY) {
            // add msgType 3/5 attributes
            newAttrs.put(AttributeConstants.GROUP_ID, eventInfo.getGroupId());
            newAttrs.put(AttributeConstants.STREAM_ID, eventInfo.getStreamId());
            newAttrs.put(AttributeConstants.DATA_TIME, String.valueOf(eventInfo.getDtMs()));
            newAttrs.put(AttributeConstants.MESSAGE_COUNT, String.valueOf(eventInfo.getMsgCnt()));
            if (enableDataComp) {
                newAttrs.put(AttributeConstants.COMPRESS_TYPE, "snappy");
            }
        } else {
            // add msgType 7 attributes
            if (enableDataComp) {
                intMsgType |= SdkConsts.FLAG_ALLOW_COMPRESS;
            }
            int extField = 0;
            if (tcpConfig.isSeparateEventByLF()) {
                extField |= SdkConsts.EXT_FIELD_FLAG_SEP_BY_LF;
            }
            boolean id2Num = false;
            int streamIdNum = 0;
            if (this.idTransNum && (this.groupIdNum != 0)
                    && eventInfo.getGroupId().equals(tcpConfig.getInlongGroupId())) {
                streamIdNum = getStreamIdNum(eventInfo.getStreamId());
                id2Num = streamIdNum != 0;
            }
            if (id2Num) {
                encodeObject.setGroupAndStreamId2Num(this.groupIdNum, streamIdNum);
            } else {
                extField |= SdkConsts.EXT_FIELD_FLAG_DISABLE_ID2NUM;
                newAttrs.put(AttributeConstants.GROUP_ID, eventInfo.getGroupId());
                newAttrs.put(AttributeConstants.STREAM_ID, eventInfo.getStreamId());
            }
            encodeObject.setExtField(extField);
        }
        byte[] aesKey = null;
        // set encrypt attributes
        if (tcpConfig.isEnableReportEncrypt()) {
            EncryptConfigEntry encryptEntry = configManager.getUserEncryptConfigEntry();
            newAttrs.put("_userName", tcpConfig.getRptUserName());
            newAttrs.put("_encyVersion", encryptEntry.getVersion());
            newAttrs.put("_encyAesKey", encryptEntry.getRsaEncryptedKey());
            aesKey = encryptEntry.getAesKey();
            intMsgType |= SdkConsts.FLAG_ALLOW_ENCRYPT;
        }
        encodeObject.setAttrInfo(intMsgType, enableDataComp, aesKey, newAttrs);
    }

    private boolean procEventBodyInfo(TcpEventInfo eventInfo, ProcessResult procResult, EncodeObject encObject) {
        // encode message body
        byte[] body = encBodyList(senderId, encObject.getMsgType(),
                tcpConfig.isSeparateEventByLF(), eventInfo, procResult);
        if (body == null) {
            return false;
        }
        // compress body
        if (encObject.isCompress()) {
            body = compressBodyInfo(senderId, body, procResult);
            if (body == null) {
                return false;
            }
        }
        // encrypt body
        if (tcpConfig.isEnableReportEncrypt()) {
            body = aesEncryptBodyInfo(senderId, body, encObject.getAesKey(), procResult);
            if (body == null) {
                return false;
            }
        }
        encObject.setBodyData(eventInfo.getMsgCnt(), body);
        return true;
    }

    private byte[] encBodyList(String senderId,
            MsgType msgType, boolean sepByLF, TcpEventInfo eventInfo, ProcessResult procResult) {
        try {
            int totalCnt = 0;
            ByteArrayOutputStream bodyOut = new ByteArrayOutputStream();
            if (msgType == MsgType.MSG_ACK_SERVICE) {
                for (byte[] entry : eventInfo.getBodyList()) {
                    if (totalCnt++ > 0) {
                        bodyOut.write(AttributeConstants.LINE_FEED_SEP.getBytes(StandardCharsets.UTF_8));
                    }
                    bodyOut.write(entry);
                }
            } else if (msgType == MsgType.MSG_MULTI_BODY) {
                for (byte[] entry : eventInfo.getBodyList()) {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                    byteBuffer.putInt(entry.length);
                    bodyOut.write(byteBuffer.array());
                    bodyOut.write(entry);
                }
            } else {
                if (sepByLF) {
                    ByteArrayOutputStream data = new ByteArrayOutputStream();
                    for (byte[] entry : eventInfo.getBodyList()) {
                        if (totalCnt++ > 0) {
                            data.write(AttributeConstants.LINE_FEED_SEP.getBytes(StandardCharsets.UTF_8));
                        }
                        data.write(entry);
                    }
                    ByteBuffer dataBuffer = ByteBuffer.allocate(4);
                    dataBuffer.putInt(data.toByteArray().length);
                    bodyOut.write(dataBuffer.array());
                    bodyOut.write(data.toByteArray());
                } else {
                    for (byte[] entry : eventInfo.getBodyList()) {
                        ByteBuffer dataBuffer = ByteBuffer.allocate(4);
                        dataBuffer.putInt(entry.length);
                        bodyOut.write(dataBuffer.array());
                        bodyOut.write(entry);
                    }
                }
            }
            return bodyOut.toByteArray();
        } catch (Throwable ex) {
            procResult.setFailResult(ErrorCode.ENCODE_BODY_EXCEPTION, ex.getMessage());
            if (tcpExceptCnt.shouldPrint()) {
                logger.warn("Sender({}) encode body exception", senderId, ex);
            }
            return null;
        }
    }

    private byte[] compressBodyInfo(String senderId, byte[] body, ProcessResult procResult) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(body);
            int guessLen = Snappy.maxCompressedLength(out.size());
            byte[] tmpData = new byte[guessLen];
            int len = Snappy.compress(out.toByteArray(), 0, out.size(),
                    tmpData, 0);
            body = new byte[len];
            System.arraycopy(tmpData, 0, body, 0, len);
            return body;
        } catch (Throwable ex) {
            procResult.setFailResult(ErrorCode.COMPRESS_BODY_EXCEPTION, ex.getMessage());
            if (tcpExceptCnt.shouldPrint()) {
                logger.warn("Sender({}) compress body exception", senderId, ex);
            }
            return null;
        }
    }

    private byte[] aesEncryptBodyInfo(String senderId,
            byte[] plainText, byte[] aesKey, ProcessResult procResult) {
        try {
            SecretKeySpec secretKeySpec = new SecretKeySpec(aesKey, EncryptUtil.AES);
            Cipher cipher = Cipher.getInstance(EncryptUtil.AES);
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
            return cipher.doFinal(plainText);
        } catch (Throwable ex) {
            procResult.setFailResult(ErrorCode.ENCRYPT_BODY_EXCEPTION, ex.getMessage());
            if (tcpExceptCnt.shouldPrint()) {
                logger.warn("Sender({}) aesEncrypt body exception", senderId, ex);
            }
            return null;
        }
    }
}
