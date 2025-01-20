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

package org.apache.inlong.sdk.dataproxy;

import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.common.util.MessageUtils;
import org.apache.inlong.sdk.dataproxy.codec.EncodeObject;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.common.SendMessageCallback;
import org.apache.inlong.sdk.dataproxy.common.SendResult;
import org.apache.inlong.sdk.dataproxy.config.ProxyConfigEntry;
import org.apache.inlong.sdk.dataproxy.config.ProxyConfigManager;
import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.network.Sender;
import org.apache.inlong.sdk.dataproxy.network.SequentialID;
import org.apache.inlong.sdk.dataproxy.threads.IndexCollectThread;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class DefaultMessageSender implements MessageSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMessageSender.class);
    private static final ConcurrentHashMap<Integer, DefaultMessageSender> CACHE_SENDER =
            new ConcurrentHashMap<>();
    private static final AtomicBoolean MANAGER_FETCHER_THREAD_STARTED = new AtomicBoolean(false);
    private static final SequentialID idGenerator = new SequentialID();
    private final Sender sender;
    private final IndexCollectThread indexCol;
    /* Store index <groupId_streamId,cnt> */
    private final Map<String, Long> storeIndex = new ConcurrentHashMap<String, Long>();
    private String groupId;
    private int msgtype = SdkConsts.MSG_TYPE;
    private boolean isCompress = true;
    private boolean isGroupIdTransfer = false;
    private boolean isReport = false;
    private boolean isSupportLF = false;
    private int maxPacketLength = -1;
    private int cpsSize = SdkConsts.COMPRESS_SIZE;
    private final int senderMaxAttempt;

    public DefaultMessageSender(TcpMsgSenderConfig configure) throws Exception {
        this(configure, null);
    }

    public DefaultMessageSender(TcpMsgSenderConfig configure, ThreadFactory selfDefineFactory) throws Exception {
        ProxyUtils.validClientConfig(configure);
        sender = new Sender(configure, selfDefineFactory);
        sender.start();
        groupId = configure.getInlongGroupId();
        indexCol = new IndexCollectThread(storeIndex);
        senderMaxAttempt = configure.getMaxSyncSendAttempt();
        indexCol.start();

    }

    /**
     * generate by cluster id
     *
     * @param tcpConfig - sender
     * @return - sender
     */
    public static DefaultMessageSender generateSenderByClusterId(
            TcpMsgSenderConfig tcpConfig) throws Exception {

        return generateSenderByClusterId(tcpConfig, null);
    }

    /**
     * generate by cluster id
     *
     * @param tcpConfig - sender
     * @param selfDefineFactory - sender factory
     * @return - sender
     */
    public static DefaultMessageSender generateSenderByClusterId(TcpMsgSenderConfig tcpConfig,
            ThreadFactory selfDefineFactory) throws Exception {
        LOGGER.info("Initial tcp sender, configure is {}", tcpConfig);
        // initial sender object
        ProcessResult procResult = new ProcessResult();
        ProxyConfigManager proxyConfigManager = new ProxyConfigManager(tcpConfig);
        if (!proxyConfigManager.getGroupIdConfigure(true, procResult)) {
            throw new Exception(procResult.toString());
        }
        ProxyConfigEntry configEntry = (ProxyConfigEntry) procResult.getRetData();
        DefaultMessageSender sender = CACHE_SENDER.get(configEntry.getClusterId());
        if (sender != null) {
            return sender;
        } else {
            DefaultMessageSender tmpMessageSender =
                    new DefaultMessageSender(tcpConfig, selfDefineFactory);
            tmpMessageSender.setMaxPacketLength(configEntry.getMaxPacketLength());
            CACHE_SENDER.put(configEntry.getClusterId(), tmpMessageSender);
            return tmpMessageSender;
        }
    }

    /**
     * finally clean up
     */
    public static void finallyCleanup() {
        for (DefaultMessageSender sender : CACHE_SENDER.values()) {
            sender.close();
        }
        CACHE_SENDER.clear();
    }

    @Override
    public void close() {
        LOGGER.info("ready to close resources, may need five minutes !");
        if (sender.getClusterId() != -1) {
            CACHE_SENDER.remove(sender.getClusterId());
        }
        sender.close();
        shutdownInternalThreads();
    }

    public TcpMsgSenderConfig getProxyClientConfig() {
        return sender.getTcpConfig();
    }

    public boolean isSupportLF() {
        return isSupportLF;
    }

    public void setSupportLF(boolean supportLF) {
        isSupportLF = supportLF;
    }

    public boolean isGroupIdTransfer() {
        return isGroupIdTransfer;
    }

    public void setGroupIdTransfer(boolean isGroupIdTransfer) {
        this.isGroupIdTransfer = isGroupIdTransfer;
    }

    public boolean isReport() {
        return isReport;
    }

    public void setReport(boolean isReport) {
        this.isReport = isReport;
    }

    public int getCpsSize() {
        return cpsSize;
    }

    public void setCpsSize(int cpsSize) {
        this.cpsSize = cpsSize;
    }

    public int getMsgtype() {
        return msgtype;
    }

    public void setMsgtype(int msgtype) {
        this.msgtype = msgtype;
    }

    public boolean isCompress() {
        return isCompress;
    }

    public void setCompress(boolean isCompress) {
        this.isCompress = isCompress;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public int getMaxPacketLength() {
        return maxPacketLength;
    }

    public void setMaxPacketLength(int maxPacketLength) {
        this.maxPacketLength = maxPacketLength;
    }

    public String getSDKVersion() {
        return ProxyUtils.getJarVersion();
    }

    private SendResult attemptSendMessage(Function<Sender, SendResult> sendOperation) {
        int attempts = 0;
        SendResult sendResult = null;
        while (attempts < this.senderMaxAttempt) {
            sendResult = sendOperation.apply(sender);
            if (sendResult != null && sendResult.equals(SendResult.OK)) {
                return sendResult;
            }
            attempts++;
        }
        return sendResult;
    }

    private String attemptSendMessageIndex(Function<Sender, String> sendOperation) {
        int attempts = 0;
        String sendIndexResult = null;
        while (attempts < this.senderMaxAttempt) {
            sendIndexResult = sendOperation.apply(sender);
            if (sendIndexResult != null && sendIndexResult.startsWith(SendResult.OK.toString())) {
                return sendIndexResult;
            }
            attempts++;
        }
        return sendIndexResult;
    }

    public SendResult sendMessage(byte[] body, String groupId, String streamId, long dt, String msgUUID) {
        return sendMessage(body, groupId, streamId, dt, msgUUID, false);
    }

    /**
     * ync send single message
     *
     * @param body message data
     * @param groupId groupId
     * @param streamId streamId
     * @param dt data report timestamp
     * @param msgUUID msg uuid
     * @param isProxySend true: dataproxy doesn't return response message until data is sent to MQ
     * @return SendResult.OK means success
     */
    public SendResult sendMessage(byte[] body, String groupId,
            String streamId, long dt, String msgUUID, boolean isProxySend) {
        dt = ProxyUtils.covertZeroDt(dt);
        if (!ProxyUtils.isBodyValid(body) || !ProxyUtils.isDtValid(dt)) {
            return SendResult.INVALID_ATTRIBUTES;
        }
        if (!ProxyUtils.isBodyLengthValid(body, maxPacketLength)) {
            return SendResult.BODY_EXCEED_MAX_LEN;
        }
        addIndexCnt(groupId, streamId, 1);

        String proxySend = "";
        if (isProxySend) {
            proxySend = AttributeConstants.MESSAGE_PROXY_SEND + "=true";
        }

        boolean isCompressEnd = (isCompress && (body.length > cpsSize));

        if (msgtype == 7 || msgtype == 8) {
            EncodeObject encodeObject =
                    new EncodeObject(Collections.singletonList(body), msgtype, isCompressEnd, isReport,
                            isGroupIdTransfer, dt / 1000, idGenerator.getNextInt(), groupId, streamId, proxySend);
            encodeObject.setSupportLF(isSupportLF);
            Function<Sender, SendResult> sendOperation = (sender) -> sender.syncSendMessage(encodeObject, msgUUID);
            return attemptSendMessage(sendOperation);
        } else if (msgtype == 3 || msgtype == 5) {
            if (isProxySend) {
                proxySend = "&" + proxySend;
            }
            final String finalProxySend = proxySend;
            final long finalDt = dt;
            Function<Sender, SendResult> sendOperation;
            if (isCompressEnd) {
                sendOperation = (sender) -> sender.syncSendMessage(new EncodeObject(Collections.singletonList(body),
                        "groupId=" + groupId + "&streamId=" + streamId + "&dt=" + finalDt + "&cp=snappy"
                                + finalProxySend,
                        idGenerator.getNextId(), this.getMsgtype(),
                        true, groupId), msgUUID);
            } else {
                sendOperation = (sender) -> sender.syncSendMessage(new EncodeObject(Collections.singletonList(body),
                        "groupId=" + groupId + "&streamId=" + streamId + "&dt=" + finalDt
                                + finalProxySend,
                        idGenerator.getNextId(), this.getMsgtype(),
                        false, groupId), msgUUID);

            }
            return attemptSendMessage(sendOperation);
        }

        return null;
    }

    public SendResult sendMessage(byte[] body, String groupId, String streamId, long dt, String msgUUID,
            Map<String, String> extraAttrMap) {
        return sendMessage(body, groupId, streamId, dt, msgUUID, extraAttrMap, false);
    }

    /**
     * sync send single message
     *
     * @param body message data
     * @param groupId groupId
     * @param streamId streamId
     * @param dt data report timestamp
     * @param msgUUID msg uuid
     * @param extraAttrMap extra attributes
     * @param isProxySend true: dataproxy doesn't return response message until data is sent to MQ
     * @return SendResult.OK means success
     */
    public SendResult sendMessage(byte[] body, String groupId, String streamId, long dt, String msgUUID,
            Map<String, String> extraAttrMap, boolean isProxySend) {

        dt = ProxyUtils.covertZeroDt(dt);
        if (!ProxyUtils.isBodyValid(body) || !ProxyUtils.isDtValid(dt) || !ProxyUtils.isAttrKeysValid(extraAttrMap)) {
            return SendResult.INVALID_ATTRIBUTES;
        }
        if (!ProxyUtils.isBodyLengthValid(body, maxPacketLength)) {
            return SendResult.BODY_EXCEED_MAX_LEN;
        }
        addIndexCnt(groupId, streamId, 1);

        if (isProxySend) {
            extraAttrMap.put(AttributeConstants.MESSAGE_PROXY_SEND, "true");
        }
        StringBuilder attrs = MessageUtils.convertAttrToStr(extraAttrMap);

        boolean isCompressEnd = (isCompress && (body.length > cpsSize));

        if (msgtype == 7 || msgtype == 8) {
            EncodeObject encodeObject =
                    new EncodeObject(Collections.singletonList(body), msgtype, isCompressEnd, isReport,
                            isGroupIdTransfer, dt / 1000,
                            idGenerator.getNextInt(), groupId, streamId, attrs.toString());
            encodeObject.setSupportLF(isSupportLF);
            Function<Sender, SendResult> sendOperation = (sender) -> sender.syncSendMessage(encodeObject, msgUUID);
            return attemptSendMessage(sendOperation);
        } else if (msgtype == 3 || msgtype == 5) {
            attrs.append("&groupId=").append(groupId).append("&streamId=").append(streamId).append("&dt=").append(dt);
            if (isCompressEnd) {
                attrs.append("&cp=snappy");
                Function<Sender, SendResult> sendOperation = (sender) -> sender.syncSendMessage(
                        new EncodeObject(Collections.singletonList(body),
                                attrs.toString(), idGenerator.getNextId(), this.getMsgtype(), true, groupId),
                        msgUUID);
                return attemptSendMessage(sendOperation);
            } else {
                Function<Sender, SendResult> sendOperation = (sender) -> sender.syncSendMessage(
                        new EncodeObject(Collections.singletonList(body),
                                attrs.toString(), idGenerator.getNextId(),
                                this.getMsgtype(), false, groupId),
                        msgUUID);
                return attemptSendMessage(sendOperation);
            }
        }
        return null;

    }

    public SendResult sendMessage(List<byte[]> bodyList, String groupId, String streamId, long dt, String msgUUID) {
        return sendMessage(bodyList, groupId, streamId, dt, msgUUID, false);
    }

    /**
     * sync send a batch of messages
     *
     * @param bodyList list of messages
     * @param groupId groupId
     * @param streamId streamId
     * @param dt data report timestamp
     * @param msgUUID msg uuid
     * @param isProxySend true: dataproxy doesn't return response message until data is sent to MQ
     * @return SendResult.OK means success
     */
    public SendResult sendMessage(List<byte[]> bodyList,
            String groupId, String streamId, long dt, String msgUUID, boolean isProxySend) {
        dt = ProxyUtils.covertZeroDt(dt);
        if (!ProxyUtils.isBodyValid(bodyList) || !ProxyUtils.isDtValid(dt)) {
            return SendResult.INVALID_ATTRIBUTES;
        }
        if (!ProxyUtils.isBodyLengthValid(bodyList, maxPacketLength)) {
            return SendResult.BODY_EXCEED_MAX_LEN;
        }
        addIndexCnt(groupId, streamId, bodyList.size());

        String proxySend = "";
        if (isProxySend) {
            proxySend = AttributeConstants.MESSAGE_SYNC_SEND + "=true";
        }

        if (msgtype == 7 || msgtype == 8) {
            EncodeObject encodeObject = new EncodeObject(bodyList, msgtype, isCompress, isReport,
                    isGroupIdTransfer, dt / 1000,
                    idGenerator.getNextInt(), groupId, streamId, proxySend);
            encodeObject.setSupportLF(isSupportLF);
            Function<Sender, SendResult> sendOperation = (sender) -> sender.syncSendMessage(encodeObject, msgUUID);
            return attemptSendMessage(sendOperation);
        } else if (msgtype == 3 || msgtype == 5) {
            if (isProxySend) {
                proxySend = "&" + proxySend;
            }
            final long finalDt = dt;
            final String finalProxySend = proxySend;
            Function<Sender, SendResult> sendOperation;
            if (isCompress) {
                sendOperation = (sender) -> sender.syncSendMessage(new EncodeObject(bodyList,
                        "groupId=" + groupId + "&streamId=" + streamId + "&dt=" + finalDt + "&cp=snappy" + "&cnt="
                                + bodyList.size() + finalProxySend,
                        idGenerator.getNextId(), this.getMsgtype(), true, groupId), msgUUID);
            } else {
                sendOperation = (sender) -> sender.syncSendMessage(new EncodeObject(bodyList,
                        "groupId=" + groupId + "&streamId=" + streamId + "&dt=" + finalDt + "&cnt=" + bodyList.size()
                                + finalProxySend,
                        idGenerator.getNextId(), this.getMsgtype(), false, groupId), msgUUID);
            }
            return attemptSendMessage(sendOperation);
        }
        return null;
    }

    public SendResult sendMessage(List<byte[]> bodyList, String groupId, String streamId, long dt,
            String msgUUID, Map<String, String> extraAttrMap) {
        return sendMessage(bodyList, groupId, streamId, dt, msgUUID, extraAttrMap, false);
    }

    @Override
    public void asyncSendMessage(SendMessageCallback callback, byte[] body, String groupId, String streamId, long dt,
            String msgUUID, Map<String, String> extraAttrMap) throws ProxySdkException {
        asyncSendMessage(callback, body, groupId, streamId, dt, msgUUID, extraAttrMap, false);
    }

    @Override
    public void asyncSendMessage(SendMessageCallback callback, byte[] body, String groupId, String streamId, long dt,
            String msgUUID) throws ProxySdkException {
        asyncSendMessage(callback, body, groupId, streamId, dt, msgUUID, false);
    }

    @Override
    public void asyncSendMessage(SendMessageCallback callback, List<byte[]> bodyList, String groupId, String streamId,
            long dt, String msgUUID) throws ProxySdkException {
        asyncSendMessage(callback, bodyList, groupId, streamId, dt, msgUUID, false);
    }

    @Override
    public void asyncSendMessage(SendMessageCallback callback, List<byte[]> bodyList, String groupId, String streamId,
            long dt, String msgUUID, Map<String, String> extraAttrMap) throws ProxySdkException {
        asyncSendMessage(callback, bodyList, groupId, streamId, dt, msgUUID, extraAttrMap, false);
    }

    /**
     * sync send a batch of messages
     *
     * @param bodyList list of messages
     * @param groupId groupId
     * @param streamId streamId
     * @param dt data report timestamp
     * @param msgUUID msg uuid
     * @param extraAttrMap extra attributes
     * @param isProxySend true: dataproxy doesn't return response message until data is sent to MQ
     * @return SendResult.OK means success
     */
    public SendResult sendMessage(List<byte[]> bodyList, String groupId, String streamId, long dt,
            String msgUUID, Map<String, String> extraAttrMap, boolean isProxySend) {
        dt = ProxyUtils.covertZeroDt(dt);
        if (!ProxyUtils.isBodyValid(bodyList) || !ProxyUtils.isDtValid(dt) || !ProxyUtils.isAttrKeysValid(
                extraAttrMap)) {
            return SendResult.INVALID_ATTRIBUTES;
        }
        if (!ProxyUtils.isBodyLengthValid(bodyList, maxPacketLength)) {
            return SendResult.BODY_EXCEED_MAX_LEN;
        }
        addIndexCnt(groupId, streamId, bodyList.size());
        if (isProxySend) {
            extraAttrMap.put(AttributeConstants.MESSAGE_PROXY_SEND, "true");
        }
        StringBuilder attrs = MessageUtils.convertAttrToStr(extraAttrMap);

        if (msgtype == 7 || msgtype == 8) {
            EncodeObject encodeObject = new EncodeObject(bodyList, msgtype, isCompress, isReport,
                    isGroupIdTransfer, dt / 1000,
                    idGenerator.getNextInt(), groupId, streamId, attrs.toString());
            encodeObject.setSupportLF(isSupportLF);
            Function<Sender, SendResult> sendOperation = (sender) -> sender.syncSendMessage(encodeObject, msgUUID);
            return attemptSendMessage(sendOperation);
        } else if (msgtype == 3 || msgtype == 5) {
            attrs.append("&groupId=").append(groupId).append("&streamId=").append(streamId)
                    .append("&dt=").append(dt).append("&cnt=").append(bodyList.size());
            if (isCompress) {
                attrs.append("&cp=snappy");
                Function<Sender, SendResult> sendOperation =
                        (sender) -> sender.syncSendMessage(new EncodeObject(bodyList, attrs.toString(),
                                idGenerator.getNextId(), this.getMsgtype(), true, groupId), msgUUID);
                return attemptSendMessage(sendOperation);
            } else {
                Function<Sender, SendResult> sendOperation =
                        (sender) -> sender.syncSendMessage(new EncodeObject(bodyList, attrs.toString(),
                                idGenerator.getNextId(), this.getMsgtype(), false, groupId), msgUUID);
                return attemptSendMessage(sendOperation);
            }
        }
        return null;
    }

    /**
     * async send single message
     *
     * @param callback callback can be null
     * @param body message data
     * @param groupId groupId
     * @param streamId streamId
     * @param dt data report timestamp
     * @param msgUUID msg uuid
     * @param isProxySend true: dataproxy doesn't return response message until data is sent to MQ
     * @throws ProxySdkException
     */
    public void asyncSendMessage(SendMessageCallback callback, byte[] body, String groupId,
            String streamId, long dt, String msgUUID, boolean isProxySend) throws ProxySdkException {
        dt = ProxyUtils.covertZeroDt(dt);
        if (!ProxyUtils.isBodyValid(body) || !ProxyUtils.isDtValid(dt)) {
            throw new ProxySdkException(SendResult.INVALID_ATTRIBUTES.toString());
        }
        if (!ProxyUtils.isBodyLengthValid(body, maxPacketLength)) {
            throw new ProxySdkException(SendResult.BODY_EXCEED_MAX_LEN.toString());
        }
        addIndexCnt(groupId, streamId, 1);

        String proxySend = "";
        if (isProxySend) {
            proxySend = AttributeConstants.MESSAGE_PROXY_SEND + "=true";
        }
        boolean isCompressEnd = (isCompress && (body.length > cpsSize));
        if (msgtype == 7 || msgtype == 8) {
            EncodeObject encodeObject =
                    new EncodeObject(Collections.singletonList(body), this.getMsgtype(), isCompressEnd, isReport,
                            isGroupIdTransfer, dt / 1000, idGenerator.getNextInt(),
                            groupId, streamId, proxySend);
            encodeObject.setSupportLF(isSupportLF);
            sender.asyncSendMessage(encodeObject, callback, msgUUID);
        } else if (msgtype == 3 || msgtype == 5) {
            if (isCompressEnd) {
                if (isProxySend) {
                    proxySend = "&" + proxySend;
                }
                sender.asyncSendMessage(new EncodeObject(Collections.singletonList(body), "groupId="
                        + groupId + "&streamId=" + streamId + "&dt=" + dt + "&cp=snappy" + proxySend,
                        idGenerator.getNextId(), this.getMsgtype(), true, groupId),
                        callback, msgUUID);
            } else {
                sender.asyncSendMessage(
                        new EncodeObject(Collections.singletonList(body), "groupId=" + groupId + "&streamId="
                                + streamId + "&dt=" + dt + proxySend, idGenerator.getNextId(),
                                this.getMsgtype(), false, groupId),
                        callback, msgUUID);
            }
        }
    }

    /**
     * async send single message
     *
     * @param callback callback can be null
     * @param body message data
     * @param groupId groupId
     * @param streamId streamId
     * @param dt data report timestamp
     * @param msgUUID msg uuid
     * @param extraAttrMap extra attributes
     * @param isProxySend true: dataproxy doesn't return response message until data is sent to MQ
     * @throws ProxySdkException
     */
    public void asyncSendMessage(SendMessageCallback callback, byte[] body, String groupId, String streamId, long dt,
            String msgUUID, Map<String, String> extraAttrMap, boolean isProxySend) throws ProxySdkException {
        dt = ProxyUtils.covertZeroDt(dt);
        if (!ProxyUtils.isBodyValid(body) || !ProxyUtils.isDtValid(dt) || !ProxyUtils.isAttrKeysValid(extraAttrMap)) {
            throw new ProxySdkException(SendResult.INVALID_ATTRIBUTES.toString());
        }
        if (!ProxyUtils.isBodyLengthValid(body, maxPacketLength)) {
            throw new ProxySdkException(SendResult.BODY_EXCEED_MAX_LEN.toString());
        }
        addIndexCnt(groupId, streamId, 1);
        if (isProxySend) {
            extraAttrMap.put(AttributeConstants.MESSAGE_PROXY_SEND, "true");
        }
        StringBuilder attrs = MessageUtils.convertAttrToStr(extraAttrMap);

        boolean isCompressEnd = (isCompress && (body.length > cpsSize));
        if (msgtype == 7 || msgtype == 8) {
            EncodeObject encodeObject =
                    new EncodeObject(Collections.singletonList(body), this.getMsgtype(), isCompressEnd,
                            isReport, isGroupIdTransfer, dt / 1000, idGenerator.getNextInt(),
                            groupId, streamId, attrs.toString());
            encodeObject.setSupportLF(isSupportLF);
            sender.asyncSendMessage(encodeObject, callback, msgUUID);
        } else if (msgtype == 3 || msgtype == 5) {
            attrs.append("&groupId=").append(groupId).append("&streamId=").append(streamId).append("&dt=").append(dt);
            if (isCompressEnd) {
                attrs.append("&cp=snappy");
                sender.asyncSendMessage(new EncodeObject(Collections.singletonList(body), attrs.toString(),
                        idGenerator.getNextId(), this.getMsgtype(), true, groupId),
                        callback, msgUUID);
            } else {
                sender.asyncSendMessage(
                        new EncodeObject(Collections.singletonList(body), attrs.toString(), idGenerator.getNextId(),
                                this.getMsgtype(), false, groupId),
                        callback, msgUUID);
            }
        }
    }

    /**
     * async send a batch of messages
     *
     * @param callback callback can be null
     * @param bodyList list of messages
     * @param groupId groupId
     * @param streamId streamId
     * @param dt data report time
     * @param msgUUID msg uuid
     * @param isProxySend true: dataproxy doesn't return response message until data is sent to MQ
     * @throws ProxySdkException
     */
    public void asyncSendMessage(SendMessageCallback callback, List<byte[]> bodyList,
            String groupId, String streamId, long dt, String msgUUID, boolean isProxySend) throws ProxySdkException {
        dt = ProxyUtils.covertZeroDt(dt);
        if (!ProxyUtils.isBodyValid(bodyList) || !ProxyUtils.isDtValid(dt)) {
            throw new ProxySdkException(SendResult.INVALID_ATTRIBUTES.toString());
        }
        if (!ProxyUtils.isBodyLengthValid(bodyList, maxPacketLength)) {
            throw new ProxySdkException(SendResult.BODY_EXCEED_MAX_LEN.toString());
        }
        addIndexCnt(groupId, streamId, bodyList.size());
        String proxySend = "";
        if (isProxySend) {
            proxySend = AttributeConstants.MESSAGE_PROXY_SEND + "=true";
        }
        if (msgtype == 7 || msgtype == 8) {
            EncodeObject encodeObject = new EncodeObject(bodyList, this.getMsgtype(), isCompress,
                    isReport, isGroupIdTransfer, dt / 1000, idGenerator.getNextInt(),
                    groupId, streamId, proxySend);
            encodeObject.setSupportLF(isSupportLF);
            sender.asyncSendMessage(encodeObject, callback, msgUUID);
        } else if (msgtype == 3 || msgtype == 5) {
            if (isProxySend) {
                proxySend = "&" + proxySend;
            }
            if (isCompress) {
                sender.asyncSendMessage(
                        new EncodeObject(bodyList, "groupId=" + groupId + "&streamId=" + streamId
                                + "&dt=" + dt + "&cp=snappy" + "&cnt=" + bodyList.size() + proxySend,
                                idGenerator.getNextId(),
                                this.getMsgtype(), true, groupId),
                        callback, msgUUID);
            } else {
                sender.asyncSendMessage(
                        new EncodeObject(bodyList,
                                "groupId=" + groupId + "&streamId=" + streamId + "&dt=" + dt + "&cnt=" + bodyList.size()
                                        + proxySend,
                                idGenerator.getNextId(), this.getMsgtype(), false, groupId),
                        callback, msgUUID);
            }
        }
    }

    /**
     * async send a batch of messages
     *
     * @param callback callback can be null
     * @param bodyList list of messages
     * @param groupId groupId
     * @param streamId streamId
     * @param dt data report time
     * @param msgUUID msg uuid
     * @param extraAttrMap extra attributes
     * @param isProxySend true: dataproxy doesn't return response message until data is sent to MQ
     * @throws ProxySdkException
     */
    public void asyncSendMessage(SendMessageCallback callback,
            List<byte[]> bodyList, String groupId, String streamId, long dt, String msgUUID,
            Map<String, String> extraAttrMap, boolean isProxySend) throws ProxySdkException {
        dt = ProxyUtils.covertZeroDt(dt);
        if (!ProxyUtils.isBodyValid(bodyList) || !ProxyUtils.isDtValid(dt) || !ProxyUtils.isAttrKeysValid(
                extraAttrMap)) {
            throw new ProxySdkException(SendResult.INVALID_ATTRIBUTES.toString());
        }
        if (!ProxyUtils.isBodyLengthValid(bodyList, maxPacketLength)) {
            throw new ProxySdkException(SendResult.BODY_EXCEED_MAX_LEN.toString());
        }
        addIndexCnt(groupId, streamId, bodyList.size());
        if (isProxySend) {
            extraAttrMap.put(AttributeConstants.MESSAGE_PROXY_SEND, "true");
        }
        StringBuilder attrs = MessageUtils.convertAttrToStr(extraAttrMap);

        if (msgtype == 7 || msgtype == 8) {
            // if (!isGroupIdTransfer)
            EncodeObject encodeObject = new EncodeObject(bodyList, this.getMsgtype(),
                    isCompress, isReport, isGroupIdTransfer, dt / 1000, idGenerator.getNextInt(),
                    groupId, streamId, attrs.toString());
            encodeObject.setSupportLF(isSupportLF);
            sender.asyncSendMessage(encodeObject, callback, msgUUID);
        } else if (msgtype == 3 || msgtype == 5) {
            attrs.append("&groupId=").append(groupId).append("&streamId=").append(streamId)
                    .append("&dt=").append(dt).append("&cnt=").append(bodyList.size());
            if (isCompress) {
                attrs.append("&cp=snappy");
                sender.asyncSendMessage(new EncodeObject(bodyList, attrs.toString(), idGenerator.getNextId(),
                        this.getMsgtype(), true, groupId), callback, msgUUID);
            } else {
                sender.asyncSendMessage(new EncodeObject(bodyList, attrs.toString(), idGenerator.getNextId(),
                        this.getMsgtype(), false, groupId), callback, msgUUID);
            }
        }

    }

    /**
     * asyncSendMessage
     *
     * @param inlongGroupId
     * @param inlongStreamId
     * @param body
     * @param callback
     * @throws ProxySdkException
     */
    @Override
    public void asyncSendMessage(String inlongGroupId, String inlongStreamId, byte[] body, SendMessageCallback callback)
            throws ProxySdkException {
        this.asyncSendMessage(callback, body, inlongGroupId,
                inlongStreamId, System.currentTimeMillis(), idGenerator.getNextId());
    }

    /**
     * async send single message
     *
     * @param inlongGroupId groupId
     * @param inlongStreamId streamId
     * @param body a single message
     * @param callback callback can be null
     * @param isProxySend true: dataproxy doesn't return response message until data is sent to MQ
     * @throws ProxySdkException
     */
    public void asyncSendMessage(String inlongGroupId, String inlongStreamId, byte[] body, SendMessageCallback callback,
            boolean isProxySend) throws ProxySdkException {
        this.asyncSendMessage(callback, body, inlongGroupId,
                inlongStreamId, System.currentTimeMillis(), idGenerator.getNextId(), isProxySend);
    }

    /**
     * async send a batch of messages
     *
     * @param inlongGroupId groupId
     * @param inlongStreamId streamId
     * @param bodyList list of messages
     * @param callback callback can be null
     * @throws ProxySdkException
     */
    @Override
    public void asyncSendMessage(String inlongGroupId, String inlongStreamId, List<byte[]> bodyList,
            SendMessageCallback callback) throws ProxySdkException {
        this.asyncSendMessage(callback, bodyList, inlongGroupId,
                inlongStreamId, System.currentTimeMillis(), idGenerator.getNextId());
    }

    /**
     * async send a batch of messages
     *
     * @param inlongGroupId groupId
     * @param inlongStreamId streamId
     * @param bodyList list of messages
     * @param callback callback can be null
     * @param isProxySend true: dataproxy doesn't return response message until data is sent to MQ
     * @throws ProxySdkException
     */
    public void asyncSendMessage(String inlongGroupId, String inlongStreamId, List<byte[]> bodyList,
            SendMessageCallback callback, boolean isProxySend) throws ProxySdkException {
        this.asyncSendMessage(callback, bodyList, inlongGroupId,
                inlongStreamId, System.currentTimeMillis(), idGenerator.getNextId(), isProxySend);
    }

    private void addIndexCnt(String groupId, String streamId, long cnt) {
        try {
            String key = groupId + "|" + streamId;
            if (storeIndex.containsKey(key)) {
                long sum = storeIndex.get(key);
                storeIndex.put(key, sum + cnt);
            } else {
                storeIndex.put(key, cnt);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

    private void shutdownInternalThreads() {
        indexCol.shutDown();
        MANAGER_FETCHER_THREAD_STARTED.set(false);
    }
}
