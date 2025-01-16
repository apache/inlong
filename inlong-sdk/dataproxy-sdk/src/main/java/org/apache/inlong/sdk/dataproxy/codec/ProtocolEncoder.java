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

package org.apache.inlong.sdk.dataproxy.codec;

import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.sdk.dataproxy.config.EncryptConfigEntry;
import org.apache.inlong.sdk.dataproxy.config.EncryptInfo;
import org.apache.inlong.sdk.dataproxy.utils.AuthzUtils;
import org.apache.inlong.sdk.dataproxy.utils.EncryptUtil;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.List;

import static org.apache.inlong.sdk.dataproxy.ConfigConstants.FLAG_ALLOW_AUTH;
import static org.apache.inlong.sdk.dataproxy.ConfigConstants.FLAG_ALLOW_COMPRESS;
import static org.apache.inlong.sdk.dataproxy.ConfigConstants.FLAG_ALLOW_ENCRYPT;

public class ProtocolEncoder extends MessageToMessageEncoder<EncodeObject> {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolEncoder.class);
    private static final LogCounter exptCounter = new LogCounter(10, 100000, 60 * 1000L);

    protected void encode(ChannelHandlerContext ctx,
            EncodeObject message, List<Object> out) throws Exception {
        ByteBuf buf = null;
        try {
            if (message.getMsgtype() == 3) {
                buf = writeToBuf3(message);
            } else if (message.getMsgtype() == 5) {
                buf = writeToBuf5(message);
            } else if (message.getMsgtype() == 7) {
                buf = writeToBuf7(message);
            } else if (message.getMsgtype() == 8) {
                buf = writeToBuf8(message);
            }
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.error("ProtocolEncoder encode message failure", ex);
            }
        }
        if (buf != null) {
            out.add(buf);
        } else {
            logger.warn("write buf is null !");
        }
    }

    private ByteBuf writeToBuf8(EncodeObject object) {
        ByteBuf buf = null;
        try {
            String endAttr = object.getCommonattr();
            if (object.isAuth()) {
                if (StringUtils.isNotBlank(endAttr)) {
                    endAttr = endAttr + "&";
                }
                long timestamp = System.currentTimeMillis();
                int nonce = new SecureRandom(String.valueOf(timestamp).getBytes()).nextInt(Integer.MAX_VALUE);
                endAttr = endAttr + "_userName=" + object.getUserName() + "&_clientIP=" + ProxyUtils.getLocalIp()
                        + "&_signature=" + AuthzUtils.generateSignature(object.getUserName(),
                                timestamp, nonce, object.getSecretKey())
                        + "&_timeStamp=" + timestamp + "&_nonce=" + nonce;
            }
            if (StringUtils.isNotBlank(object.getMsgUUID())) {
                if (StringUtils.isNotBlank(endAttr)) {
                    endAttr = endAttr + "&";
                }
                endAttr = endAttr + "msgUUID=" + object.getMsgUUID();
            }
            int msgType = 8;
            if (object.isAuth()) {
                msgType |= FLAG_ALLOW_AUTH;
            }
            byte[] attrData = endAttr.getBytes(StandardCharsets.UTF_8);
            int totalLength = 1 + 4 + 1 + 4 + 2 + attrData.length + 2;
            buf = ByteBufAllocator.DEFAULT.buffer(4 + totalLength);
            buf.writeInt(totalLength);
            buf.writeByte(msgType);
            buf.writeInt((int) object.getDt());
            buf.writeByte(1);
            buf.writeInt(0);
            buf.writeShort(attrData.length);
            if (attrData.length > 0) {
                buf.writeBytes(attrData);
            }
            buf.writeShort(0xee01);
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.error("Write type8 data exception", ex);
            }
        }
        return buf;
    }

    private ByteBuf constructBody(byte[] body, EncodeObject object,
            int totalLength, int cnt) throws UnsupportedEncodingException {
        ByteBuf buf = null;
        if (body != null) {
            if (object.isCompress()) {
                body = processCompress(body);
            }
            String endAttr = object.getCommonattr();
            if (object.isEncrypt()) {
                EncryptConfigEntry encryptEntry = object.getEncryptEntry();
                if (encryptEntry != null) {
                    if (StringUtils.isNotBlank(endAttr)) {
                        endAttr = endAttr + "&";
                    }
                    EncryptInfo encryptInfo = encryptEntry.getRsaEncryptInfo();
                    endAttr = endAttr + "_userName=" + object.getUserName()
                            + "&_encyVersion=" + encryptInfo.getVersion()
                            + "&_encyAesKey=" + encryptInfo.getRsaEncryptedKey();
                    body = EncryptUtil.aesEncrypt(body, encryptInfo.getAesKey());
                }
            }
            if (!object.isGroupIdTransfer()) {
                if (StringUtils.isNotBlank(endAttr)) {
                    endAttr = endAttr + "&";
                }
                endAttr = (endAttr + "groupId=" + object.getGroupId() + "&streamId=" + object.getStreamId());
            }
            if (StringUtils.isNotBlank(object.getMsgUUID())) {
                if (StringUtils.isNotBlank(endAttr)) {
                    endAttr = endAttr + "&";
                }
                endAttr = endAttr + "msgUUID=" + object.getMsgUUID();
            }

            int msgType = 7;
            if (object.isEncrypt()) {
                msgType |= FLAG_ALLOW_ENCRYPT;
            }
            if (object.isCompress()) {
                msgType |= FLAG_ALLOW_COMPRESS;
            }
            byte[] attrData = endAttr.getBytes(StandardCharsets.UTF_8);
            totalLength = totalLength + body.length + attrData.length;
            buf = ByteBufAllocator.DEFAULT.buffer(4 + totalLength);
            buf.writeInt(totalLength);
            buf.writeByte(msgType);
            buf.writeShort(object.getGroupIdNum());
            buf.writeShort(object.getStreamIdNum());
            String bitStr = object.isSupportLF() ? "1" : "0";
            bitStr += (object.getMessageKey().equals("minute")) ? "1" : "0";
            bitStr += (object.getMessageKey().equals("file")) ? "1" : "0";
            bitStr += !object.isGroupIdTransfer() ? "1" : "0";
            bitStr += object.isReport() ? "1" : "0";
            bitStr += "0";
            buf.writeShort(Integer.parseInt(bitStr, 2));
            buf.writeInt((int) object.getDt());

            buf.writeShort(cnt);
            buf.writeInt(Integer.parseInt(object.getMessageId()));

            buf.writeInt(body.length);
            buf.writeBytes(body);

            buf.writeShort(attrData.length);
            buf.writeBytes(attrData);
            buf.writeShort(0xee01);
        }
        return buf;
    }

    private ByteBuf writeToBuf7(EncodeObject object) {
        ByteBuf buf = null;
        try {
            int totalLength = 1 + 2 + 2 + 2 + 4 + 2 + 4 + 4 + 2 + 2;
            byte[] body = null;
            int cnt = 1;

            if (object.getBodylist() != null && !object.getBodylist().isEmpty()) {
                if (object.getCnt() > 0) {
                    cnt = object.getCnt();
                } else {
                    cnt = object.getBodylist().size();
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                if (object.isSupportLF()) {
                    int totalCnt = 0;
                    ByteArrayOutputStream data = new ByteArrayOutputStream();
                    for (byte[] entry : object.getBodylist()) {
                        if (totalCnt++ > 0) {
                            data.write(AttributeConstants.LINE_FEED_SEP.getBytes(StandardCharsets.UTF_8));
                        }
                        data.write(entry);
                    }
                    ByteBuffer dataBuffer = ByteBuffer.allocate(4);
                    dataBuffer.putInt(data.toByteArray().length);
                    out.write(dataBuffer.array());
                    out.write(data.toByteArray());
                } else {
                    for (byte[] entry : object.getBodylist()) {
                        ByteBuffer dataBuffer = ByteBuffer.allocate(4);
                        dataBuffer.putInt(entry.length);
                        out.write(dataBuffer.array());
                        out.write(entry);
                    }
                }
                body = out.toByteArray();
            }
            buf = constructBody(body, object, totalLength, cnt);
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.error("Write type7 data exception", ex);
            }
        }
        return buf;
    }

    private ByteBuf writeToBuf5(EncodeObject object) {
        ByteBuf buf = null;
        try {
            int totalLength = 1 + 4 + 4;
            byte[] body = null;

            // send multiple messages one time
            if (object.getBodylist() != null && !object.getBodylist().isEmpty()) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                for (byte[] entry : object.getBodylist()) {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                    byteBuffer.putInt(entry.length);
                    out.write(byteBuffer.array());
                    out.write(entry);
                }
                body = out.toByteArray();
            }
            if (body != null) {
                String msgAttrs = object.getAttributes();
                if (object.isCompress()) {
                    body = processCompress(body);
                }
                if (object.isEncrypt()) {
                    EncryptConfigEntry encryptEntry = object.getEncryptEntry();
                    if (encryptEntry != null) {
                        if (StringUtils.isNotBlank(msgAttrs)) {
                            msgAttrs = msgAttrs + "&";
                        }
                        EncryptInfo encryptInfo = encryptEntry.getRsaEncryptInfo();
                        msgAttrs = msgAttrs + "_userName=" + object.getUserName()
                                + "&_encyVersion=" + encryptInfo.getVersion()
                                + "&_encyAesKey=" + encryptInfo.getRsaEncryptedKey();
                        body = EncryptUtil.aesEncrypt(body, encryptInfo.getAesKey());
                    }
                }
                if (StringUtils.isNotBlank(object.getMsgUUID())) {
                    if (StringUtils.isNotBlank(msgAttrs)) {
                        msgAttrs = msgAttrs + "&";
                    }
                    msgAttrs = msgAttrs + "msgUUID=" + object.getMsgUUID();
                }

                int msgType = 5;
                if (object.isEncrypt()) {
                    msgType |= FLAG_ALLOW_ENCRYPT;
                }
                byte[] attrData = msgAttrs.getBytes(StandardCharsets.UTF_8);
                totalLength = totalLength + body.length + attrData.length;
                buf = ByteBufAllocator.DEFAULT.buffer(4 + totalLength);
                buf.writeInt(totalLength);
                buf.writeByte(msgType);
                buf.writeInt(body.length);
                buf.writeBytes(body);
                buf.writeInt(attrData.length);
                buf.writeBytes(attrData);
            }
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.error("Write type5 data exception", ex);
            }
        }
        return buf;
    }

    private ByteBuf writeToBuf3(EncodeObject object) {
        ByteBuf buf = null;
        try {
            int totalLength = 1 + 4 + 4;
            byte[] body = null;

            // send multiple messages one time
            if (object.getBodylist() != null && !object.getBodylist().isEmpty()) {
                int totalCnt = 0;
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                for (byte[] entry : object.getBodylist()) {
                    if (totalCnt++ > 0) {
                        out.write(AttributeConstants.LINE_FEED_SEP.getBytes(StandardCharsets.UTF_8));
                    }
                    out.write(entry);
                }
                body = out.toByteArray();
            }
            if (body != null) {
                String msgAttrs = object.getAttributes();
                if (object.isCompress()) {
                    body = processCompress(body);
                }
                if (object.isEncrypt()) {
                    EncryptConfigEntry encryptEntry = object.getEncryptEntry();
                    if (encryptEntry != null) {
                        if (StringUtils.isNotBlank(msgAttrs)) {
                            msgAttrs = msgAttrs + "&";
                        }
                        EncryptInfo encryptInfo = encryptEntry.getRsaEncryptInfo();
                        msgAttrs = msgAttrs + "_userName=" + object.getUserName()
                                + "&_encyVersion=" + encryptInfo.getVersion()
                                + "&_encyAesKey=" + encryptInfo.getRsaEncryptedKey();
                        body = EncryptUtil.aesEncrypt(body, encryptInfo.getAesKey());
                    }
                }
                if (StringUtils.isNotBlank(object.getMsgUUID())) {
                    if (StringUtils.isNotBlank(msgAttrs)) {
                        msgAttrs = msgAttrs + "&";
                    }
                    msgAttrs = msgAttrs + "msgUUID=" + object.getMsgUUID();
                }

                int msgType = 3;
                if (object.isEncrypt()) {
                    msgType |= FLAG_ALLOW_ENCRYPT;
                }
                byte[] attrData = msgAttrs.getBytes(StandardCharsets.UTF_8);
                totalLength = totalLength + body.length + attrData.length;
                buf = ByteBufAllocator.DEFAULT.buffer(4 + totalLength);
                buf.writeInt(totalLength);
                buf.writeByte(msgType);
                buf.writeInt(body.length);
                buf.writeBytes(body);
                buf.writeInt(attrData.length);
                buf.writeBytes(attrData);
            }
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.error("Write type3 data exception", ex);
            }
        }
        return buf;
    }

    private byte[] processCompress(byte[] body) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(body);
            int guessLen = Snappy.maxCompressedLength(out.size());
            byte[] tmpData = new byte[guessLen];
            int len = Snappy.compress(out.toByteArray(), 0, out.size(),
                    tmpData, 0);
            body = new byte[len];
            System.arraycopy(tmpData, 0, body, 0, len);
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.error("Compress data exception", ex);
            }
        }
        return body;
    }
}
