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

package org.apache.inlong.sdk.dataproxy.network.tcp.codec;

import org.apache.inlong.common.msg.MsgType;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * TCP protocol encoder class
 *
 * Used to encode the request package sent to DataProxy
 */
public class ProtocolEncoder extends MessageToMessageEncoder<EncodeObject> {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolEncoder.class);
    private static final LogCounter exptCounter = new LogCounter(10, 100000, 60 * 1000L);

    @Override
    protected void encode(ChannelHandlerContext ctx,
            EncodeObject encObject, List<Object> out) throws Exception {
        ByteBuf buf = null;
        int totalLength;
        try {
            if (encObject.getMsgType() == MsgType.MSG_ACK_SERVICE) {
                totalLength = 1 + 4 + 4 + encObject.getMsgSize();
                buf = ByteBufAllocator.DEFAULT.buffer(4 + totalLength);
                buf.writeInt(totalLength);
                buf.writeByte(encObject.getIntMsgType());
                buf.writeInt(encObject.getBodyDataLength());
                if (encObject.getBodyDataLength() > 0) {
                    buf.writeBytes(encObject.getBodyData());
                }
                buf.writeInt(encObject.getAttrDataLength());
                if (encObject.getAttrDataLength() > 0) {
                    buf.writeBytes(encObject.getAttrData());
                }
            } else if (encObject.getMsgType() == MsgType.MSG_MULTI_BODY) {
                totalLength = 1 + 4 + 4 + encObject.getMsgSize();
                buf = ByteBufAllocator.DEFAULT.buffer(4 + totalLength);
                buf.writeInt(totalLength);
                buf.writeByte(encObject.getIntMsgType());
                buf.writeInt(encObject.getBodyDataLength());
                if (encObject.getBodyDataLength() > 0) {
                    buf.writeBytes(encObject.getBodyData());
                }
                buf.writeInt(encObject.getAttrDataLength());
                if (encObject.getAttrDataLength() > 0) {
                    buf.writeBytes(encObject.getAttrData());
                }
            } else if (encObject.getMsgType() == MsgType.MSG_BIN_MULTI_BODY) {
                totalLength = 1 + 2 + 2 + 2 + 4 + 2 + 4 + 4 + 2 + 2 + encObject.getMsgSize();
                buf = ByteBufAllocator.DEFAULT.buffer(4 + totalLength);
                buf.writeInt(totalLength);
                buf.writeByte(encObject.getIntMsgType());
                buf.writeShort(encObject.getGroupIdNum());
                buf.writeShort(encObject.getStreamIdNum());
                buf.writeShort(encObject.getExtField());
                buf.writeInt((int) encObject.getDtMs());
                buf.writeShort(encObject.getMsgCnt());
                buf.writeInt(encObject.getMessageId());
                buf.writeInt(encObject.getBodyDataLength());
                if (encObject.getBodyDataLength() > 0) {
                    buf.writeBytes(encObject.getBodyData());
                }
                buf.writeShort(encObject.getAttrDataLength());
                if (encObject.getAttrDataLength() > 0) {
                    buf.writeBytes(encObject.getAttrData());
                }
                buf.writeShort(0xee01);
            } else if (encObject.getMsgType() == MsgType.MSG_BIN_HEARTBEAT) {
                totalLength = 1 + 4 + 1 + 4 + 2 + encObject.getAttrDataLength() + 2;
                buf = ByteBufAllocator.DEFAULT.buffer(4 + totalLength);
                buf.writeInt(totalLength);
                buf.writeByte(encObject.getIntMsgType());
                buf.writeInt((int) encObject.getDtMs());
                buf.writeByte(2);
                buf.writeInt(0);
                buf.writeShort(encObject.getAttrDataLength());
                if (encObject.getAttrDataLength() > 0) {
                    buf.writeBytes(encObject.getAttrData());
                }
                buf.writeShort(0xee01);
            }
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ProtocolEncoder encode({}) message failure", encObject.getMsgType(), ex);
            }
        }
        if (buf != null) {
            out.add(buf);
        } else {
            if (exptCounter.shouldPrint()) {
                logger.warn("ProtocolEncoder write({}) buffer is null!", encObject.getMsgType());
            }
        }
    }
}
