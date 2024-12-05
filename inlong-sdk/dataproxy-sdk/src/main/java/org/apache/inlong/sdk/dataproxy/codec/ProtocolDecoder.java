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

import org.apache.inlong.sdk.dataproxy.utils.LogCounter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class ProtocolDecoder extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolDecoder.class);
    private static final LogCounter decExptCounter = new LogCounter(10, 200000, 60 * 1000L);
    @Override
    protected void decode(ChannelHandlerContext ctx,
            ByteBuf buffer, List<Object> out) throws Exception {
        buffer.markReaderIndex();
        // totallen
        int totalLen = buffer.readInt();
        if (totalLen != buffer.readableBytes()) {
            if (decExptCounter.shouldPrint()) {
                logger.error("Length not equal, totalLen={},readableBytes={},from={}",
                        totalLen, buffer.readableBytes(), ctx.channel());
            }
            buffer.resetReaderIndex();
            throw new Exception("totalLen is not equal readableBytes.total");
        }
        // msgtype
        int msgType = buffer.readByte() & 0x1f;

        if (msgType == 4) {
            if (logger.isDebugEnabled()) {
                logger.debug("debug decode");
            }
        } else if (msgType == 3 | msgType == 5) {
            // bodylen
            int bodyLength = buffer.readInt();
            if (bodyLength >= totalLen) {
                if (decExptCounter.shouldPrint()) {
                    logger.error("bodyLen greater than totalLen, totalLen={},bodyLen={},from={}",
                            totalLen, bodyLength, ctx.channel());
                }
                buffer.resetReaderIndex();
                throw new Exception("bodyLen is greater than totalLen.totalLen");
            }
            byte[] bodyBytes = null;
            if (bodyLength > 0) {
                bodyBytes = new byte[bodyLength];
                buffer.readBytes(bodyBytes);
            }
            // attrlen
            String attrInfo = "";
            int attrLength = buffer.readInt();
            if (attrLength > 0) {
                byte[] attrBytes = new byte[attrLength];
                buffer.readBytes(attrBytes);
                attrInfo = new String(attrBytes, StandardCharsets.UTF_8);
            }
            EncodeObject object;
            if (bodyBytes == null) {
                object = new EncodeObject(attrInfo);
            } else {
                object = new EncodeObject(Collections.singletonList(bodyBytes), attrInfo);
            }
            object.setMsgtype(5);
            out.add(object);
        } else if (msgType == 7) {

            int seqId = buffer.readInt();
            int attrLen = buffer.readShort();
            String attrInfo = "";
            if (attrLen > 0) {
                byte[] attrBytes = new byte[attrLen];
                buffer.readBytes(attrBytes);
                attrInfo = new String(attrBytes, StandardCharsets.UTF_8);
            }
            EncodeObject object = new EncodeObject(attrInfo);
            object.setMessageId(String.valueOf(seqId));

            buffer.readShort();

            object.setMsgtype(msgType);
            out.add(object);

        } else if (msgType == 8) {
            // dataTime(4) + body_ver(1) + body_len(4) + body + attr_len(2) + attr + magic(2)
            buffer.skipBytes(4 + 1 + 4); // skip datatime, body_ver and body_len
            final short load = buffer.readShort(); // read from body
            int attrLen = buffer.readShort();
            String attrInfo = "";
            if (attrLen > 0) {
                byte[] attrBytes = new byte[attrLen];
                buffer.readBytes(attrBytes);
                attrInfo = new String(attrBytes, StandardCharsets.UTF_8);
            }
            buffer.skipBytes(2); // skip magic
            EncodeObject object = new EncodeObject(attrInfo);
            object.setMsgtype(8);
            object.setLoad(load);
            out.add(object);
        }
    }
}
