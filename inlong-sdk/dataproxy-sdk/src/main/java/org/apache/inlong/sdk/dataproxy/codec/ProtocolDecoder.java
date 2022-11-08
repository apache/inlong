/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sdk.dataproxy.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ProtocolDecoder extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx,
            ByteBuf buffer, List<Object> out) throws Exception {
        buffer.markReaderIndex();
        // totallen
        int totalLen = buffer.readInt();
        LOGGER.debug("decode totalLen : {}", totalLen);
        if (totalLen != buffer.readableBytes()) {
            LOGGER.error("totalLen is not equal readableBytes.total:" + totalLen
                    + ";readableBytes:" + buffer.readableBytes());
            buffer.resetReaderIndex();
            throw new Exception("totalLen is not equal readableBytes.total");
        }
        // msgtype
        int msgType = buffer.readByte() & 0x1f;

        if (msgType == 4) {
            LOGGER.info("debug decode");
        }
        if (msgType == 3 | msgType == 5) {
            // bodylen
            int bodyLength = buffer.readInt();
            if (bodyLength >= totalLen) {
                LOGGER.error("bodyLen is greater than totalLen.totalLen:" + totalLen
                        + ";bodyLen:" + bodyLength);
                buffer.resetReaderIndex();
                throw new Exception("bodyLen is greater than totalLen.totalLen");
            }
            byte[] bodyBytes = null;
            if (bodyLength > 0) {
                bodyBytes = new byte[bodyLength];
                buffer.readBytes(bodyBytes);
            }

            // attrlen
            int attrLength = buffer.readInt();
            byte[] attrBytes = null;
            if (attrLength > 0) {
                attrBytes = new byte[attrLength];
                buffer.readBytes(attrBytes);
            }
            EncodeObject object = new EncodeObject(bodyBytes, new String(attrBytes, StandardCharsets.UTF_8));
            object.setMsgtype(5);
            out.add(object);
        } else if (msgType == 7) {

            int seqId = buffer.readInt();
            int attrLen = buffer.readShort();
            byte[] attrBytes = null;
            if (attrLen > 0) {
                attrBytes = new byte[attrLen];
                buffer.readBytes(attrBytes);
            }
            EncodeObject object = new EncodeObject(new String(attrBytes, StandardCharsets.UTF_8));
            object.setMessageId(String.valueOf(seqId));

            buffer.readShort();

            object.setMsgtype(msgType);
            out.add(object);

        } else if (msgType == 8) {
            int attrlen = buffer.getShort(4 + 1 + 4 + 1 + 4 + 2);
            buffer.skipBytes(13 + attrlen + 2);
            EncodeObject object = new EncodeObject();
            object.setMsgtype(8);
            object.setLoad(buffer.getShort(4 + 1 + 4 + 1 + 4));
            out.add(object);
        }
    }
}
