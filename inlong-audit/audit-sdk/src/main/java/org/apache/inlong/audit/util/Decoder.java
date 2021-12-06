/**
 * Tencent is pleased to support the open source community by making Tars available.
 * <p>
 * Copyright (C) 2015,2016 THL A29 Limited, a Tencent company. All rights reserved.
 * <p>
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * https://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.inlong.audit.util;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class Decoder extends FrameDecoder {
    // Maximum return packet size
    private static final int MAX_RESPONSE_LENGTH = 8 * 1024 * 1024;
    //length content
    private int HEADER_TAG_LENGTH = 4;

    /**
     * Constructor
     */
    public Decoder() {
    }

    /**
     * decoding
     */
    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) {
        // Every time you need to read the complete package (that is, read to the end of the package),
        // otherwise only the first one will be parsed correctly, which will adversely affect the parsing of the subsequent package
        buffer.array();
        buffer.markReaderIndex();
        //Packet composition: 4 bytes length content + ProtocolBuffer content
        int totalLen = buffer.readInt();
        // Respond to abnormal channel, interrupt in time to avoid stuck
        if (totalLen > MAX_RESPONSE_LENGTH) {
            channel.close();
            return null;
        }
        // If the package is not complete, continue to wait for the return package
        if (buffer.readableBytes() < (totalLen - HEADER_TAG_LENGTH)) {
            buffer.resetReaderIndex();
            return null;
        }
        ChannelBuffer returnBuffer = new DynamicChannelBuffer(ChannelBuffers.BIG_ENDIAN, totalLen);
        returnBuffer.writeInt(totalLen);
        buffer.readBytes(returnBuffer, HEADER_TAG_LENGTH, totalLen - HEADER_TAG_LENGTH);
        return returnBuffer;
    }
}
