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

package org.apache.inlong.sdk.dataproxy.network;

import org.apache.inlong.sdk.dataproxy.codec.EncodeObject;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientHandler extends SimpleChannelInboundHandler<EncodeObject> {

    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);
    private static final LogCounter exceptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter thrownCnt = new LogCounter(10, 100000, 60 * 1000L);

    private final Sender sender;
    private final ClientMgr clientMgr;

    public ClientHandler(Sender sender, ClientMgr clientMgr) {
        this.sender = sender;
        this.clientMgr = clientMgr;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, EncodeObject encodeObject) {
        try {
            if (encodeObject.getMsgtype() != 8) {
                sender.notifyFeedback(ctx.channel(), encodeObject);
            }
        } catch (Throwable ex) {
            if (thrownCnt.shouldPrint()) {
                logger.warn("ClientHandler({}) channelRead0 throw exception", sender.getInstanceId(), ex);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        if (exceptCnt.shouldPrint()) {
            logger.warn("ClientHandler({})'s channel {} has error!",
                    sender.getInstanceId(), ctx.channel(), e);
        }
        try {
            clientMgr.setConnectionFrozen(ctx.channel());
        } catch (Throwable ex) {
            if (thrownCnt.shouldPrint()) {
                logger.warn("ClientHandler({}) exceptionCaught throw exception",
                        sender.getInstanceId(), ex);
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ctx.fireChannelInactive();
        if (logger.isDebugEnabled()) {
            logger.debug("ClientHandler({}) channelDisconnected {}",
                    sender.getInstanceId(), ctx.channel());
        }
        try {
            sender.notifyConnectionDisconnected(ctx.channel());
        } catch (Throwable ex) {
            if (thrownCnt.shouldPrint()) {
                logger.warn("ClientHandler({}) channelInactive throw exception",
                        sender.getInstanceId(), ex);
            }
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("ClientHandler({}) channelUnregistered {}",
                    sender.getInstanceId(), ctx.channel());
        }
        try {
            sender.notifyConnectionDisconnected(ctx.channel());
        } catch (Throwable ex) {
            if (thrownCnt.shouldPrint()) {
                logger.warn("ClientHandler({}) channelUnregistered throw exception",
                        sender.getInstanceId(), ex);
            }
        }
    }
}
