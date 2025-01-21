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

package org.apache.inlong.sdk.dataproxy.network.tcp;

import org.apache.inlong.common.msg.MsgType;
import org.apache.inlong.sdk.dataproxy.network.tcp.codec.DecodeObject;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP client handler class
 *
 * Used to process TCP response message.
 */
public class ClientHandler extends SimpleChannelInboundHandler<DecodeObject> {

    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);
    private static final LogCounter exceptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter thrownCnt = new LogCounter(10, 100000, 60 * 1000L);

    private final TcpClientMgr tcpClientMgr;

    public ClientHandler(TcpClientMgr tcpClientMgr) {
        this.tcpClientMgr = tcpClientMgr;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, DecodeObject decObject) {
        if (decObject.getMsgType() != MsgType.MSG_BIN_HEARTBEAT) {
            tcpClientMgr.feedbackMsgResponse(ctx.channel().toString(), decObject);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        if (exceptCnt.shouldPrint()) {
            logger.warn("ClientHandler({})'s channel {} has error!",
                    tcpClientMgr.getSenderId(), ctx.channel(), e);
        }
        try {
            tcpClientMgr.setChannelFrozen(ctx.channel().toString());
        } catch (Throwable ex) {
            if (thrownCnt.shouldPrint()) {
                logger.warn("ClientHandler({}) exceptionCaught throw exception",
                        tcpClientMgr.getSenderId(), ex);
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ctx.fireChannelInactive();
        if (logger.isDebugEnabled()) {
            logger.debug("ClientHandler({}) channelDisconnected {}",
                    tcpClientMgr.getSenderId(), ctx.channel());
        }
        try {
            tcpClientMgr.notifyChannelDisconnected(ctx.channel().toString());
        } catch (Throwable ex) {
            if (thrownCnt.shouldPrint()) {
                logger.warn("ClientHandler({}) channelInactive throw exception",
                        tcpClientMgr.getSenderId(), ex);
            }
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("ClientHandler({}) channelUnregistered {}",
                    tcpClientMgr.getSenderId(), ctx.channel());
        }
        try {
            tcpClientMgr.notifyChannelDisconnected(ctx.channel().toString());
        } catch (Throwable ex) {
            if (thrownCnt.shouldPrint()) {
                logger.warn("ClientHandler({}) channelUnregistered throw exception",
                        tcpClientMgr.getSenderId(), ex);
            }
        }
    }
}
