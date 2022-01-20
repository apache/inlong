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

package org.apache.inlong.sdk.dataproxy.network;

import org.apache.inlong.sdk.dataproxy.codec.EncodeObject;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientHandler extends IdleStateAwareChannelHandler {
    private static final Logger logger = LoggerFactory
            .getLogger(ClientHandler.class);

    private final Sender sender;
    private final ClientMgr clientMgr;

    public ClientHandler(Sender sender, ClientMgr clientMgr) {
        this.sender = sender;
        this.clientMgr = clientMgr;
    }

    @Override
    public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) throws Exception {

    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        try {
            EncodeObject encodeObject = (EncodeObject) e.getMessage();
            if (encodeObject.getMsgtype() != 8) {
                sender.notifyFeedback(e.getChannel(), encodeObject);
            } else {
                clientMgr.notifyHBAck(e.getChannel(), encodeObject.getLoad());
            }
        } catch (Exception ex) {
            logger.error("error :", ex);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.error("this channel {} has error! , reason is {} ", e.getChannel(), e.getCause());
        try {
            clientMgr.setConnectionFrozen(e.getChannel());
        } catch (Exception e1) {
            logger.error("exceptionCaught error :", e1);
        }
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx,
                                    ChannelStateEvent e) {
        // clientMgr.resetClient(e.getChannel());
        logger.info("ClientHandler channelDisconnected {}", e.getChannel());
        try {
            sender.notifyConnectionDisconnected(e.getChannel());
        } catch (Exception e1) {
            logger.error("exceptionCaught error {}", e1.getMessage());
        }
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
        // clientMgr.resetClient(e.getChannel());
        logger.info("ClientHandler channelClosed {}", e.getChannel());
        try {
            sender.notifyConnectionDisconnected(e.getChannel());
        } catch (Exception e1) {
            logger.error("exceptionCaught error ", e1);
        }
    }
}
