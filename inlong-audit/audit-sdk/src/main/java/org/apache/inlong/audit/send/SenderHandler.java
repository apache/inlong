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

package org.apache.inlong.audit.send;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class SenderHandler extends SimpleChannelHandler {
    private SenderManager manager;

    /**
     * Constructor
     *
     * @param manager
     */
    public SenderHandler(SenderManager manager) {
        this.manager = manager;
    }

    /**
     * Message Received
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        try {
            manager.onMessageReceived(ctx, e);
        } catch (Throwable ex) {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
    }

    /**
     * Caught exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        try {
            manager.onExceptionCaught(ctx, e);
        } catch (Throwable ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
     * Disconnected channel
     */
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        try {
            super.channelDisconnected(ctx, e);
        } catch (Throwable ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
     * Closed channel
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
        try {
            super.channelClosed(ctx, e);
        } catch (Throwable ex) {
            System.out.println(ex.getMessage());
        }
    }
}
