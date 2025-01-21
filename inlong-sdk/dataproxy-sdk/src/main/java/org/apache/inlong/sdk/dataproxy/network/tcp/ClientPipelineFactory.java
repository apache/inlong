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

import org.apache.inlong.sdk.dataproxy.network.tcp.codec.ProtocolDecoder;
import org.apache.inlong.sdk.dataproxy.network.tcp.codec.ProtocolEncoder;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * TCP client Pipeline Factory class
 *
 * Used to build TCP pipeline
 */
public class ClientPipelineFactory extends ChannelInitializer<SocketChannel> {

    private final TcpClientMgr tcpClientMgr;

    public ClientPipelineFactory(TcpClientMgr tcpClientMgr) {
        this.tcpClientMgr = tcpClientMgr;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {

        // Setup channel except for the SsHandler for TLS enabled connections

        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                65536, 0, 4, 0, 0));

        ch.pipeline().addLast("contentDecoder", new ProtocolDecoder());
        ch.pipeline().addLast("contentEncoder", new ProtocolEncoder());
        ch.pipeline().addLast("handler", new ClientHandler(tcpClientMgr));
    }
}
