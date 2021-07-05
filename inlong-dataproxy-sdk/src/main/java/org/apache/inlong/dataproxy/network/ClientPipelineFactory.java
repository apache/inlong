/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.network;

import org.apache.inlong.dataproxy.codec.ProtocolDecoder;
import org.apache.inlong.dataproxy.codec.ProtocolEncoder;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

public class ClientPipelineFactory implements ChannelPipelineFactory {
    private final ClientMgr clientMgr;
    private final Sender sender;

    public ClientPipelineFactory(ClientMgr clientMgr, Sender sender) {
        this.clientMgr = clientMgr;
        this.sender = sender;
    }

    public ChannelPipeline getPipeline() throws Exception {

        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                65536, 0, 4, 0, 0));
        pipeline.addLast("contentDecoder", new ProtocolDecoder());
        pipeline.addLast("contentEncoder", new ProtocolEncoder());
        pipeline.addLast("handler", new ClientHandler(sender, clientMgr));

        return pipeline;
    }
}
