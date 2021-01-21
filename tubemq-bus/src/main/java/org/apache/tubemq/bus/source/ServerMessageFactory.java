/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.bus.source;

import static org.apache.tubemq.bus.source.SourceConfigConstants.DEFAULT_READ_IDLE_TIME;
import static org.apache.tubemq.bus.source.SourceConfigConstants.TCP_PROTOCOL;

import java.util.concurrent.TimeUnit;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

public class ServerMessageFactory implements ChannelPipelineFactory {

    private ServiceDecoder decoder;
    private String protocolType;
    private int maxMsgLength;

    private final Timer timer = new HashedWheelTimer();

    /**
     * Init message factory.
     */
    public ServerMessageFactory() {
    }

    public ServerMessageFactory setDecoder(ServiceDecoder decoder) {
        this.decoder = decoder;
        return this;
    }

    public ServerMessageFactory setProtocolType(String protocolType) {
        this.protocolType = protocolType;
        return this;
    }

    public ServerMessageFactory setMaxMsgLength(int maxMsgLength) {
        this.maxMsgLength = maxMsgLength;
        return this;
    }



    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline cp = Channels.pipeline();
        if (TCP_PROTOCOL.equals(this.protocolType)) {
            cp.addLast("messageDecoder", new LengthFieldBasedFrameDecoder(
                this.maxMsgLength, 0, 4, 0, 0));
            cp.addLast("readTimeoutHandler", new ReadTimeoutHandler(timer,
                DEFAULT_READ_IDLE_TIME, TimeUnit.MILLISECONDS));
            SimpleChannelHandler messageHandler = new ServerMessageHandler();
            cp.addLast("messageHandler", messageHandler);
        }
        return cp;
    }
}
