package org.apache.inlong.audit.send;

import org.apache.inlong.audit.util.Encoder;
import org.apache.inlong.audit.util.IpPort;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;

class SenderChannelTest {
    private ClientBootstrap client = new ClientBootstrap();
    private IpPort ipPortObj = new IpPort("127.0.0.1", 80);
    private ChannelFuture future;
    SenderChannel senderChannel;

    public SenderChannelTest() {
        try {
            client.setFactory(new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool(),
                    10));

            client.setPipelineFactory(() -> {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("encoder", new Encoder());
                return pipeline;
            });
            client.setOption("tcpNoDelay", true);
            client.setOption("child.tcpNoDelay", true);
            client.setOption("keepAlive", true);
            client.setOption("child.keepAlive", true);
            client.setOption("reuseAddr", true);

            future = client.connect(ipPortObj.addr).await();
            senderChannel = new SenderChannel(future.getChannel(), ipPortObj, 10);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    void tryAcquire() {
        boolean ret = senderChannel.tryAcquire();
        System.out.println(ret);
    }

    @Test
    void release() {
        senderChannel.release();
    }

    @Test
    void testToString() {
        IpPort ipPort = senderChannel.getIpPort();
        System.out.println(ipPort);
    }

    @Test
    void getIpPort() {
        String toString = senderChannel.toString();
        System.out.println(toString);
    }

    @Test
    void getChannel() {
        Channel channel = senderChannel.getChannel();
        System.out.println(channel.getConfig().getConnectTimeoutMillis());
        System.out.println(channel.getRemoteAddress());
        System.out.println(channel.getId());
        System.out.println(channel.getInterestOps());
    }
}