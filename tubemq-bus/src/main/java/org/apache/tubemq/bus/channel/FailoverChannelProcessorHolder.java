package org.apache.tubemq.bus.channel;

import org.apache.flume.channel.ChannelProcessor;

public class FailoverChannelProcessorHolder {

    private static ChannelProcessor channelProcessor;

    public static ChannelProcessor getChannelProcessor() {
        return channelProcessor;
    }

    public static void setChannelProcessor(ChannelProcessor cp) {
        channelProcessor = cp;
    }
}
