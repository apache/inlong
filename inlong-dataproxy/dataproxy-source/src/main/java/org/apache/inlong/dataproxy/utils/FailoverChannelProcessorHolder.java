package org.apache.inlong.dataproxy.utils;

import org.apache.flume.channel.ChannelProcessor;

/**
 * Created by kennyjiang on 2017/4/7.
 */
public class FailoverChannelProcessorHolder {
    private static ChannelProcessor channelProcessor;

    public static ChannelProcessor getChannelProcessor() {
        return channelProcessor;
    }

    public static void setChannelProcessor(ChannelProcessor cp) {
        channelProcessor = cp;
    }
}
