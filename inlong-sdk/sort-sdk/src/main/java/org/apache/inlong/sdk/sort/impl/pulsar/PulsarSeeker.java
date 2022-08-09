package org.apache.inlong.sdk.sort.impl.pulsar;

import org.apache.inlong.sdk.sort.api.Seeker;
import org.apache.inlong.sdk.sort.entity.InLongTopic;
import org.apache.inlong.sdk.sort.util.TimeUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarSeeker implements Seeker {

    private final Logger logger = LoggerFactory.getLogger(PulsarSeeker.class);
    private long seekTime = -1;
    private Consumer<byte[]> consumer;
    private String topic;

    public PulsarSeeker(Consumer<byte[]> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void configure(InLongTopic inLongTopic) {
        seekTime = TimeUtil.parseStartTime(inLongTopic);
        topic = inLongTopic.getTopic();
        logger.info("start to config pulsar seeker, topic is {}, seek time is {}", topic, seekTime);
    }

    @Override
    public void seek() {
        if (seekTime < 0) {
            return;
        }
        logger.info("start to seek pulsar topic {}, seek time is {}", topic, seekTime);
        try {
            consumer.seek(seekTime);
        } catch (PulsarClientException e) {
            logger.error("fail to seek, start time is {}, ex is {}", seekTime, e.getMessage(), e);
        }
    }

    @Override
    public long getSeekTime() {
        return seekTime;
    }
}
