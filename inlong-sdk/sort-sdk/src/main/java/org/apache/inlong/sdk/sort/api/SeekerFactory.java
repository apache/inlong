package org.apache.inlong.sdk.sort.api;

import org.apache.inlong.sdk.sort.entity.InLongTopic;
import org.apache.inlong.sdk.sort.impl.kafka.KafkaSeeker;
import org.apache.inlong.sdk.sort.impl.pulsar.PulsarSeeker;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.pulsar.client.api.Consumer;

/**
 * Factory that create configured seeker
 */
public class SeekerFactory {

    public static PulsarSeeker createPulsarSeeker(Consumer<byte[]> consumer, InLongTopic inLongTopic) {
        PulsarSeeker seeker = new PulsarSeeker(consumer);
        seeker.configure(inLongTopic);
        return seeker;
    }

    public static KafkaSeeker createKafkaSeeker(KafkaConsumer<byte[], byte[]> consumer, InLongTopic inLongTopic) {
        KafkaSeeker seeker = new KafkaSeeker(consumer);
        seeker.configure(inLongTopic);
        return seeker;
    }
}
