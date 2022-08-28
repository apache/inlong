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

package org.apache.inlong.agent.plugin.sources;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.utils.CloudEventsConverterUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * test cloudEvents converter util
 */
public class TestCloudEventsConverterUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestCloudEventsConverterUtil.class);

    @Before
    public void createKafkaConsumer() {
        //Basic producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.3:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-cloudevents-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
        props.put(CloudEventSerializer.ENCODING_CONFIG, Encoding.STRUCTURED);
        props.put(CloudEventSerializer.EVENT_FORMAT_CONFIG, JsonFormat.CONTENT_TYPE);

        //create the kafkaProducer
        KafkaProducer<String, CloudEvent> producer = new KafkaProducer<String, CloudEvent>(props);
        String topic = "test";

        CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                .withSource(URI.create("https://github.com/inLong"))
                .withType("producer.example");
        for (int i = 0; i < 5; i++) {
            try {
                String id = UUID.randomUUID().toString();
                String data = "Event number " + i;

                CloudEvent event = eventTemplate.newBuilder()
                        .withId(id)
                        .withData("text/plain", data.getBytes())
                        .build();

                RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, id, event)).get();
                LOGGER.info("Record sent to partition {} with offset {}. \n", metadata.partition(), metadata.offset());
            } catch (Exception e) {
                LOGGER.error("Error while trying to send the record");
                e.printStackTrace();
                return;
            }
        }

        producer.flush();
        producer.close();
    }

    @Test
    public void testConverter() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.3:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-cloudevents-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // Create the consumer and subscribe to the topic
        KafkaConsumer<String, CloudEvent> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test"));

        LOGGER.info("Consumer started");
        List<DefaultMessage> list = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, CloudEvent> consumerRecords = consumer.poll(Duration.ofMillis(100));
            consumerRecords.forEach(record -> {
                DefaultMessage message = CloudEventsConverterUtil.convertToDefaultMessage(record.value());
                list.add(message);
            });
            if (!list.isEmpty()) {
                for (Message message : list) {
                    LOGGER.info("Message info is : {}", message.toString());
                }
                break;
            }
        }
    }
}
