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

package org.apache.inlong.sort.singletenant.flink.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.protocol.serialization.CanalSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.SerializationInfo;
import org.apache.inlong.sort.protocol.sink.KafkaSinkInfo;
import org.apache.inlong.sort.singletenant.flink.serialization.RowDataSerializationSchemaFactory;
import org.apache.inlong.sort.singletenant.flink.serialization.RowSerializationSchemaFactory;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;

import static org.apache.inlong.sort.configuration.Constants.SINK_KAFKA_PRODUCER_POOL_SIZE;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.createRowConverter;

public class KafkaSinkBuilder {

    public static <T> SinkFunction<T> buildKafkaSink(
            KafkaSinkInfo kafkaSinkInfo,
            Map<String, Object> properties,
            SerializationSchema<T> serializationSchema,
            Configuration config
    ) {
        String topic = kafkaSinkInfo.getTopic();
        Properties producerProperties = buildProducerProperties(properties, kafkaSinkInfo.getAddress());

        return new FlinkKafkaProducer<>(
                topic,
                serializationSchema,
                producerProperties,
                new FlinkFixedPartitioner<>(),
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE,
                config.getInteger(SINK_KAFKA_PRODUCER_POOL_SIZE)
        );
    }

    private static Properties buildProducerProperties(Map<String, Object> properties, String address) {
        Properties producerProperties = new Properties();
        producerProperties.putAll(properties);
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, address);
        return producerProperties;
    }

    public static void buildKafkaSinkStream(
            DataStream<Row> sourceStream,
            KafkaSinkInfo kafkaSinkInfo,
            Map<String, Object> properties,
            Configuration config,
            int sinkParallelism
    ) throws JsonProcessingException {
        SerializationInfo serializationInfo = kafkaSinkInfo.getSerializationInfo();
        if (serializationInfo instanceof CanalSerializationInfo) {
            DataFormatConverters.RowConverter rowConverter = createRowConverter(kafkaSinkInfo);
            DataStream<RowData> dataStream = sourceStream
                    .map(rowConverter::toInternal)
                    .uid(Constants.CONVERTER_UID)
                    .name("Row to RowData Converter")
                    .setParallelism(sinkParallelism);

            SerializationSchema<RowData> schema = RowDataSerializationSchemaFactory.build(
                    kafkaSinkInfo.getFields(), kafkaSinkInfo.getSerializationInfo());

            dataStream
                    .addSink(buildKafkaSink(kafkaSinkInfo, properties, schema, config))
                    .uid(Constants.SINK_UID)
                    .name("Kafka Sink")
                    .setParallelism(sinkParallelism);
        } else {
            SerializationSchema<Row> schema = RowSerializationSchemaFactory.build(
                    kafkaSinkInfo.getFields(), kafkaSinkInfo.getSerializationInfo());
            sourceStream
                    .addSink(buildKafkaSink(kafkaSinkInfo, properties, schema, config))
                    .uid(Constants.SINK_UID)
                    .name("Kafka Sink")
                    .setParallelism(sinkParallelism);
        }
    }
}
