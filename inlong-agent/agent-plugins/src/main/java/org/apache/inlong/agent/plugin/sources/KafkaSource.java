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

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.plugin.Reader;
import org.apache.inlong.agent.plugin.Source;
import org.apache.inlong.agent.plugin.metrics.SourceJmxMetric;
import org.apache.inlong.agent.plugin.metrics.SourceMetrics;
import org.apache.inlong.agent.plugin.metrics.SourcePrometheusMetrics;
import org.apache.inlong.agent.plugin.sources.reader.KafkaReader;
import org.apache.inlong.agent.utils.ConfigUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import static org.apache.inlong.agent.constant.JobConstants.DEFAULT_JOB_LINE_FILTER;
import static org.apache.inlong.agent.constant.JobConstants.JOB_KAFKA_OFFSET;
import static org.apache.inlong.agent.constant.JobConstants.JOB_KAFKA_PARTITION_OFFSET_DELIMITER;
import static org.apache.inlong.agent.constant.JobConstants.JOB_LINE_FILTER_PATTERN;
import static org.apache.inlong.agent.constant.JobConstants.JOB_OFFSET_DELIMITER;

public class KafkaSource implements Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);

    private static final String KAFKA_SOURCE_TAG_NAME = "AgentKafkaSourceMetric";
    private static final String JOB_KAFKAJOB_PARAM_PREFIX = "job.kafkajob.";
    private static final String JOB_KAFKAJOB_TOPIC = "job.kafkajob.topic";
    private static final String JOB_KAFKAJOB_BOOTSTRAP_SERVERS = "job.kafkajob.bootstrap.servers";
    private static final String JOB_KAFKAJOB_GROUP_ID = "job.kafkajob.group.id";
    private static final String JOB_KAFKAJOB_WAIT_TIMEOUT = "job.kafkajob.wait.timeout";
    //private static final String JOB_KAFKAJOB_PARTITION_OFFSET = "job.kafkajob.topic.partition.offset";
    private static final String KAFKA_COMMIT_AUTO = "enable.auto.commit";
    private static final String KAFKA_DESERIALIZER_METHOD = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String KAFKA_KEY_DESERIALIZER = "key.deserializer";
    private static final String KAFKA_VALUE_DESERIALIZER = "value.deserializer";

    private final SourceMetrics sourceMetrics;

    public KafkaSource() {
        if (ConfigUtil.isPrometheusEnabled()) {
            this.sourceMetrics = new SourcePrometheusMetrics(KAFKA_SOURCE_TAG_NAME);
        } else {
            this.sourceMetrics = new SourceJmxMetric(KAFKA_SOURCE_TAG_NAME);
        }

    }

    @Override
    public List<Reader> split(JobProfile conf) {
        List<Reader> result = new ArrayList<>();
        String filterPattern = conf.get(JOB_LINE_FILTER_PATTERN, DEFAULT_JOB_LINE_FILTER);

        Properties props = new Properties();
        Map<String,String> map = (Map)JSON.parse(conf.toJsonStr());
        Iterator<Map.Entry<String,String>> iterator = map.entrySet().iterator();
        //begin build kafkaConsumer
        while (iterator.hasNext()) {
            Map.Entry<String,String> entry = iterator.next();
            if (entry.getKey() != null && (entry.getKey().equals(JOB_KAFKAJOB_BOOTSTRAP_SERVERS)
                    || entry.getKey().equals(JOB_KAFKAJOB_GROUP_ID))) {
                props.put(entry.getKey().replace(JOB_KAFKAJOB_PARAM_PREFIX,""), entry.getValue());
            }
        }
        props.put(KAFKA_KEY_DESERIALIZER, KAFKA_DESERIALIZER_METHOD);
        props.put(KAFKA_VALUE_DESERIALIZER, KAFKA_DESERIALIZER_METHOD);
        //set offset
        props.put(KAFKA_COMMIT_AUTO, false);
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(conf.get(JOB_KAFKAJOB_TOPIC));
        String allPartitionOffsets = map.get(JOB_KAFKA_OFFSET);
        Long offset = 0L;
        String[] partitionOffsets = null;
        if (StringUtils.isNotBlank(allPartitionOffsets)) {
            //example:0#110_1#666_2#222
            partitionOffsets = allPartitionOffsets.split(JOB_OFFSET_DELIMITER);
        }
        // spilt reader reduce to partition
        if (null != partitionInfoList) {
            for (PartitionInfo partitionInfo : partitionInfoList) {
                KafkaConsumer<String,String> partitonConsumer = new KafkaConsumer<>(props);
                partitonConsumer.assign(Collections.singletonList(
                        new TopicPartition(partitionInfo.topic(), partitionInfo.partition())));
                //if get offset,consume from offset; if not,consume from 0
                if (partitionOffsets != null && partitionOffsets.length > 0) {
                    for (String partitionOffset : partitionOffsets) {
                        if (partitionOffset.contains(JOB_KAFKA_PARTITION_OFFSET_DELIMITER)
                                && partitionOffset.split(JOB_KAFKA_PARTITION_OFFSET_DELIMITER)[0]
                                .equals(String.valueOf(partitionInfo.partition()))) {
                            offset = Long.valueOf(partitionOffset.split(JOB_KAFKA_PARTITION_OFFSET_DELIMITER)[1]);
                        }
                    }
                }
                LOGGER.info("kafka topic partition offset:{}", offset);
                partitonConsumer.seek(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), offset);
                KafkaReader<String,String> kafkaReader = new KafkaReader<>(partitonConsumer, map);
                //kafkaReader.setReadTimeout(conf.getInt(JOB_KAFKAJOB_READ_TIMEOUT, -1));
                kafkaReader.setWaitMillisecs(conf.getInt(JOB_KAFKAJOB_WAIT_TIMEOUT,100));
                addValidator(filterPattern, kafkaReader);
                result.add(kafkaReader);
            }
        }
        return result;
    }

    private void addValidator(String filterPattern, KafkaReader kafkaReader) {
        kafkaReader.addPatternValidator(filterPattern);
    }
}
