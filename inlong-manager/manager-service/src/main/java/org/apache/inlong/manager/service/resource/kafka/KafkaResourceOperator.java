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

package org.apache.inlong.manager.service.resource.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.pojo.sink.SinkInfo;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkDTO;
import org.apache.inlong.manager.service.resource.SinkResourceOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

@Service
@Slf4j
public class KafkaResourceOperator implements SinkResourceOperator {

    @Autowired
    private StreamSinkService sinkService;

    @Override
    public Boolean accept(SinkType sinkType) {
        return SinkType.KAFKA == sinkType;
    }

    @Override
    public void createSinkResource(String groupId, SinkInfo sinkInfo) {
        KafkaSinkDTO kafkaInfo = KafkaSinkDTO.getFromJson(sinkInfo.getExtParams());
        try (Admin admin = getKafkaAdmin(kafkaInfo)) {
            if (needCreateTopic(admin, kafkaInfo)) {
                CreateTopicsResult result = admin.createTopics(Collections.singleton(
                        new NewTopic(kafkaInfo.getTopicName(),
                                Optional.of(Integer.parseInt(kafkaInfo.getPartitionNum())),
                                Optional.empty())));
                result.values().get(kafkaInfo.getTopicName()).get();
            }
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_SUCCESSFUL.getCode(),
                    "create kafka topic success");
            log.info("success create kafka topic {} for data group [{}]", kafkaInfo.getTopicName(), groupId);
        } catch (Throwable e) {
            log.error("create kafka topic error, ", e);
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_FAILED.getCode(), e.getMessage());
            throw new WorkflowException("create kafka topic failed, reason: " + e.getMessage());
        }
    }

    private Admin getKafkaAdmin(KafkaSinkDTO kafkaInfo) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaInfo.getBootstrapServers());
        return Admin.create(props);
    }

    private boolean needCreateTopic(Admin admin, KafkaSinkDTO kafkaInfo) throws Exception {
        ListTopicsResult listResult = admin.listTopics();
        if (!listResult.namesToListings().get().containsKey(kafkaInfo.getTopicName())) {
            log.debug("kafka topic {} not existed, proceed to create", kafkaInfo.getTopicName());
            return true;
        }
        DescribeTopicsResult result = admin.describeTopics(Collections.singletonList(kafkaInfo.getTopicName()));
        TopicDescription desc = result.values().get(kafkaInfo.getTopicName()).get();
        if (desc.partitions().size() != Integer.parseInt(kafkaInfo.getPartitionNum())) {
            String errMsg = String.format(
                    "kafka topic %s already existed with partition num %d <> requested partition num %s",
                    kafkaInfo.getTopicName(), desc.partitions().size(), kafkaInfo.getPartitionNum());
            log.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        } else {
            log.debug("kafka topic {} with {} partitions already existed, no need to create",
                    kafkaInfo.getTopicName(), kafkaInfo.getPartitionNum());
            return false;
        }
    }
}
