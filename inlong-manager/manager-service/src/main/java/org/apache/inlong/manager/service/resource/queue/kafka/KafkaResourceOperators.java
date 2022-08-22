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

package org.apache.inlong.manager.service.resource.queue.kafka;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.kafka.KafkaClusterInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamBriefInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.core.ConsumptionService;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperator;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * kafka for create kafka Topic and Subscription
 */
@Slf4j
@Service
public class KafkaResourceOperators implements QueueResourceOperator {

    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private KafkaOperator kafkaOperator;
    @Autowired
    private ConsumptionService consumptionService;

    @Override
    public boolean accept(String mqType) {
        return MQType.KAFKA.equals(mqType);
    }

    @Override
    public void createQueueForGroup(@NotNull InlongGroupInfo groupInfo, @NotBlank String operator) {

        String groupId = groupInfo.getInlongGroupId();
        log.info("begin to create kafka resource for groupId={}", groupId);

        // get kafka cluster via the inlong cluster tag from the inlong group
        String clusterTag = groupInfo.getInlongClusterTag();
        KafkaClusterInfo kafkaCluster = (KafkaClusterInfo) clusterService.getOne(clusterTag, null,
                ClusterType.KAFKA);
        try {
            // 1. create kafka Topic - each Inlong Stream corresponds to a Kafka Topic
            List<InlongStreamBriefInfo> streamInfoList = streamService.getTopicList(groupId);
            if (streamInfoList == null || streamInfoList.isEmpty()) {
                log.warn("skip to create kafka topic and subscription as no streams for groupId={}", groupId);
                return;
            }
            for (InlongStreamBriefInfo streamInfo : streamInfoList) {
                this.createKafkaTopic(groupInfo, kafkaCluster, streamInfo.getInlongStreamId());
            }
        } catch (Exception e) {
            String msg = String.format("failed to create kafka resource for groupId=%s", groupId);
            log.error(msg, e);
            throw new WorkflowListenerException(msg + ": " + e.getMessage());
        }

        log.info("success to create kafka resource for groupId={}, cluster={}", groupId, kafkaCluster);
    }

    @Override
    public void deleteQueueForGroup(InlongGroupInfo groupInfo, String operator) {
        Preconditions.checkNotNull(groupInfo, "inlong group info cannot be null");

        String groupId = groupInfo.getInlongGroupId();
        log.info("begin to delete kafka resource for groupId={}", groupId);

        ClusterInfo clusterInfo = clusterService.getOne(groupInfo.getInlongClusterTag(), null, ClusterType.KAFKA);
        try {
            List<InlongStreamBriefInfo> streamInfoList = streamService.getTopicList(groupId);
            if (streamInfoList == null || streamInfoList.isEmpty()) {
                log.warn("skip to create kafka topic and subscription as no streams for groupId={}", groupId);
                return;
            }
            for (InlongStreamBriefInfo streamInfo : streamInfoList) {
                this.deleteKafkaTopic(groupInfo, (KafkaClusterInfo) clusterInfo, streamInfo.getInlongStreamId());
            }
        } catch (Exception e) {
            log.error("failed to delete kafka resource for groupId=" + groupId, e);
            throw new WorkflowListenerException("failed to delete kafka resource: " + e.getMessage());
        }

        log.info("success to delete kafka resource for groupId={}, cluster={}", groupId, clusterInfo);

    }

    @Override
    public void createQueueForStream(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            String operator) {
        Preconditions.checkNotNull(groupInfo, "inlong group info cannot be null");
        Preconditions.checkNotNull(streamInfo, "inlong stream info cannot be null");
        Preconditions.checkNotNull(operator, "operator cannot be null");

        String groupId = streamInfo.getInlongGroupId();
        String streamId = streamInfo.getInlongStreamId();
        log.info("begin to create kafka resource for groupId={}, streamId={}", groupId, streamId);

        try {
            // get kafka cluster via the inlong cluster tag from the inlong group
            String clusterTag = groupInfo.getInlongClusterTag();
            ClusterInfo clusterInfo = clusterService.getOne(clusterTag, null, ClusterType.KAFKA);
            // create kafka topic
            this.createKafkaTopic(groupInfo, (KafkaClusterInfo) clusterInfo, streamInfo.getInlongStreamId());
        } catch (Exception e) {
            String msg = String.format("failed to create kafka topic for groupId=%s, streamId=%s", groupId, streamId);
            log.error(msg, e);
            throw new WorkflowListenerException(msg + ": " + e.getMessage());
        }

        log.info("success to create kafka resource for groupId={}, streamId={}", groupId, streamId);

    }

    @Override
    public void deleteQueueForStream(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            String operator) {
        Preconditions.checkNotNull(groupInfo, "inlong group info cannot be null");
        Preconditions.checkNotNull(streamInfo, "inlong stream info cannot be null");

        String groupId = streamInfo.getInlongGroupId();
        String streamId = streamInfo.getInlongStreamId();
        log.info("begin to delete kafka resource for groupId={} streamId={}", groupId, streamId);

        try {
            ClusterInfo clusterInfo = clusterService.getOne(groupInfo.getInlongClusterTag(), null, ClusterType.KAFKA);
            this.deleteKafkaTopic(groupInfo, (KafkaClusterInfo) clusterInfo, streamInfo.getMqResource());
            log.info("success to delete kafka topic for groupId={}, streamId={}", groupId, streamId);
        } catch (Exception e) {
            String msg = String.format("failed to delete kafka topic for groupId=%s, streamId=%s", groupId, streamId);
            log.error(msg, e);
            throw new WorkflowListenerException(msg);
        }

        log.info("success to delete kafka resource for groupId={}, streamId={}", groupId, streamId);
    }

    /**
     * Create Kafka Topic and Subscription, and save the consumer group info.
     */
    private void createKafkaTopic(InlongGroupInfo groupInfo, KafkaClusterInfo kafkaCluster, String streamId)
            throws Exception {
        // 1. create kafka topic
        String topicName = groupInfo.getInlongGroupId() + "_" + streamId;
        kafkaOperator.createTopic(kafkaCluster, topicName);

        boolean exist = kafkaOperator.topicIsExists(kafkaCluster, topicName);
        if (!exist) {
            String bootStrapServers = kafkaCluster.getBootstrapServers();
            log.error("topic={} not exists in {}", topicName, bootStrapServers);
            throw new WorkflowListenerException("topic=" + topicName + " not exists in " + bootStrapServers);
        }

        // 2. create a subscription for the kafka topic
        kafkaOperator.createSubscription(kafkaCluster, topicName);
        String groupId = groupInfo.getInlongGroupId();
        log.info("success to create pulsar subscription for groupId={}, topic={}, subs={}",
                groupId, topicName, topicName);

        // 3. insert the consumer group info into the consumption table
        consumptionService.saveSortConsumption(groupInfo, topicName, topicName);
        log.info("success to save consume for groupId={}, topic={}, subs={}", groupId, topicName, topicName);
    }

    /**
     * Delete Kafka Topic and Subscription, and delete the consumer group info.
     */
    private void deleteKafkaTopic(InlongGroupInfo groupInfo, KafkaClusterInfo clusterInfo, String streamId) {
        // 1. delete kafka topic
        String topicName = groupInfo.getInlongGroupId() + "_" + streamId;
        kafkaOperator.forceDeleteTopic(clusterInfo, topicName);
    }
}
