package org.apache.inlong.manager.service.resource.queue.kafka;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.kafka.KafkaClusterInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.pojo.stream.InlongStreamBriefInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.core.ConsumptionService;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperator;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarOperator;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarUtils;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author zcy
 * @Date 2022/8/6 18:58
 * @Version 1.0
 */
@Slf4j
@Service
public class KafkaResourceOperator implements QueueResourceOperator {

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
        Preconditions.checkNotNull(groupInfo, "inlong group info cannot be null");
        Preconditions.checkNotNull(operator, "operator cannot be null");

        String groupId = groupInfo.getInlongGroupId();
        log.info("begin to create kafka resource for groupId={}", groupId);

        // get kafka cluster via the inlong cluster tag from the inlong group
        String clusterTag = groupInfo.getInlongClusterTag();
        KafkaClusterInfo kafkaCluster = (KafkaClusterInfo) clusterService.getOne(clusterTag, null,
                ClusterType.KAFKA);
        // TODO kafkaUtils get kafka admin through kafkaCluster
        // try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(kafkaCluster)) {
        try {
            // 1. create kafka Topic - each Inlong Stream corresponds to a Kafka Topic
            List<InlongStreamBriefInfo> streamInfoList = streamService.getTopicList(groupId);
            if (streamInfoList == null || streamInfoList.isEmpty()) {
                log.warn("skip to create kafka topic and subscription as no streams for groupId={}", groupId);
                return;
            }
            for (InlongStreamBriefInfo streamInfo : streamInfoList) {
                this.createKafkaTopic(groupInfo, kafkaCluster, streamInfo.getMqResource());
            }
        } catch (Exception e) {
            String msg = String.format("failed to create pulsar resource for groupId=%s", groupId);
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
                this.deleteKafkaTopic(groupInfo, (KafkaClusterInfo) clusterInfo, streamInfo.getMqResource());
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
            this.createKafkaTopic(groupInfo, (KafkaClusterInfo) clusterInfo, streamInfo.getMqResource());
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
    private void createKafkaTopic(InlongGroupInfo groupInfo, KafkaClusterInfo kafkaCluster, String topicName)
            throws Exception {
        // 1. create kafka topic
        kafkaOperator.createTopic();

        // 2. create a subscription for the kafka topic
        boolean exist = kafkaOperator.topicIsExists();
        if (!exist) {
            String topicFullName = "";
            String serviceUrl = kafkaCluster.getAdminUrl();
            log.error("topic={} not exists in {}", topicFullName, serviceUrl);
            throw new WorkflowListenerException("topic=" + topicFullName + " not exists in " + serviceUrl);
        }

        // subscription naming rules: clusterTag_topicName_consumer_group
        //TODO kafkaOperator.createSubscription(pulsarAdmin, topicBean, subscription);
        String subscription = "todo_name";
        String groupId = groupInfo.getInlongGroupId();
        log.info("success to create pulsar subscription for groupId={}, topic={}, subs={}",
                groupId, topicName, subscription);

        // 3. insert the consumer group info into the consumption table
        consumptionService.saveSortConsumption(groupInfo, topicName, subscription);
        log.info("success to save consume for groupId={}, topic={}, subs={}", groupId, topicName, subscription);
    }

    /**
     * Delete Kafka Topic and Subscription, and delete the consumer group info.
     */
    private void deleteKafkaTopic(InlongGroupInfo groupInfo, KafkaClusterInfo clusterInfo, String mqResource) {
        // 1. delete kafka topic
        kafkaOperator.forceDeleteTopic();
    }
}
