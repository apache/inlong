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

package org.apache.inlong.manager.service.resource.queue.pulsar;

import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.stream.InlongStreamBriefInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.QueryMessageRequest;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.consume.InlongConsumeService;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.stream.InlongStreamService;

import com.google.common.base.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Operator for create Pulsar Tenant, Namespace, Topic and Subscription
 */
@Slf4j
@Service
public class PulsarQueueResourceOperator implements QueueResourceOperator {

    /**
     * The name rule for Pulsar subscription: clusterTag_topicName_sinkId_consumer_group
     */
    public static final String PULSAR_SUBSCRIPTION = "%s_%s_%s_consumer_group";

    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongConsumeService consumeService;
    @Autowired
    private PulsarOperator pulsarOperator;

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    @Override
    public boolean accept(String mqType) {
        return MQType.PULSAR.equals(mqType) || MQType.TDMQ_PULSAR.equals(mqType);
    }

    @Override
    public void createQueueForGroup(InlongGroupInfo groupInfo, String operator) {
        Preconditions.expectNotNull(groupInfo, "inlong group info cannot be null");
        Preconditions.expectNotBlank(operator, ErrorCodeEnum.INVALID_PARAMETER, "operator cannot be null");

        String groupId = groupInfo.getInlongGroupId();
        String clusterTag = groupInfo.getInlongClusterTag();
        log.info("begin to create pulsar resource for groupId={}, clusterTag={}", groupId, clusterTag);

        if (!(groupInfo instanceof InlongPulsarInfo)) {
            throw new BusinessException("the mqType must be PULSAR for inlongGroupId=" + groupId);
        }

        InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) groupInfo;
        String tenant = pulsarInfo.getPulsarTenant();
        // get pulsar cluster via the inlong cluster tag from the inlong group
        List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(clusterTag, ClusterType.PULSAR);
        for (ClusterInfo clusterInfo : clusterInfos) {
            PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;
            try {
                // create pulsar tenant and namespace
                if (StringUtils.isBlank(tenant)) {
                    tenant = pulsarCluster.getPulsarTenant();
                }

                // if the group was not successful, need create tenant and namespace
                if (!Objects.equal(GroupStatus.CONFIG_SUCCESSFUL.getCode(), groupInfo.getStatus())) {
                    pulsarOperator.createTenant(pulsarCluster, tenant);
                    String namespace = groupInfo.getMqResource();
                    pulsarOperator.createNamespace(pulsarCluster, pulsarInfo, tenant, namespace);

                    log.info("success to create pulsar resource for groupId={}, tenant={}, namespace={}, cluster={}",
                            groupId, tenant, namespace, pulsarCluster);
                }
            } catch (Exception e) {
                String msg = "failed to create pulsar resource for groupId=" + groupId;
                log.error(msg + ", cluster=" + pulsarCluster, e);
                throw new WorkflowListenerException(msg + ": " + e.getMessage());
            }
        }

        log.info("success to create pulsar resource for groupId={}, clusterTag={}", groupId, clusterTag);
    }

    @Override
    public void deleteQueueForGroup(InlongGroupInfo groupInfo, String operator) {
        Preconditions.expectNotNull(groupInfo, "inlong group info cannot be null");
        String groupId = groupInfo.getInlongGroupId();
        String clusterTag = groupInfo.getInlongClusterTag();
        log.info("begin to delete pulsar resource for groupId={}, clusterTag={}", groupId, clusterTag);

        List<InlongStreamBriefInfo> streamInfos = streamService.getTopicList(groupId);
        if (CollectionUtils.isEmpty(streamInfos)) {
            log.warn("skip to delete pulsar resource as no streams for groupId={}", groupId);
            return;
        }

        List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(clusterTag, ClusterType.PULSAR);
        for (ClusterInfo clusterInfo : clusterInfos) {
            PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;
            try {
                for (InlongStreamBriefInfo streamInfo : streamInfos) {
                    this.deletePulsarTopic((InlongPulsarInfo) groupInfo, pulsarCluster, streamInfo.getMqResource());
                }
            } catch (Exception e) {
                String msg = "failed to delete pulsar resource for groupId=" + groupId;
                log.error(msg + ", cluster=" + pulsarCluster, e);
                throw new WorkflowListenerException(msg + ": " + e.getMessage());
            }
        }

        log.info("success to delete pulsar resource for groupId={}, clusterTag={}", groupId, clusterTag);
    }

    @Override
    public void createQueueForStream(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, String operator) {
        Preconditions.expectNotNull(groupInfo, "inlong group info cannot be null");
        Preconditions.expectNotNull(streamInfo, "inlong stream info cannot be null");
        Preconditions.expectNotBlank(operator, ErrorCodeEnum.INVALID_PARAMETER, "operator cannot be null");

        String groupId = streamInfo.getInlongGroupId();
        String streamId = streamInfo.getInlongStreamId();
        String clusterTag = groupInfo.getInlongClusterTag();
        log.info("begin to create pulsar resource for groupId={}, streamId={}, clusterTag={}",
                groupId, streamId, clusterTag);

        // get pulsar cluster via the inlong cluster tag from the inlong group
        List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(clusterTag, ClusterType.PULSAR);
        for (ClusterInfo clusterInfo : clusterInfos) {
            PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;
            try {
                // create pulsar topic and subscription
                String topicName = streamInfo.getMqResource();
                this.createTopic((InlongPulsarInfo) groupInfo, pulsarCluster, topicName);
                this.createSubscription((InlongPulsarInfo) groupInfo, pulsarCluster, topicName, streamId);

                log.info("success to create pulsar resource for groupId={}, streamId={}, topic={}, cluster={}",
                        groupId, streamId, topicName, pulsarCluster);
            } catch (Exception e) {
                String msg = "failed to create pulsar resource for groupId=" + groupId + ", streamId=" + streamId;
                log.error(msg + ", cluster=" + pulsarCluster, e);
                throw new WorkflowListenerException(msg + ": " + e.getMessage());
            }
        }

        log.info("success to create pulsar resource for groupId={}, streamId={}", groupId, streamId);
    }

    @Override
    public void deleteQueueForStream(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, String operator) {
        Preconditions.expectNotNull(groupInfo, "inlong group info cannot be null");
        Preconditions.expectNotNull(streamInfo, "inlong stream info cannot be null");

        String groupId = streamInfo.getInlongGroupId();
        String streamId = streamInfo.getInlongStreamId();
        String clusterTag = groupInfo.getInlongClusterTag();
        log.info("begin to delete pulsar resource for groupId={}, streamId={}, clusterTag={}",
                groupId, streamId, clusterTag);

        List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(clusterTag, ClusterType.PULSAR);
        for (ClusterInfo clusterInfo : clusterInfos) {
            PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;
            try {
                this.deletePulsarTopic((InlongPulsarInfo) groupInfo, pulsarCluster, streamInfo.getMqResource());
                log.info("success to delete pulsar topic for groupId={}, streamId={}, topic={}, cluster={}",
                        groupId, streamId, streamInfo.getMqResource(), pulsarCluster);
            } catch (Exception e) {
                String msg = "failed to delete pulsar topic for groupId=" + groupId + ", streamId=" + streamId;
                log.error(msg + ", cluster=" + pulsarCluster, e);
                throw new WorkflowListenerException(msg + ": " + e.getMessage());
            }
        }

        log.info("success to delete pulsar resource for groupId={}, streamId={}", groupId, streamId);
    }

    /**
     * Create Pulsar Topic and Subscription, and save the consumer group info.
     */
    private void createTopic(InlongPulsarInfo pulsarInfo, PulsarClusterInfo pulsarCluster, String topicName)
            throws Exception {
        String tenant = pulsarInfo.getPulsarTenant();
        if (StringUtils.isBlank(tenant)) {
            tenant = pulsarCluster.getPulsarTenant();
        }
        String namespace = pulsarInfo.getMqResource();
        PulsarTopicInfo topicInfo = PulsarTopicInfo.builder()
                .pulsarTenant(tenant)
                .namespace(namespace)
                .topicName(topicName)
                .queueModule(pulsarInfo.getQueueModule())
                .numPartitions(pulsarInfo.getPartitionNum())
                .build();
        pulsarOperator.createTopic(pulsarCluster, topicInfo);
    }

    /**
     * Create Pulsar Subscription, and save the consumer group info.
     */
    private void createSubscription(InlongPulsarInfo pulsarInfo, PulsarClusterInfo pulsarCluster, String topicName,
            String streamId) throws Exception {
        String tenant = pulsarInfo.getPulsarTenant();
        if (StringUtils.isBlank(tenant)) {
            tenant = pulsarCluster.getPulsarTenant();
        }
        String namespace = pulsarInfo.getMqResource();
        String fullTopicName = tenant + "/" + namespace + "/" + topicName;
        boolean exist = pulsarOperator.topicExists(pulsarCluster, tenant, namespace, topicName,
                InlongConstants.PULSAR_QUEUE_TYPE_PARALLEL.equals(pulsarInfo.getQueueModule()));
        if (!exist) {
            String serviceUrl = pulsarCluster.getAdminUrl();
            log.error("topic={} not exists in {}", fullTopicName, serviceUrl);
            throw new WorkflowListenerException("topic=" + fullTopicName + " not exists in " + serviceUrl);
        }

        // create subscription for all sinks
        String groupId = pulsarInfo.getInlongGroupId();
        List<StreamSink> streamSinks = sinkService.listSink(groupId, streamId);
        if (CollectionUtils.isEmpty(streamSinks)) {
            log.warn("no need to create subs, as no sink exists for groupId={}, streamId={}", groupId, streamId);
            return;
        }

        // subscription naming rules: clusterTag_topicName_sinkId_consumer_group
        String clusterTag = pulsarInfo.getInlongClusterTag();
        for (StreamSink sink : streamSinks) {
            String subs = String.format(PULSAR_SUBSCRIPTION, clusterTag, topicName, sink.getId());
            pulsarOperator.createSubscription(pulsarCluster, fullTopicName, pulsarInfo.getQueueModule(), subs);
            log.info("success to create subs={} for groupId={}, topic={}", subs, groupId, fullTopicName);

            // insert the consumer group info into the inlong_consume table
            Integer id = consumeService.saveBySystem(pulsarInfo, topicName, subs);
            log.info("success to save inlong consume [{}] for subs={}, groupId={}, topic={}",
                    id, subs, groupId, topicName);
        }
    }

    /**
     * Delete Pulsar Topic and Subscription, and delete the consumer group info.
     * TODO delete Subscription and InlongConsume info
     */
    private void deletePulsarTopic(InlongPulsarInfo pulsarInfo, PulsarClusterInfo pulsarCluster, String topicName)
            throws Exception {
        String tenant = pulsarInfo.getPulsarTenant();
        if (StringUtils.isBlank(tenant)) {
            tenant = pulsarCluster.getPulsarTenant();
        }
        String namespace = pulsarInfo.getMqResource();
        PulsarTopicInfo topicInfo = PulsarTopicInfo.builder()
                .pulsarTenant(tenant)
                .namespace(namespace)
                .topicName(topicName)
                .build();
        pulsarOperator.forceDeleteTopic(pulsarCluster, topicInfo);
    }

    /**
     * Query latest message from pulsar
     */
    public List<BriefMQMessage> queryLatestMessages(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            QueryMessageRequest request) throws Exception {
        List<ClusterInfo> pulsarClusterList = clusterService.listByTagAndType(groupInfo.getInlongClusterTag(),
                ClusterType.PULSAR);
        List<BriefMQMessage> briefMQMessages = Collections.synchronizedList(new ArrayList<>());
        QueryCountDownLatch queryLatch = new QueryCountDownLatch(request.getMessageCount(), pulsarClusterList.size());
        InlongPulsarInfo inlongPulsarInfo = ((InlongPulsarInfo) groupInfo);
        for (ClusterInfo clusterInfo : pulsarClusterList) {
            QueryLatestMessagesRunnable task = new QueryLatestMessagesRunnable(inlongPulsarInfo, streamInfo,
                    (PulsarClusterInfo) clusterInfo, pulsarOperator, request, briefMQMessages, queryLatch);
            this.executor.execute(task);
        }
        queryLatch.await(30, TimeUnit.SECONDS);
        log.info("success query pulsar message for groupId={}, streamId={}", streamInfo.getInlongGroupId(),
                streamInfo.getInlongStreamId());

        int finalMsgCount = Math.min(request.getMessageCount(), briefMQMessages.size());
        if (finalMsgCount > 0) {
            return new ArrayList<>(briefMQMessages.subList(0, finalMsgCount));
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Reset cursor for consumer group
     */
    public void resetCursor(InlongGroupInfo groupInfo, InlongStreamEntity streamEntity, StreamSinkEntity sinkEntity,
            Long resetTime) throws Exception {
        log.info("begin to reset cursor for sinkId={}", sinkEntity.getId());
        InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) groupInfo;
        List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(pulsarInfo.getInlongClusterTag(),
                ClusterType.PULSAR);
        for (ClusterInfo clusterInfo : clusterInfos) {
            PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;
            try {
                String tenant = pulsarInfo.getPulsarTenant();
                if (StringUtils.isBlank(tenant)) {
                    tenant = pulsarCluster.getPulsarTenant();
                }
                String namespace = pulsarInfo.getMqResource();
                String topicName = streamEntity.getMqResource();
                String fullTopicName = tenant + "/" + namespace + "/" + topicName;
                String subs = String.format(PULSAR_SUBSCRIPTION, groupInfo.getInlongClusterTag(), topicName,
                        sinkEntity.getId());
                pulsarOperator.resetCursor(pulsarCluster, fullTopicName, subs, resetTime);
            } catch (Exception e) {
                log.error("failed reset cursor consumer:", e);
                throw new BusinessException("failed reset cursor consumer:" + e.getMessage());
            }
        }
        log.info("success to reset cursor for sinkId={}", sinkEntity.getId());
    }

    @Override
    public String getSortConsumeGroup(InlongGroupInfo groupInfo, InlongStreamEntity streamEntity,
            StreamSinkEntity sinkEntity) {
        String topicName = streamEntity.getMqResource();
        return String.format(PULSAR_SUBSCRIPTION, groupInfo.getInlongClusterTag(), topicName,
                sinkEntity.getId());
    }
}
