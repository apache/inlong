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

import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.conversion.ConversionHandle;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarMessageInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarNamespacePolicies;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarPersistencePolicies;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarRetentionPolicies;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTenantInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicMetadata;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.QueryMessageRequest;
import org.apache.inlong.manager.service.cluster.InlongClusterServiceImpl;
import org.apache.inlong.manager.service.message.DeserializeOperator;
import org.apache.inlong.manager.service.message.DeserializeOperatorFactory;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pulsar operator, supports creating topics and creating subscription.
 */
@Service
public class PulsarOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongClusterServiceImpl.class);
    /**
     * The maximum number of partitions, which is an empirical value,
     * generally does not exceed 1000 in large clusters.
     */
    private static final int MAX_PARTITION = 1000;
    private static final int RETRY_TIMES = 3;
    private static final int DELAY_SECONDS = 5;
    @Autowired
    public DeserializeOperatorFactory deserializeOperatorFactory;
    @Autowired
    private ConversionHandle conversionHandle;
    @Autowired
    private RestTemplate restTemplate;

    /**
     * Create Pulsar tenant
     */
    public void createTenant(PulsarClusterInfo pulsarClusterInfo, String tenant) throws Exception {
        LOGGER.info("begin to create pulsar tenant={}", tenant);
        Preconditions.expectNotBlank(tenant, ErrorCodeEnum.INVALID_PARAMETER, "Tenant cannot be empty");

        try {
            List<String> clusters = PulsarUtils.getClusters(restTemplate, pulsarClusterInfo);
            boolean exists = this.tenantIsExists(pulsarClusterInfo, tenant);
            if (exists) {
                LOGGER.warn("pulsar tenant={} already exists, skip to create", tenant);
                return;
            }
            PulsarTenantInfo tenantInfo = new PulsarTenantInfo();
            tenantInfo.setAllowedClusters(Sets.newHashSet(clusters));
            tenantInfo.setAdminRoles(Sets.newHashSet());
            PulsarUtils.createTenant(restTemplate, pulsarClusterInfo, tenant, tenantInfo);
            LOGGER.info("success to create pulsar tenant={}", tenant);
        } catch (Exception e) {
            LOGGER.error("failed to create pulsar tenant=" + tenant, e);
            throw e;
        }
    }

    /**
     * Create Pulsar namespace.
     */
    public void createNamespace(PulsarClusterInfo pulsarClusterInfo, InlongPulsarInfo pulsarInfo, String tenant,
            String namespace)
            throws Exception {
        Preconditions.expectNotBlank(tenant, ErrorCodeEnum.INVALID_PARAMETER,
                "pulsar tenant cannot be empty during create namespace");
        Preconditions.expectNotBlank(namespace, ErrorCodeEnum.INVALID_PARAMETER,
                "pulsar namespace cannot be empty during create namespace");

        String tenantNamespaceName = tenant + "/" + namespace;
        LOGGER.info("begin to create namespace={}", tenantNamespaceName);
        try {
            // Check whether the namespace exists, and create it if it does not exist
            boolean isExists = this.namespaceExists(pulsarClusterInfo, tenant, tenantNamespaceName);
            if (isExists) {
                LOGGER.warn("namespace={} already exists, skip to create", tenantNamespaceName);
                return;
            }

            PulsarNamespacePolicies policies = new PulsarNamespacePolicies();
            // Configure message TTL
            Integer ttl = pulsarInfo.getTtl();
            if (ttl > 0) {
                policies.setMessageTtlInSeconds(conversionHandle.handleConversion(ttl,
                        pulsarInfo.getTtlUnit().toLowerCase() + "_seconds"));
            }

            // retentionTimeInMinutes retentionSizeInMB
            Integer retentionTime = pulsarInfo.getRetentionTime();
            if (retentionTime > 0) {
                retentionTime = conversionHandle.handleConversion(retentionTime,
                        pulsarInfo.getRetentionTimeUnit().toLowerCase() + "_minutes");
            }
            Integer retentionSize = pulsarInfo.getRetentionSize();
            if (retentionSize > 0) {
                retentionSize = conversionHandle.handleConversion(retentionSize,
                        pulsarInfo.getRetentionSizeUnit().toLowerCase() + "_mb");
            }

            // Configure retention policies
            PulsarRetentionPolicies retentionPolicies = new PulsarRetentionPolicies(retentionTime, retentionSize);
            policies.setRetentionPolicies(retentionPolicies);

            // Configure persistence policies
            PulsarPersistencePolicies persistencePolicies = new PulsarPersistencePolicies(pulsarInfo.getEnsemble(),
                    pulsarInfo.getWriteQuorum(), pulsarInfo.getAckQuorum(), pulsarInfo.getMaxMarkDeleteRate());
            policies.setPersistence(persistencePolicies);

            PulsarUtils.createNamespace(restTemplate, pulsarClusterInfo, tenant, namespace, policies);
            LOGGER.info("success to create namespace={}", tenantNamespaceName);
        } catch (Exception e) {
            LOGGER.error("failed to create namespace=" + tenantNamespaceName, e);
            throw e;
        }
    }

    /**
     * Create Pulsar topic
     */
    public void createTopic(PulsarClusterInfo pulsarClusterInfo, PulsarTopicInfo topicInfo) throws Exception {
        Preconditions.expectNotNull(topicInfo, "pulsar topic info cannot be empty");
        String tenant = topicInfo.getPulsarTenant();
        String namespace = topicInfo.getNamespace();
        String topicName = topicInfo.getTopicName();
        String fullTopicName = tenant + "/" + namespace + "/" + topicName;

        // Topic will be returned if it exists, and created if it does not exist
        if (topicExists(pulsarClusterInfo, tenant, namespace, topicName,
                InlongConstants.PULSAR_QUEUE_TYPE_PARALLEL.equals(topicInfo.getQueueModule()))) {
            LOGGER.warn("pulsar topic={} already exists in {}", fullTopicName, pulsarClusterInfo.getAdminUrl());
            return;
        }

        try {
            if (InlongConstants.PULSAR_QUEUE_TYPE_SERIAL.equals(topicInfo.getQueueModule())) {
                PulsarUtils.createNonPartitionedTopic(restTemplate, pulsarClusterInfo, fullTopicName);
                String res = PulsarUtils.lookupTopic(restTemplate, pulsarClusterInfo, fullTopicName);
                LOGGER.info("success to create topic={}, lookup result is {}", fullTopicName, res);
            } else {
                // The number of brokers as the default value of topic partition
                List<String> clusters = PulsarUtils.getClusters(restTemplate, pulsarClusterInfo);
                Integer numPartitions = topicInfo.getNumPartitions();
                if (numPartitions < 0 || numPartitions >= MAX_PARTITION) {
                    List<String> brokers = PulsarUtils.getBrokers(restTemplate, pulsarClusterInfo);
                    numPartitions = brokers.size();
                }
                PulsarUtils.createPartitionedTopic(restTemplate, pulsarClusterInfo, fullTopicName,
                        numPartitions);
                Map<String, String> res = PulsarUtils.lookupPartitionedTopic(restTemplate,
                        pulsarClusterInfo, fullTopicName);
                // if lookup failed (res.size not equals the partition number)
                if (res.size() != numPartitions) {
                    // look up partition failed, retry to get partition numbers
                    for (int i = 0; (i < RETRY_TIMES && res.size() != numPartitions); i++) {
                        res = PulsarUtils.lookupPartitionedTopic(restTemplate, pulsarClusterInfo,
                                fullTopicName);
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            LOGGER.error("Thread has been interrupted");
                        }
                    }
                }
                if (numPartitions != res.size()) {
                    throw new Exception("The number of partitions not equal to lookupPartitionedTopic");
                }
                LOGGER.info("success to create topic={}", fullTopicName);
            }
        } catch (Exception e) {
            LOGGER.error("failed to create topic=" + fullTopicName, e);
            throw e;
        }
    }

    /**
     * Force delete Pulsar topic
     */
    public void forceDeleteTopic(PulsarClusterInfo pulsarClusterInfo, PulsarTopicInfo topicInfo) throws Exception {
        Preconditions.expectNotNull(topicInfo, "pulsar topic info cannot be empty");

        String tenant = topicInfo.getPulsarTenant();
        String namespace = topicInfo.getNamespace();
        String topic = topicInfo.getTopicName();
        String fullTopicName = tenant + "/" + namespace + "/" + topic;
        boolean isPartitioned = InlongConstants.PULSAR_QUEUE_TYPE_PARALLEL.equals(topicInfo.getQueueModule());

        // Topic will be returned if it not exists
        if (topicExists(pulsarClusterInfo, tenant, namespace, topic, isPartitioned)) {
            LOGGER.warn("pulsar topic={} already delete", fullTopicName);
            return;
        }

        try {
            PulsarUtils.forceDeleteTopic(restTemplate, pulsarClusterInfo, fullTopicName, isPartitioned);
            LOGGER.info("success to delete topic={}", fullTopicName);
        } catch (Exception e) {
            LOGGER.error("failed to delete topic=" + fullTopicName, e);
            throw e;
        }
    }

    /**
     * Create a Pulsar subscription for the given topic
     */
    public void createSubscription(PulsarClusterInfo pulsarClusterInfo, String fullTopicName, String queueModule,
            String subscription) throws Exception {
        LOGGER.info("begin to create pulsar subscription={} for topic={}", subscription, fullTopicName);
        try {
            boolean isExists = this.subscriptionExists(pulsarClusterInfo, fullTopicName, subscription,
                    InlongConstants.PULSAR_QUEUE_TYPE_PARALLEL.equals(queueModule));
            if (isExists) {
                LOGGER.warn("pulsar subscription={} already exists, skip to create", subscription);
                return;
            }

            PulsarUtils.createSubscription(restTemplate, pulsarClusterInfo, fullTopicName, subscription);
            LOGGER.info("success to create subscription={}", subscription);
        } catch (Exception e) {
            LOGGER.error("failed to create pulsar subscription=" + subscription, e);
            throw e;
        }
    }

    /**
     * Create a Pulsar subscription for the specified topic list
     */
    public void createSubscriptions(PulsarClusterInfo pulsarClusterInfo, String subscription, PulsarTopicInfo topicInfo,
            List<String> topicList) throws Exception {
        for (String topic : topicList) {
            topicInfo.setTopicName(topic);
            String fullTopicName = topicInfo.getPulsarTenant() + "/" + topicInfo.getNamespace() + "/" + topic;
            this.createSubscription(pulsarClusterInfo, fullTopicName, topicInfo.getQueueModule(), subscription);
        }
        LOGGER.info("success to create subscription={} for multiple topics={}", subscription, topicList);
    }

    /**
     * Check if Pulsar tenant exists
     *
     * @param pulsarClusterInfo pulsar cluster info
     * @param tenant pulsar tenant info
     * @return true or false
     * @throws Exception any exception if occurred
     */
    private boolean tenantIsExists(PulsarClusterInfo pulsarClusterInfo, String tenant) throws Exception {
        List<String> tenants = PulsarUtils.getTenants(restTemplate, pulsarClusterInfo);
        return tenants.contains(tenant);
    }

    /**
     * Check whether the Pulsar namespace exists under the specified tenant.
     *
     * @param pulsarClusterInfo pulsar cluster info
     * @param tenant pulsar tenant info
     * @param namespace pulsar namespace info
     * @return true or false
     * @throws Exception any exception if occurred
     */
    private boolean namespaceExists(PulsarClusterInfo pulsarClusterInfo, String tenant, String namespace)
            throws Exception {
        List<String> namespaces = PulsarUtils.getNamespaces(restTemplate, pulsarClusterInfo, tenant);
        return namespaces.contains(namespace);
    }

    /**
     * Verify whether the specified Topic exists under the specified Tenant/Namespace
     *
     * @apiNote cannot compare whether the string contains, otherwise it may be misjudged, such as:
     *         Topic "ab" does not exist, but if "abc" exists, "ab" will be mistakenly judged to exist
     */
    public boolean topicExists(PulsarClusterInfo pulsarClusterInfo, String tenant, String namespace, String topicName,
            boolean isPartitioned) {
        if (StringUtils.isBlank(topicName)) {
            return true;
        }

        // persistent://tenant/namespace/topic
        List<String> topics;
        boolean topicExists = false;
        try {
            if (isPartitioned) {
                topics = PulsarUtils.getPartitionedTopics(restTemplate, pulsarClusterInfo, tenant,
                        namespace);
            } else {
                topics = PulsarUtils.getTopics(restTemplate, pulsarClusterInfo, tenant, namespace);
            }
            for (String t : topics) {
                t = t.substring(t.lastIndexOf("/") + 1); // not contains /
                if (!isPartitioned) {
                    int suffixIndex = t.lastIndexOf("-partition-");
                    if (suffixIndex > 0) {
                        t = t.substring(0, suffixIndex);
                    }
                }
                if (topicName.equals(t)) {
                    topicExists = true;
                    break;
                }
            }
        } catch (Exception pe) {
            LOGGER.error("check if the pulsar topic={} exists error, begin retry", topicName, pe);
            int count = 0;
            try {
                while (!topicExists && ++count <= RETRY_TIMES) {
                    LOGGER.info("check whether the pulsar topic={} exists error, try count={}", topicName, count);
                    Thread.sleep(DELAY_SECONDS);

                    topics = PulsarUtils.getPartitionedTopics(restTemplate, pulsarClusterInfo,
                            tenant, namespace);
                    for (String t : topics) {
                        t = t.substring(t.lastIndexOf("/") + 1);
                        if (topicName.equals(t)) {
                            topicExists = true;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("after retry, check if the pulsar topic={} exists still error", topicName, pe);
            }
        }
        return topicExists;
    }

    /**
     * Check whether the Pulsar topic exists.
     */
    private boolean subscriptionExists(PulsarClusterInfo pulsarClusterInfo, String topic, String subscription,
            boolean isPartitioned) {
        int count = 0;
        while (++count <= RETRY_TIMES) {
            try {
                LOGGER.info("check whether the subscription exists for topic={}, try count={}", topic, count);
                Thread.sleep(DELAY_SECONDS);

                // first lookup to load the topic, and then query whether the subscription exists
                if (isPartitioned) {
                    Map<String, String> topicMap = PulsarUtils.lookupPartitionedTopic(restTemplate,
                            pulsarClusterInfo, topic);
                    if (topicMap.isEmpty()) {
                        LOGGER.error("result of lookups topic={} is empty, continue retry", topic);
                        continue;
                    }
                } else {
                    String lookupTopic = PulsarUtils.lookupTopic(restTemplate, pulsarClusterInfo, topic);
                    if (StringUtils.isBlank(lookupTopic)) {
                        LOGGER.error("result of lookups topic={} is empty, continue retry", topic);
                        continue;
                    }
                }

                List<String> subscriptionList = PulsarUtils.getSubscriptions(restTemplate,
                        pulsarClusterInfo, topic);
                return subscriptionList.contains(subscription);
            } catch (Exception e) {
                LOGGER.error("check if the subscription exists for topic={} error, continue retry", topic, e);
                if (count == RETRY_TIMES) {
                    LOGGER.error("after {} times retry, still check subscription exception for topic {}", count, topic);
                    throw new BusinessException("check if the subscription exists error: " + e.getMessage());
                }
            }
        }
        return false;
    }

    /**
     * Query topic message for the given pulsar cluster.
     */
    public List<BriefMQMessage> queryLatestMessage(PulsarClusterInfo pulsarClusterInfo, String topicFullName,
            QueryMessageRequest request, InlongStreamInfo streamInfo, boolean serial) {
        LOGGER.info("begin to query message for topic {}, adminUrl={}", topicFullName, pulsarClusterInfo.getAdminUrl());
        List<BriefMQMessage> messageList = new ArrayList<>();
        int partitionCount = getPartitionCount(pulsarClusterInfo, topicFullName);
        for (int messageIndex = 0; messageIndex < 100; messageIndex++) {
            int currentPartitionNum = messageIndex % partitionCount;
            int messagePosition = messageIndex / partitionCount + 1;
            String topicNameOfPartition = buildTopicNameOfPartition(topicFullName, currentPartitionNum, serial);
            messageList.addAll(queryMessageFromPulsar(topicNameOfPartition, pulsarClusterInfo, messageIndex, streamInfo,
                    messagePosition, request));
        }
        LOGGER.info("success query message for topic={}", topicFullName);
        return messageList;
    }

    /**
     * Get topic partition count.
     */
    private int getPartitionCount(PulsarClusterInfo pulsarClusterInfo, String topicFullName) {
        PulsarTopicMetadata pulsarTopicMetadata;
        try {
            pulsarTopicMetadata = PulsarUtils.getPartitionedTopicMetadata(restTemplate,
                    pulsarClusterInfo, topicFullName);
        } catch (Exception e) {
            String errMsg = "get pulsar partition error ";
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg + e.getMessage());
        }
        return pulsarTopicMetadata.getPartitions() > 0 ? pulsarTopicMetadata.getPartitions() : 1;
    }

    /**
     * Query pulsar message.
     */
    private List<BriefMQMessage> queryMessageFromPulsar(String topicPartition, PulsarClusterInfo pulsarClusterInfo,
            int index, InlongStreamInfo streamInfo, int messagePosition, QueryMessageRequest request) {
        List<BriefMQMessage> briefMQMessages = new ArrayList<>();
        try {
            ResponseEntity<byte[]> httpResponse =
                    PulsarUtils.examineMessage(restTemplate, pulsarClusterInfo, topicPartition, "latest",
                            messagePosition);
            PulsarMessageInfo messageInfo = PulsarUtils.getMessageFromHttpResponse(httpResponse, topicPartition);
            Map<String, String> headers = messageInfo.getProperties();
            if (headers == null) {
                headers = new HashMap<>();
            }
            MessageWrapType messageWrapType = MessageWrapType.forType(streamInfo.getWrapType());
            if (headers.get(InlongConstants.MSG_ENCODE_VER) != null) {
                messageWrapType =
                        MessageWrapType.valueOf(Integer.parseInt(headers.get(InlongConstants.MSG_ENCODE_VER)));
            }
            DeserializeOperator deserializeOperator = deserializeOperatorFactory.getInstance(messageWrapType);
            deserializeOperator.decodeMsg(streamInfo, briefMQMessages, messageInfo.getBody(), headers, index, request);
        } catch (Exception e) {
            LOGGER.warn("query message from pulsar error for groupId = {}, streamId = {}, adminUrl={}",
                    streamInfo.getInlongGroupId(), streamInfo.getInlongStreamId(), pulsarClusterInfo.getAdminUrl(), e);
        }
        return briefMQMessages;
    }

    /**
     * Reset cursor for consumer group.
     */
    public void resetCursor(PulsarClusterInfo pulsarClusterInfo, String topicFullName, String subName,
            Long resetTime) {
        try {
            PulsarUtils.resetCursor(restTemplate, pulsarClusterInfo, topicFullName, subName,
                    resetTime);
        } catch (Exception e) {
            LOGGER.error("failed reset cursor consumer:", e);
            throw new BusinessException("failed reset cursor consumer:" + e.getMessage());
        }
    }

    /**
     * Build topicName Of Partition
     */
    private String buildTopicNameOfPartition(String topicName, int partition, boolean serial) {
        if (serial) {
            return topicName;
        }
        return topicName + "-partition-" + partition;
    }
}
