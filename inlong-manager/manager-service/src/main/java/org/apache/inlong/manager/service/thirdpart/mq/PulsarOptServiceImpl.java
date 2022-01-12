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

package org.apache.inlong.manager.service.thirdpart.mq;

import com.google.common.collect.Sets;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.conversion.ConversionHandle;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.pojo.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.BusinessPulsarEntity;
import org.apache.inlong.manager.service.thirdpart.mq.util.PulsarUtils;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Operation interface of Pulsar
 */
@Service
@Slf4j
public class PulsarOptServiceImpl implements PulsarOptService {

    @Autowired
    private ConversionHandle conversionHandle;

    @Override
    public void createTenant(PulsarAdmin pulsarAdmin, String tenant) throws PulsarAdminException {
        log.info("begin to create tenant={}", tenant);

        Preconditions.checkNotEmpty(tenant, "Tenant cannot be empty");
        try {
            List<String> clusters = PulsarUtils.getPulsarClusters(pulsarAdmin);
            boolean exists = this.tenantIsExists(pulsarAdmin, tenant);
            if (exists) {
                log.warn("pulsar tenant={} already exists, skip to create", tenant);
                return;
            }
            TenantInfoImpl tenantInfo = new TenantInfoImpl();
            tenantInfo.setAllowedClusters(Sets.newHashSet(clusters));
            tenantInfo.setAdminRoles(Sets.newHashSet());
            pulsarAdmin.tenants().createTenant(tenant, tenantInfo);
            log.info("success to create pulsar tenant={}", tenant);
        } catch (PulsarAdminException e) {
            log.error("create pulsar tenant={} failed", tenant, e);
            throw e;
        }
    }

    @Override
    public void createNamespace(PulsarAdmin pulsarAdmin, BusinessPulsarEntity pulsarEntity,
            String tenant, String namespace) throws PulsarAdminException {
        Preconditions.checkNotNull(tenant, "pulsar tenant cannot be empty during create namespace");
        Preconditions.checkNotNull(namespace, "pulsar namespace cannot be empty during create namespace");

        String namespaceName = tenant + "/" + namespace;
        log.info("begin to create namespace={}", namespaceName);

        try {
            // Check whether the namespace exists, and create it if it does not exist
            boolean isExists = this.namespacesIsExists(pulsarAdmin, tenant, namespace);
            if (isExists) {
                log.warn("namespace={} already exists, skip to create", namespaceName);
                return;
            }

            List<String> clusters = PulsarUtils.getPulsarClusters(pulsarAdmin);
            Namespaces namespaces = pulsarAdmin.namespaces();
            namespaces.createNamespace(namespaceName, Sets.newHashSet(clusters));

            // Configure message TTL
            Integer ttl = pulsarEntity.getTtl();
            if (ttl > 0) {
                namespaces.setNamespaceMessageTTL(namespaceName, conversionHandle.handleConversion(ttl,
                        pulsarEntity.getTtlUnit().toLowerCase() + "_seconds"));
            }

            // retentionTimeInMinutes retentionSizeInMB
            Integer retentionTime = pulsarEntity.getRetentionTime();
            if (retentionTime > 0) {
                retentionTime = conversionHandle.handleConversion(retentionTime,
                        pulsarEntity.getRetentionTimeUnit().toLowerCase() + "_minutes");
            }
            Integer retentionSize = pulsarEntity.getRetentionSize();
            if (retentionSize > 0) {
                retentionSize = conversionHandle.handleConversion(retentionSize,
                        pulsarEntity.getRetentionSizeUnit().toLowerCase() + "_mb");
            }

            // Configure retention policies
            RetentionPolicies retentionPolicies = new RetentionPolicies(retentionTime, retentionSize);
            namespaces.setRetention(namespaceName, retentionPolicies);

            // Configure persistence policies
            PersistencePolicies persistencePolicies = new PersistencePolicies(pulsarEntity.getEnsemble(),
                    pulsarEntity.getWriteQuorum(), pulsarEntity.getAckQuorum(), 0);
            namespaces.setPersistence(namespaceName, persistencePolicies);

            log.info("success to create namespace={}", tenant);
        } catch (PulsarAdminException e) {
            log.error("create namespace={} error", tenant, e);
            throw e;
        }
    }

    @Override
    public void createTopic(PulsarAdmin pulsarAdmin, PulsarTopicBean topicBean) throws PulsarAdminException {
        Preconditions.checkNotNull(topicBean, "pulsar topic info cannot be empty");

        String tenant = topicBean.getTenant();
        String namespace = topicBean.getNamespace();
        String topic = topicBean.getTopicName();
        String topicFullName = tenant + "/" + namespace + "/" + topic;

        // Topic will be returned if it exists, and created if it does not exist
        if (topicIsExists(pulsarAdmin, tenant, namespace, topic)) {
            log.warn("pulsar topic={} already exists in {}", topicFullName, pulsarAdmin.getServiceUrl());
            return;
        }

        try {
            String queueModule = topicBean.getQueueModule();
            // create partition topic
            if (BizConstant.PULSAR_TOPIC_TYPE_SERIAL.equalsIgnoreCase(queueModule)) {
                pulsarAdmin.topics().createPartitionedTopic(topicFullName, 1);
            } else {
                List<String> clusters = PulsarUtils.getPulsarClusters(pulsarAdmin);
                // The number of brokers as the default value of topic partition
                List<String> brokers = pulsarAdmin.brokers().getActiveBrokers(clusters.get(0));
                Integer numPartitions = brokers.size();
                if (topicBean.getNumPartitions() > 0) {
                    numPartitions = topicBean.getNumPartitions();
                }
                pulsarAdmin.topics().createPartitionedTopic(topicFullName, numPartitions);
            }

            log.info("success to create topic={}", topicFullName);
        } catch (Exception e) {
            log.error("create topic={} failed", topicFullName, e);
            throw e;
        }
    }

    @Override
    public void createSubscription(PulsarAdmin pulsarAdmin, PulsarTopicBean topicBean, String subscription)
            throws PulsarAdminException {
        Preconditions.checkNotNull(topicBean, "can not find tenant information to create subscription");
        Preconditions.checkNotNull(subscription, "subscription cannot be empty during creating subscription");

        String topicName = topicBean.getTenant() + "/" + topicBean.getNamespace() + "/" + topicBean.getTopicName();
        log.info("begin to create pulsar subscription={} for topic={}", subscription, topicName);

        try {
            boolean isExists = this.subscriptionIsExists(pulsarAdmin, topicName, subscription);
            if (!isExists) {
                pulsarAdmin.topics().createSubscription(topicName, subscription, MessageId.latest);
                log.info("success to create subscription={}", subscription);
            } else {
                log.warn("pulsar subscription={} already exists, skip to create", subscription);
            }
        } catch (Exception e) {
            log.error("create pulsar subscription={} failed", subscription, e);
            throw e;
        }
    }

    @Override
    public void createSubscriptions(PulsarAdmin pulsarAdmin, String subscription, PulsarTopicBean topicBean,
            List<String> topicList) throws PulsarAdminException {
        for (String topic : topicList) {
            topicBean.setTopicName(topic);
            this.createSubscription(pulsarAdmin, topicBean, subscription);
        }
        log.info("success to create subscription={} for multiple topics={}", subscription, topicList);
    }

    /**
     * Check if Pulsar tenant exists
     */
    private boolean tenantIsExists(PulsarAdmin pulsarAdmin, String tenant) throws PulsarAdminException {
        List<String> tenantList = pulsarAdmin.tenants().getTenants();
        return tenantList.contains(tenant);
    }

    /**
     * Check whether the specified Namespace exists under the specified Tenant
     */
    private boolean namespacesIsExists(PulsarAdmin pulsarAdmin, String tenant, String namespace)
            throws PulsarAdminException {
        List<String> namespaceList = pulsarAdmin.namespaces().getNamespaces(tenant);
        return namespaceList.contains(tenant + "/" + namespace);
    }

    /**
     * Verify whether the specified Topic exists under the specified Tenant/Namespace
     *
     * @apiNote cannot compare whether the string contains, otherwise it may be misjudged, such as:
     *         Topic "ab" does not exist, but if "abc" exists, "ab" will be mistakenly judged to exist
     */
    @Override
    public boolean topicIsExists(PulsarAdmin pulsarAdmin, String tenant, String namespace, String topic)
            throws PulsarAdminException {
        if (StringUtils.isBlank(topic)) {
            return true;
        }

        // persistent://tenant/namespace/topic
        List<String> topicList = pulsarAdmin.topics().getPartitionedTopicList(tenant + "/" + namespace);
        for (String t : topicList) {
            t = t.substring(t.lastIndexOf("/") + 1); // Cannot contain /
            if (topic.equals(t)) {
                return true;
            }
        }
        return false;
    }

    public boolean subscriptionIsExists(PulsarAdmin pulsarAdmin, String topic, String subscription) {
        try {
            List<String> subscriptionList = pulsarAdmin.topics().getSubscriptions(topic);
            return subscriptionList.contains(subscription);
        } catch (PulsarAdminException e) {
            log.error("check if the topic={} is exists error,", topic, e);
            return false;
        }
    }
}
