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

package org.apache.inlong.manager.service.cluster.queue;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueClearTopicRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueControlRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueOfflineRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueOnlineRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueSynchronizeTopicRequest;
import org.apache.inlong.manager.pojo.cluster.tubemq.TubeClusterInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.service.resource.queue.tubemq.TubeMQOperator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Inlong message queue cluster operator.
 */
@Service
public class MessageQueueServiceImpl implements MessageQueueService {

    public static final Logger LOG = LoggerFactory.getLogger(MessageQueueServiceImpl.class);

    public static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on(AttributeConstants.SEPARATOR)
            .trimResults().withKeyValueSeparator(AttributeConstants.KEY_VALUE_SEPARATOR);
    public static final Joiner.MapJoiner MAP_JOINER = Joiner.on(AttributeConstants.SEPARATOR)
            .withKeyValueSeparator(AttributeConstants.KEY_VALUE_SEPARATOR);
    public static final String KEY_PRODUCER = "producer";
    public static final String KEY_CONSUMER = "consumer";
    public static final String KEY_ADMIN_URL = "adminUrl";
    public static final String KEY_AUTHENTICATION = "authentication";
    public static final String KEY_TENANT = "tenant";
    public static final String KEY_NAMESPACE = "namespace";
    public static final String KEY_NUM_PARTITIONS = "numPartitions";
    public static final String KEY_REPLICATION_FACTOR = "replicationFactor";
    public static final int DEFAULT_NUM_PARTITIONS = 10;
    public static final short DEFAULT_REPLICATION_FACTOR = 2;

    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private TubeMQOperator tubeMQOperator;

    /**
     * Control produce operation and consume operation of Inlong message queue cluster 
     */
    @Override
    public void control(MessageQueueControlRequest request) {
        String name = request.getName();
        // check parameters
        if (StringUtils.isEmpty(name)) {
            throw new BusinessException("miss cluster name.");
        }
        Boolean canProduce = request.getCanProduce();
        if (canProduce == null) {
            canProduce = Boolean.FALSE;
        }
        Boolean canConsume = request.getCanConsume();
        if (canConsume == null) {
            canConsume = Boolean.FALSE;
        }
        // find cluster
        List<InlongClusterEntity> clusters = clusterMapper.selectByKey(null, name, null);
        if (clusters.size() <= 0) {
            throw new BusinessException("Can not find a cluster by name:" + name);
        }
        if (clusters.size() > 1) {
            throw new BusinessException(String.format("Cluster:%s data is more than 1.", name));
        }
        for (InlongClusterEntity cluster : clusters) {
            String strExtTag = cluster.getExtTag();
            strExtTag = StringUtils.trimToEmpty(strExtTag);
            Map<String, String> extTagMap = new HashMap<>(MAP_SPLITTER.split(strExtTag));
            extTagMap.put(KEY_PRODUCER, canProduce.toString());
            extTagMap.put(KEY_CONSUMER, canConsume.toString());
            String newExtTag = MAP_JOINER.join(extTagMap);
            cluster.setExtTag(newExtTag);
            clusterMapper.updateById(cluster);
        }
    }

    /**
     * Build relationships between DataProxy cluster and MessageQueue cluster
     */
    @Override
    public void online(MessageQueueOnlineRequest request) {
        String mqClusterName = request.getMqClusterName();
        String proxyClusterName = request.getProxyClusterName();
        this.updateExtTag(mqClusterName, proxyClusterName, true);
    }

    /**
     * Remove relationships between DataProxy cluster and MessageQueue cluster
     */
    @Override
    public void offline(MessageQueueOfflineRequest request) {
        String mqClusterName = request.getMqClusterName();
        String proxyClusterName = request.getProxyClusterName();
        this.updateExtTag(mqClusterName, proxyClusterName, false);
    }

    /**
     * updateExtTag
     */
    private void updateExtTag(String mqClusterName, String proxyClusterName, boolean isAppendTag) {
        // check parameters
        if (StringUtils.isEmpty(mqClusterName)) {
            throw new BusinessException("miss message queue cluster name.");
        }
        if (StringUtils.isEmpty(proxyClusterName)) {
            throw new BusinessException("miss DataProxy cluster name.");
        }
        // find cluster
        List<InlongClusterEntity> mqClusters = clusterMapper.selectByKey(null, mqClusterName, null);
        if (mqClusters.size() <= 0) {
            throw new BusinessException("Can not find message queue cluster by name:" + mqClusterName);
        }
        if (mqClusters.size() > 1) {
            throw new BusinessException(String.format("MessageQueue cluster:%s data is more than 1.", mqClusterName));
        }
        List<InlongClusterEntity> proxyClusters = clusterMapper.selectByKey(null, proxyClusterName, null);
        if (proxyClusters.size() <= 0) {
            throw new BusinessException("Can not find DataProxy cluster by name:" + proxyClusterName);
        }
        if (proxyClusters.size() > 1) {
            throw new BusinessException(String.format("DataProxy cluster:%s data is more than 1.", proxyClusterName));
        }

        // parse DataProxy extTag
        InlongClusterEntity proxyCluster = proxyClusters.get(0);
        String strProxyExtTag = proxyCluster.getExtTag();
        strProxyExtTag = StringUtils.trimToEmpty(strProxyExtTag);
        Map<String, String> proxyExtTagMap = MAP_SPLITTER.split(strProxyExtTag);
        // parse MessageQueue extTag
        InlongClusterEntity mqCluster = mqClusters.get(0);
        String strMqExtTag = mqCluster.getExtTag();
        strMqExtTag = StringUtils.trimToEmpty(strMqExtTag);
        Map<String, String> mqExtTagMap = new HashMap<>(MAP_SPLITTER.split(strMqExtTag));
        // update extTag
        if (isAppendTag) {
            // append DataProxy extTag to MessageQueue extTag
            proxyExtTagMap.forEach((k, v) -> mqExtTagMap.put(k, v));
        } else {
            // remove DataProxy extTag from MessageQueue extTag
            proxyExtTagMap.forEach((k, v) -> mqExtTagMap.remove(k));
        }
        // update MessageQueue
        String newExtTag = MAP_JOINER.join(mqExtTagMap);
        mqCluster.setExtTag(newExtTag);
        clusterMapper.updateById(mqCluster);
    }

    /**
     * Synchronize all topic from cluster tag to message queue cluster
     */
    @Override
    public void synchronizeTopic(MessageQueueSynchronizeTopicRequest request) {
        String mqClusterName = request.getName();
        // check parameters
        if (StringUtils.isEmpty(mqClusterName)) {
            throw new BusinessException("miss message queue cluster name.");
        }
        // find cluster
        List<InlongClusterEntity> mqClusters = clusterMapper.selectByKey(null, mqClusterName, null);
        if (mqClusters.size() <= 0) {
            throw new BusinessException("Can not find message queue cluster by name:" + mqClusterName);
        }
        if (mqClusters.size() > 1) {
            throw new BusinessException(String.format("MessageQueue cluster:%s data is more than 1.", mqClusterName));
        }
        // find cluster tag
        InlongClusterEntity mqCluster = mqClusters.get(0);
        String clusterTag = mqCluster.getClusterTags();
        if (StringUtils.isEmpty(clusterTag)) {
            throw new BusinessException(
                    String.format("Cluster tag of message queue cluster:%s is null.", mqClusterName));
        }
        // find group entities
        InlongGroupPageRequest groupRequest = new InlongGroupPageRequest();
        groupRequest.setIsAdminRole(true);
        groupRequest.setClusterTagList(Collections.singletonList(clusterTag));
        List<InlongGroupEntity> groupEntities = groupMapper.selectByCondition(groupRequest);
        String mqType = mqCluster.getType();
        if (StringUtils.equalsIgnoreCase(mqType, MQType.PULSAR)) {
            this.createPulsarTopic(mqCluster, groupEntities);
        } else if (StringUtils.equalsIgnoreCase(mqType, MQType.KAFKA)) {
            this.createKafkaTopic(mqCluster, groupEntities);
        } else if (StringUtils.equalsAnyIgnoreCase(mqType, MQType.TUBEMQ)) {
            this.createTubeTopic(mqCluster, groupEntities);
        } else {
            throw new BusinessException(String.format("Unknown message queue type:%s", mqType));
        }
    }

    /**
     * createPulsarTopic
     */
    private void createPulsarTopic(InlongClusterEntity cluster, List<InlongGroupEntity> groupInfos) {
        // check cluster parameters
        String strExtParams = cluster.getExtParams();
        Gson gson = new Gson();
        java.lang.reflect.Type type = new TypeToken<HashMap<String, String>>() {
        }.getType();
        Map<String, String> extParams = gson.fromJson(strExtParams, type);
        if (!extParams.containsKey(KEY_ADMIN_URL)) {
            String errorMessage = String.format("Can not create pulsar topic,adminUrl is empty,cluster:%s",
                    cluster.getName());
            LOG.error(errorMessage);
            throw new BusinessException(errorMessage);
        }
        String adminUrl = extParams.get(KEY_ADMIN_URL);
        String authentication = extParams.get(KEY_AUTHENTICATION);
        // create admin
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(adminUrl);
        if (!StringUtils.isEmpty(authentication)) {
            pulsarAdminBuilder.authentication(AuthenticationFactory.token(authentication));
        }
        try (PulsarAdmin admin = pulsarAdminBuilder.build()) {
            // namespacePrefix
            String tenant = extParams.get(KEY_TENANT);
            StringBuilder producerTopic = new StringBuilder();
            if (!StringUtils.isEmpty(tenant)) {
                producerTopic.append(tenant).append("/");
            }
            String namespace = extParams.get(KEY_NAMESPACE);
            if (!StringUtils.isEmpty(namespace)) {
                producerTopic.append(namespace).append("/");
            }
            String namespacePrefix = producerTopic.toString();
            Map<String, Set<String>> topicBuffer = new HashMap<>();
            // create topic
            Topics topicAdmin = admin.topics();
            for (InlongGroupEntity groupInfo : groupInfos) {
                producerTopic.setLength(0);
                // concat topic
                producerTopic.append(namespacePrefix);
                producerTopic.append(groupInfo.getMqResource());
                String pTopic = producerTopic.toString();
                // get tenant/namespace
                int namespaceIndex = pTopic.lastIndexOf('/');
                if (namespaceIndex < 0) {
                    LOG.error("InlongGroup:{} can not find namespace:{}", groupInfo.getInlongGroupId(), pTopic);
                    continue;
                }
                String tenantNamespace = pTopic.substring(0, namespaceIndex);
                try {
                    // check topic exist
                    Set<String> topics = topicBuffer.get(tenantNamespace);
                    if (topics == null) {
                        topics = new HashSet<>();
                        topics.addAll(topicAdmin.getList(tenantNamespace));
                        topicBuffer.put(tenantNamespace, topics);
                    }
                    if (topics.contains(pTopic)) {
                        LOG.error("Topic of InlongGroup:{} is existed:{}", groupInfo.getInlongGroupId(), pTopic);
                        continue;
                    }
                    // create topic
                    String strGroupExtParams = groupInfo.getExtParams();
                    Map<String, String> groupExtParams = gson.fromJson(strGroupExtParams, type);
                    if (groupExtParams.containsKey(KEY_NUM_PARTITIONS)) {
                        int numPartitions = NumberUtils.toInt(groupExtParams.get(KEY_NUM_PARTITIONS));
                        topicAdmin.createPartitionedTopic("persistent://" + pTopic, numPartitions);
                    } else {
                        topicAdmin.createNonPartitionedTopic("persistent://" + pTopic);
                    }
                } catch (Exception e) {
                    LOG.error("Fail to create topic:{} in cluster:{},error:{}", pTopic,
                            cluster.getName(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            String errorMsg = String.format("Can not create pulsar Topic of cluster:%s,error:%s",
                    cluster.getName(), e.getMessage());
            LOG.error(errorMsg, e);
            throw new BusinessException(errorMsg);
        }
    }

    /**
     * createKafkaTopic
     */
    private void createKafkaTopic(InlongClusterEntity cluster, List<InlongGroupEntity> groupInfos) {
        String strExtParams = cluster.getExtParams();
        Gson gson = new Gson();
        java.lang.reflect.Type type = new TypeToken<HashMap<String, String>>() {
        }.getType();
        Map<String, String> extParams = gson.fromJson(strExtParams, type);
        Properties properties = new Properties();
        properties.putAll(extParams);
        // create admin
        try (AdminClient admin = AdminClient.create(properties)) {
            Set<String> topics = admin.listTopics().names().get();
            int mqDefaultNumPartitions = NumberUtils.toInt(extParams.get(KEY_NUM_PARTITIONS), DEFAULT_NUM_PARTITIONS);
            short mqDefaultReplicationFactor = NumberUtils.toShort(extParams.get(KEY_REPLICATION_FACTOR),
                    DEFAULT_REPLICATION_FACTOR);
            // create topic
            for (InlongGroupEntity groupInfo : groupInfos) {
                // check topic exist
                if (topics.contains(groupInfo.getMqResource())) {
                    LOG.error("Topic of InlongGroup:{} is existed:{}", groupInfo.getInlongGroupId(),
                            groupInfo.getMqResource());
                    continue;
                }
                try {
                    String strGroupExtParams = groupInfo.getExtParams();
                    Map<String, String> groupExtParams = gson.fromJson(strGroupExtParams, type);
                    int numPartitions = NumberUtils.toInt(groupExtParams.get(KEY_NUM_PARTITIONS),
                            mqDefaultNumPartitions);
                    short replicationFactor = NumberUtils.toShort(groupExtParams.get(KEY_REPLICATION_FACTOR),
                            mqDefaultReplicationFactor);
                    NewTopic newTopic = new NewTopic(groupInfo.getMqResource(), numPartitions, replicationFactor);
                    CreateTopicsResult result = admin.createTopics(Collections.singletonList(newTopic));
                    result.all().get();
                } catch (Exception e) {
                    LOG.error("Fail to create topic:{} in cluster:{},error:{}", groupInfo.getMqResource(),
                            cluster.getName(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            String errorMsg = String.format("Can not create kafka Topic of cluster:%s,error:%s",
                    cluster.getName(), e.getMessage());
            LOG.error(errorMsg, e);
            throw new BusinessException(errorMsg);
        }
    }

    /**
     * createTubeTopic
     */
    private void createTubeTopic(InlongClusterEntity cluster, List<InlongGroupEntity> groupInfos) {
        TubeClusterInfo tubeClusterInfo = this.createTubeClusterInfo(cluster);
        for (InlongGroupEntity groupInfo : groupInfos) {
            try {
                tubeMQOperator.createTopic(tubeClusterInfo, groupInfo.getMqResource(), cluster.getCreator());
            } catch (Exception e) {
                LOG.error("Fail to create topic:{} in cluster:{},error:{}", groupInfo.getMqResource(),
                        cluster.getName(), e.getMessage(), e);
            }
        }
    }

    /**
     * createTubeClusterInfo
     */
    private TubeClusterInfo createTubeClusterInfo(InlongClusterEntity cluster) {
        TubeClusterInfo tubeClusterInfo = new TubeClusterInfo();
        tubeClusterInfo.setClusterTags(cluster.getClusterTags());
        tubeClusterInfo.setExtParams(cluster.getExtParams());
        tubeClusterInfo.setExtTag(cluster.getExtTag());
        tubeClusterInfo.setHeartbeat(cluster.getHeartbeat());
        tubeClusterInfo.setMasterWebUrl(cluster.getUrl());
        tubeClusterInfo.setName(cluster.getName());
        tubeClusterInfo.setToken(cluster.getToken());
        tubeClusterInfo.setType(cluster.getType());
        tubeClusterInfo.setUrl(cluster.getUrl());
        tubeClusterInfo.setVersion(cluster.getVersion());
        return tubeClusterInfo;
    }

    /**
     * Clear all topic from a message queue cluster
     */
    @Override
    public void clearTopic(MessageQueueClearTopicRequest request) {
        String mqClusterName = request.getName();
        // check parameters
        if (StringUtils.isEmpty(mqClusterName)) {
            String errorMsg = "miss message queue cluster name.";
            throw new BusinessException(errorMsg);
        }
        // find cluster
        List<InlongClusterEntity> mqClusters = clusterMapper.selectByKey(null, mqClusterName, null);
        if (mqClusters.size() <= 0) {
            String errorMsg = "Can not find message queue cluster by name:" + mqClusterName;
            throw new BusinessException(errorMsg);
        }
        if (mqClusters.size() > 1) {
            String errorMsg = String.format("MessageQueue cluster:%s data is more than 1.", mqClusterName);
            throw new BusinessException(errorMsg);
        }
        // find cluster tag
        InlongClusterEntity mqCluster = mqClusters.get(0);
        String clusterTag = mqCluster.getClusterTags();
        if (StringUtils.isEmpty(clusterTag)) {
            String errorMsg = String.format("Cluster tag of message queue cluster:%s is null.", mqClusterName);
            throw new BusinessException(errorMsg);
        }
        // clear topic
        String mqType = mqCluster.getType();
        if (StringUtils.equalsIgnoreCase(mqType, MQType.PULSAR)) {
            this.clearPulsarTopic(mqCluster);
        } else if (StringUtils.equalsIgnoreCase(mqType, MQType.KAFKA)) {
            this.clearKafkaTopic(mqCluster);
        } else if (StringUtils.equalsAnyIgnoreCase(mqType, MQType.TUBEMQ)) {
            this.clearTubeTopic(mqCluster);
        } else {
            String errorMsg = String.format("Unknown message queue type:%s", mqType);
            throw new BusinessException(errorMsg);
        }
    }

    /**
     * createPulsarTopic
     */
    private void clearPulsarTopic(InlongClusterEntity cluster) {
        // check cluster parameters
        String strExtParams = cluster.getExtParams();
        Gson gson = new Gson();
        java.lang.reflect.Type type = new TypeToken<HashMap<String, String>>() {
        }.getType();
        Map<String, String> extParams = gson.fromJson(strExtParams, type);
        if (!extParams.containsKey(KEY_ADMIN_URL)) {
            String errorMessage = String.format("Can not create pulsar topic,adminUrl is empty,cluster:%s",
                    cluster.getName());
            LOG.error(errorMessage);
            throw new BusinessException(errorMessage);
        }
        // check namespace
        String tenant = extParams.get(KEY_TENANT);
        String namespace = extParams.get(KEY_NAMESPACE);
        if (StringUtils.isEmpty(tenant) || StringUtils.isEmpty(namespace)) {
            String errorMessage = String.format("tenant or namespace is null,tenant:%s,namespace:%s", tenant,
                    namespace);
            throw new BusinessException(errorMessage);
        }
        // create admin
        String adminUrl = extParams.get(KEY_ADMIN_URL);
        String authentication = extParams.get(KEY_AUTHENTICATION);
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(adminUrl);
        if (!StringUtils.isEmpty(authentication)) {
            pulsarAdminBuilder.authentication(AuthenticationFactory.token(authentication));
        }

        try (PulsarAdmin admin = pulsarAdminBuilder.build()) {
            // get topics
            Topics topicAdmin = admin.topics();
            String tenantNamespace = tenant + "/" + namespace;
            List<String> topics = topicAdmin.getList(tenantNamespace);
            for (String topic : topics) {
                try {
                    topicAdmin.delete(topic);
                } catch (Exception e) {
                    String errorMsg = String.format("Can not delete Topic:%s,error:%s",
                            topic, e.getMessage());
                    LOG.error(errorMsg, e);
                    throw new BusinessException(errorMsg);
                }
            }
        } catch (Exception e) {
            String errorMsg = String.format("Can not clear pulsar Topic of cluster:%s,error:%s",
                    cluster.getName(), e.getMessage());
            LOG.error(errorMsg, e);
            throw new BusinessException(errorMsg);
        }
    }

    /**
     * createKafkaTopic
     */
    private void clearKafkaTopic(InlongClusterEntity cluster) {
        String strExtParams = cluster.getExtParams();
        Gson gson = new Gson();
        java.lang.reflect.Type type = new TypeToken<HashMap<String, String>>() {
        }.getType();
        Map<String, String> extParams = gson.fromJson(strExtParams, type);
        Properties properties = new Properties();
        properties.putAll(extParams);
        // create admin
        try (AdminClient admin = AdminClient.create(properties)) {
            // check topic exist
            Set<String> topics = admin.listTopics().names().get();
            DeleteTopicsResult result = admin.deleteTopics(topics);
            result.all().get();
        } catch (Exception e) {
            String errorMsg = String.format("Can not clear kafka Topic of cluster:%s,error:%s",
                    cluster.getName(), e.getMessage());
            LOG.error(errorMsg, e);
            throw new BusinessException(errorMsg);
        }
    }

    /**
     * createTubeTopic
     */
    private String clearTubeTopic(InlongClusterEntity cluster) {
        return "Can not support to clear topic of TUBEMQ.";
    }
}
