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

package org.apache.inlong.manager.service.cluster.tag.topic;

import com.google.gson.Gson;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.pojo.cluster.tag.topic.ClusterTagChangeRetentionRequest;
import org.apache.inlong.manager.pojo.cluster.tag.topic.ClusterTagCreateTopicRequest;
import org.apache.inlong.manager.pojo.cluster.tag.topic.ClusterTagDeleteTopicRequest;
import org.apache.inlong.manager.pojo.cluster.tubemq.TubeClusterInfo;
import org.apache.inlong.manager.service.repository.DataProxyConfigRepository;
import org.apache.inlong.manager.service.resource.queue.tubemq.TubeMQOperator;
import org.apache.inlong.manager.service.utils.ClusterUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Inlong cluster tag operator.
 */
@Service
public class ClusterTagServiceImpl implements ClusterTagService {

    public static final Logger LOG = LoggerFactory.getLogger(ClusterTagServiceImpl.class);

    public static final String KEY_ADMIN_URL = "adminUrl";
    public static final String KEY_TENANT = "tenant";
    public static final String KEY_NAMESPACE = "namespace";
    public static final String KEY_NUM_PARTITIONS = "numPartitions";
    public static final String KEY_REPLICATION_FACTOR = "replicationFactor";
    public static final String KEY_RETENTION_TIME_IN_MINUTES = "retentionTimeInMinutes";
    public static final String KEY_RETENTION_SIZE_IN_MB = "retentionSizeInMB";
    public static final int DEFAULT_NUM_PARTITIONS = 10;
    public static final short DEFAULT_REPLICATION_FACTOR = 2;
    public static final int DEFAULT_RETENTION_TIME_IN_MINUTES = 16 * 60;
    public static final int DEFAULT_RETENTION_SIZE_IN_MB = -1;

    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private TubeMQOperator tubeMQOperator;
    @Autowired
    private DataProxyConfigRepository dataProxyConfigRepository;

    /**
     * Create a topic in all message queue of Cluster Tag
     */
    public String createTopic(ClusterTagCreateTopicRequest request) {
        try {
            // check parameters
            String clusterTag = request.getClusterTag();
            if (StringUtils.isEmpty(clusterTag)) {
                return "cluster tag is null";
            }
            ClusterPageRequest clusterPageRequest = new ClusterPageRequest();
            clusterPageRequest.setClusterTag(clusterTag);
            List<InlongClusterEntity> clusterEntities = clusterMapper.selectByCondition(clusterPageRequest);
            if (clusterEntities == null || clusterEntities.size() <= 0) {
                return String.format("Can not find a cluster for cluster tag:%s", clusterTag);
            }
            // topic
            String topic = request.getTopic();
            if (StringUtils.isEmpty(topic)) {
                return "topic is null";
            }
            String strExtParams = request.getExtParams();
            Gson gson = new Gson();
            Map<String, String> extParams = gson.fromJson(strExtParams, ClusterUtils.HASHMAP_TYPE);
            for (InlongClusterEntity cluster : clusterEntities) {
                String mqType = cluster.getType();
                if (StringUtils.equalsIgnoreCase(mqType, MQType.PULSAR)) {
                    this.createPulsarTopic(cluster, topic, extParams);
                } else if (StringUtils.equalsIgnoreCase(mqType, MQType.KAFKA)) {
                    this.createKafkaTopic(cluster, topic, extParams);
                } else if (StringUtils.equalsAnyIgnoreCase(mqType, MQType.TUBEMQ)) {
                    this.createTubeTopic(cluster, topic, extParams);
                } else {
                    continue;
                }
            }
            return null;
        } catch (Exception e) {
            String errorMsg = String.format("Can not create topic:%s of cluster tag:%s with extParams:%s,error:%s",
                    request.getTopic(), request.getClusterTag(), request.getExtParams(), e.getMessage());
            LOG.error(errorMsg, e);
            return errorMsg;
        }
    }

    /**
     * createPulsarTopic
     */
    private void createPulsarTopic(InlongClusterEntity cluster, String topic, Map<String, String> topicExtParams) {
        PulsarAdmin admin = null;
        try {
            // check cluster parameters
            Gson gson = new Gson();
            Map<String, String> extParams = gson.fromJson(cluster.getExtParams(), ClusterUtils.HASHMAP_TYPE);
            if (!extParams.containsKey(KEY_ADMIN_URL)) {
                LOG.error("Can not create pulsar topic,adminUrl is empty,cluster:{}",
                        cluster.getName());
                return;
            }
            admin = ClusterUtils.createPulsarAdmin(extParams);
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
            producerTopic.append(topic);
            String pTopic = producerTopic.toString();
            // create topic
            if (topicExtParams.containsKey(KEY_NUM_PARTITIONS)) {
                int numPartitions = NumberUtils.toInt(topicExtParams.get(KEY_NUM_PARTITIONS));
                admin.topics().createPartitionedTopic("persistent://" + pTopic, numPartitions);
            } else {
                admin.topics().createNonPartitionedTopic("persistent://" + pTopic);
            }
        } catch (Exception e) {
            LOG.error("Can not create pulsar topic:{} of cluster:{},error:{}",
                    topic, cluster.getName(), e.getMessage(), e);
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    /**
     * createKafkaTopic
     */
    private void createKafkaTopic(InlongClusterEntity cluster, String topic, Map<String, String> topicExtParams) {
        AdminClient admin = null;
        try {
            Gson gson = new Gson();
            Map<String, String> extParams = gson.fromJson(cluster.getExtParams(), ClusterUtils.HASHMAP_TYPE);
            Properties properties = new Properties();
            properties.putAll(extParams);
            // create admin
            admin = AdminClient.create(properties);
            int mqDefaultNumPartitions = NumberUtils.toInt(extParams.get(KEY_NUM_PARTITIONS), DEFAULT_NUM_PARTITIONS);
            short mqDefaultReplicationFactor = NumberUtils.toShort(extParams.get(KEY_REPLICATION_FACTOR),
                    DEFAULT_REPLICATION_FACTOR);
            int numPartitions = NumberUtils.toInt(topicExtParams.get(KEY_NUM_PARTITIONS),
                    mqDefaultNumPartitions);
            short replicationFactor = NumberUtils.toShort(topicExtParams.get(KEY_REPLICATION_FACTOR),
                    mqDefaultReplicationFactor);
            NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
            CreateTopicsResult result = admin.createTopics(Collections.singletonList(newTopic));
            result.all().get();
        } catch (Exception e) {
            LOG.error("Can not create kafka topic:{} of cluster:{},error:{}",
                    topic, cluster.getName(), e.getMessage(), e);
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    /**
     * createTubeTopic
     */
    private void createTubeTopic(InlongClusterEntity cluster, String topic, Map<String, String> topicExtParams) {
        TubeClusterInfo tubeClusterInfo = ClusterUtils.createTubeClusterInfo(cluster);
        try {
            tubeMQOperator.createTopic(tubeClusterInfo, topic, cluster.getCreator());
        } catch (Exception e) {
            LOG.error("Fail to create topic:{} in cluster:{},error:{}", topic,
                    cluster.getName(), e.getMessage(), e);
        }
    }

    /**
     * Delete a topic from all message queue of Cluster Tag
     */
    public String deleteTopic(ClusterTagDeleteTopicRequest request) {
        try {
            // check parameters
            String clusterTag = request.getClusterTag();
            if (StringUtils.isEmpty(clusterTag)) {
                return "cluster tag is null";
            }
            ClusterPageRequest clusterPageRequest = new ClusterPageRequest();
            clusterPageRequest.setClusterTag(clusterTag);
            List<InlongClusterEntity> clusterEntities = clusterMapper.selectByCondition(clusterPageRequest);
            if (clusterEntities == null || clusterEntities.size() <= 0) {
                return String.format("Can not find a cluster for cluster tag:%s", clusterTag);
            }
            // topic
            String topic = request.getTopic();
            if (StringUtils.isEmpty(topic)) {
                return "topic is null";
            }
            for (InlongClusterEntity cluster : clusterEntities) {
                String mqType = cluster.getType();
                if (StringUtils.equalsIgnoreCase(mqType, MQType.PULSAR)) {
                    this.deletePulsarTopic(cluster, topic);
                } else if (StringUtils.equalsIgnoreCase(mqType, MQType.KAFKA)) {
                    this.deleteKafkaTopic(cluster, topic);
                } else if (StringUtils.equalsAnyIgnoreCase(mqType, MQType.TUBEMQ)) {
                    this.deleteTubeTopic(cluster, topic);
                } else {
                    continue;
                }
            }
            return null;
        } catch (Exception e) {
            String errorMsg = String.format("Can not delete topic:%s of cluster tag:%s,error:%s",
                    request.getTopic(), request.getClusterTag(), e.getMessage());
            LOG.error(errorMsg, e);
            return errorMsg;
        }
    }

    /**
     * deletePulsarTopic
     */
    private void deletePulsarTopic(InlongClusterEntity cluster, String topic) {
        PulsarAdmin admin = null;
        try {
            // check cluster parameters
            String strExtParams = cluster.getExtParams();
            Gson gson = new Gson();
            Map<String, String> extParams = gson.fromJson(strExtParams, ClusterUtils.HASHMAP_TYPE);
            if (!extParams.containsKey(KEY_ADMIN_URL)) {
                LOG.error("Can not delete pulsar topic,adminUrl is empty,cluster:{}",
                        cluster.getName());
                return;
            }
            admin = ClusterUtils.createPulsarAdmin(extParams);
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
            producerTopic.append(topic);
            String pTopic = producerTopic.toString();
            admin.topics().delete(pTopic);
        } catch (Exception e) {
            LOG.error("Can not delete pulsar topic:{} of cluster:{},error:{}",
                    topic, cluster.getName(), e.getMessage(), e);
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    /**
     * deleteKafkaTopic
     */
    private void deleteKafkaTopic(InlongClusterEntity cluster, String topic) {
        AdminClient admin = null;
        try {
            String strExtParams = cluster.getExtParams();
            Gson gson = new Gson();
            Map<String, String> extParams = gson.fromJson(strExtParams, ClusterUtils.HASHMAP_TYPE);
            Properties properties = new Properties();
            properties.putAll(extParams);
            // create admin
            admin = AdminClient.create(properties);
            // delete topic
            DeleteTopicsResult result = admin.deleteTopics(Collections.singletonList(topic));
            result.all().get();
        } catch (Exception e) {
            LOG.error("Can not clear kafka topic:{} of cluster:{},error:{}",
                    topic, cluster.getName(), e.getMessage(), e);
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    /**
     * deleteTubeTopic
     */
    private String deleteTubeTopic(InlongClusterEntity cluster, String topic) {
        return "Can not support to clear topic of TUBEMQ.";
    }

    /**
     * Change retention policies about all topics of this cluster tag
     */
    public String changeRetention(ClusterTagChangeRetentionRequest request) {
        try {
            // check parameters
            String clusterTag = request.getClusterTag();
            if (StringUtils.isEmpty(clusterTag)) {
                return "cluster tag is null";
            }
            ClusterPageRequest clusterPageRequest = new ClusterPageRequest();
            clusterPageRequest.setClusterTag(clusterTag);
            List<InlongClusterEntity> clusterEntities = clusterMapper.selectByCondition(clusterPageRequest);
            if (clusterEntities == null || clusterEntities.size() <= 0) {
                return String.format("Can not find a cluster for cluster tag:%s", clusterTag);
            }
            // retentionTimeInMinutes
            Integer retentionTimeInMinutes = request.getRetentionTimeInMinutes();
            if (retentionTimeInMinutes == null) {
                retentionTimeInMinutes = DEFAULT_RETENTION_TIME_IN_MINUTES;
            }
            // retentionSizeInMB
            Integer retentionSizeInMB = request.getRetentionSizeInMB();
            if (retentionSizeInMB == null) {
                retentionSizeInMB = DEFAULT_RETENTION_SIZE_IN_MB;
            }
            // change
            for (InlongClusterEntity cluster : clusterEntities) {
                String mqType = cluster.getType();
                if (StringUtils.equalsIgnoreCase(mqType, MQType.PULSAR)) {
                    this.changePulsarRetention(cluster, retentionTimeInMinutes, retentionSizeInMB);
                } else if (StringUtils.equalsIgnoreCase(mqType, MQType.KAFKA)) {
                    this.changeKafkaRetention(cluster, retentionTimeInMinutes, retentionSizeInMB);
                } else if (StringUtils.equalsAnyIgnoreCase(mqType, MQType.TUBEMQ)) {
                    this.changeTubeRetention(cluster, retentionTimeInMinutes, retentionSizeInMB);
                } else {
                    continue;
                }
            }
            return null;
        } catch (Exception e) {
            String errorMsg = String.format("Can not change retention time:%d of cluster tag:%s,error:%s",
                    request.getRetentionTimeInMinutes(), request.getClusterTag(), e.getMessage());
            LOG.error(errorMsg, e);
            return errorMsg;
        }
    }

    /**
     * changePulsarRetentionTimeInMinutes
     */
    private void changePulsarRetention(InlongClusterEntity cluster, int retentionTimeInMinutes, int retentionSizeInMB) {
        PulsarAdmin admin = null;
        try {
            // check cluster parameters
            String strExtParams = cluster.getExtParams();
            Gson gson = new Gson();
            Map<String, String> extParams = gson.fromJson(strExtParams, ClusterUtils.HASHMAP_TYPE);
            if (!extParams.containsKey(KEY_ADMIN_URL)) {
                LOG.error("Can not delete pulsar topic,adminUrl is empty,cluster:{}",
                        cluster.getName());
                return;
            }
            admin = ClusterUtils.createPulsarAdmin(cluster);
            // namespacePrefix
            String tenant = extParams.get(KEY_TENANT);
            StringBuilder tenantNamespace = new StringBuilder();
            if (!StringUtils.isEmpty(tenant)) {
                tenantNamespace.append(tenant).append("/");
            }
            String namespace = extParams.get(KEY_NAMESPACE);
            if (!StringUtils.isEmpty(namespace)) {
                tenantNamespace.append(namespace);
            }
            RetentionPolicies retention = new RetentionPolicies(retentionTimeInMinutes, retentionSizeInMB);
            admin.namespaces().setRetention(tenantNamespace.toString(), retention);
        } catch (Exception e) {
            LOG.error("Can not change pulsar retention of cluster:{},error:{}",
                    cluster.getName(), e.getMessage(), e);
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    /**
     * changeKafkaRetentionTimeInMinutes
     */
    private void changeKafkaRetention(InlongClusterEntity cluster, int retentionTimeInMinutes, int retentionSizeInMB) {
        AdminClient admin = null;
        try {
            String strExtParams = cluster.getExtParams();
            Gson gson = new Gson();
            Map<String, String> extParams = gson.fromJson(strExtParams, ClusterUtils.HASHMAP_TYPE);
            Properties properties = new Properties();
            properties.putAll(extParams);
            // create admin
            admin = AdminClient.create(properties);
            List<AlterConfigOp> configOps = new ArrayList<>();
            configOps.add(new AlterConfigOp(
                    new ConfigEntry("retention.ms", String.valueOf(retentionTimeInMinutes * 60 * 1000)),
                    AlterConfigOp.OpType.SET));
            configOps.add(new AlterConfigOp(
                    new ConfigEntry("retention.bytes", String.valueOf(retentionSizeInMB * 1024 * 1024)),
                    AlterConfigOp.OpType.SET));
            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            Set<String> topics = admin.listTopics().names().get();
            topics.forEach((v) -> configs.put(new ConfigResource(ConfigResource.Type.TOPIC, v), configOps));
            admin.incrementalAlterConfigs(configs).all().get();
        } catch (Exception e) {
            LOG.error("Can not change retention of cluster:{},error:{}",
                    cluster.getName(), e.getMessage(), e);
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    /**
     * changeTubeRetentionTimeInMinutes
     */
    private String changeTubeRetention(InlongClusterEntity cluster, int retentionTimeInMinutes, int retentionSizeInMB) {
        return "Can not support to change retention of TUBEMQ.";
    }
}
