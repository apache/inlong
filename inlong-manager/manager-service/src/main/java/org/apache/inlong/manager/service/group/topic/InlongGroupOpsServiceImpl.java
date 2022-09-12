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

package org.apache.inlong.manager.service.group.topic;

import com.google.gson.Gson;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.pojo.group.topic.InlongGroupChangeClusterTagRequest;
import org.apache.inlong.manager.pojo.group.topic.InlongGroupChangeNumPartitionsRequest;
import org.apache.inlong.manager.pojo.group.topic.InlongStreamChangeSortClusterRequest;
import org.apache.inlong.manager.pojo.sink.SinkPageRequest;
import org.apache.inlong.manager.service.repository.DataProxyConfigRepository;
import org.apache.inlong.manager.service.utils.ClusterUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Inlong cluster tag operator.
 */
@Service
public class InlongGroupOpsServiceImpl implements InlongGroupOpsService {

    public static final Logger LOG = LoggerFactory.getLogger(InlongGroupOpsServiceImpl.class);

    public static final String KEY_ADMIN_URL = "adminUrl";
    public static final String KEY_TENANT = "tenant";
    public static final String KEY_NAMESPACE = "namespace";

    @Autowired
    private DataProxyConfigRepository dataProxyConfigRepository;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private StreamSinkEntityMapper sinkMapper;

    /**
     * Change cluster tag of a inlong group id.
     */
    public String changeClusterTag(InlongGroupChangeClusterTagRequest request) {
        String result = dataProxyConfigRepository.changeClusterTag(request.getInlongGroupId(), request.getClusterTag(),
                request.getTopic());
        return result;
    }

    /**
     * Change Partition number of a inlong group id.
     */
    public String changeNumPartitions(InlongGroupChangeNumPartitionsRequest request) {
        // check parameters
        String inlongGroupId = request.getInlongGroupId();
        if (StringUtils.isEmpty(inlongGroupId)) {
            return "Inlong group id is null";
        }
        Integer numPartitions = request.getNumPartitions();
        if (numPartitions == null) {
            return "Partition number is null";
        }
        String topic = null;
        try {
            InlongGroupEntity groupEntity = groupMapper.selectByGroupId(inlongGroupId);
            if (groupEntity == null) {
                return "Can not find group Entity by id:" + inlongGroupId;
            }
            String clusterTag = groupEntity.getInlongClusterTag();
            if (StringUtils.isEmpty(clusterTag)) {
                return String.format("Cluster tag of inlong group id:%s is null", inlongGroupId);
            }
            ClusterPageRequest clusterPageRequest = new ClusterPageRequest();
            clusterPageRequest.setClusterTag(clusterTag);
            List<InlongClusterEntity> clusterEntities = clusterMapper.selectByCondition(clusterPageRequest);
            if (clusterEntities == null || clusterEntities.size() <= 0) {
                return String.format("Can not find a cluster for cluster tag:%s", clusterTag);
            }
            // topic
            topic = groupEntity.getMqResource();
            if (StringUtils.isEmpty(topic)) {
                return "topic is null";
            }
            for (InlongClusterEntity cluster : clusterEntities) {
                String mqType = cluster.getType();
                if (StringUtils.equalsIgnoreCase(mqType, MQType.PULSAR)) {
                    this.changePulsarTopicPartitions(cluster, topic, numPartitions);
                } else if (StringUtils.equalsIgnoreCase(mqType, MQType.KAFKA)) {
                    this.changeKafkaTopicPartitions(cluster, topic, numPartitions);
                } else if (StringUtils.equalsAnyIgnoreCase(mqType, MQType.TUBEMQ)) {
                    this.changeTubeTopicPartitions(cluster, topic, numPartitions);
                } else {
                    continue;
                }
            }
            return null;
        } catch (Exception e) {
            String errorMsg = String.format("Can not change topic partition:%s of inlong group:%s,error:%s",
                    topic, inlongGroupId, e.getMessage());
            LOG.error(errorMsg, e);
            return errorMsg;
        }
    }

    /**
     * changePulsarTopicPartitions
     */
    private void changePulsarTopicPartitions(InlongClusterEntity cluster, String topic, int numPartitions) {
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
            // change topic partition
            admin.topics().updatePartitionedTopic("persistent://" + pTopic, numPartitions);
        } catch (Exception e) {
            LOG.error("Can not change topic partition:{} of cluster:{},error:{}",
                    topic, cluster.getName(), e.getMessage(), e);
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    /**
     * changeKafkaTopicPartitions
     */
    private void changeKafkaTopicPartitions(InlongClusterEntity cluster, String topic, int numPartitions) {
        AdminClient admin = null;
        try {
            Gson gson = new Gson();
            Map<String, String> extParams = gson.fromJson(cluster.getExtParams(), ClusterUtils.HASHMAP_TYPE);
            Properties properties = new Properties();
            properties.putAll(extParams);
            // create admin
            admin = AdminClient.create(properties);
            Map<String, NewPartitions> partitionsMap = new HashMap<>();
            NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
            partitionsMap.put(topic, newPartitions);
            CreatePartitionsResult createPartitionsResult = admin.createPartitions(partitionsMap);
            createPartitionsResult.all().get();
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
     * changeTubeTopicPartitions
     */
    private void changeTubeTopicPartitions(InlongClusterEntity cluster, String topic, int numPartitions) {
        LOG.error("Can not support to change topic partition of TUBEMQ with Admin client.");
    }

    /**
     * Change sort cluster of a inlong stream.
     */
    public String changeSortCluster(InlongStreamChangeSortClusterRequest request) {
        // check parameters
        String inlongGroupId = request.getInlongGroupId();
        if (StringUtils.isEmpty(inlongGroupId)) {
            return "Inlong group id is null";
        }
        String inlongStreamId = request.getInlongStreamId();
        if (inlongStreamId == null) {
            return "Inlong stream id is null";
        }
        String sortTaskName = request.getSortTaskName();
        if (sortTaskName == null) {
            return "Sink name is null";
        }
        String sortClusterName = request.getSortClusterName();
        if (sortClusterName == null) {
            return "Sort cluster name is null";
        }
        // update
        try {
            SinkPageRequest sinkPageRequest = new SinkPageRequest();
            sinkPageRequest.setInlongGroupId(inlongGroupId);
            sinkPageRequest.setInlongStreamId(inlongStreamId);
            sinkPageRequest.setSortTaskName(sortTaskName);
            List<StreamSinkEntity> sinkEntities = sinkMapper.selectByCondition(sinkPageRequest);
            if (sinkEntities == null || sinkEntities.size() <= 0) {
                return String.format("Can not find stream sink entity by request:%s", request);
            }
            for (StreamSinkEntity sinkEntity : sinkEntities) {
                sinkEntity.setInlongClusterName(sortClusterName);
                sinkMapper.updateByPrimaryKeySelective(sinkEntity);
            }
            return null;
        } catch (Exception e) {
            String errorMsg = String.format("Can not change sort cluster:%s,error:%s",
                    request, e.getMessage());
            LOG.error(errorMsg, e);
            return errorMsg;
        }
    }
}
