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

package org.apache.inlong.manager.service.core.impl;

import org.apache.inlong.common.pojo.sdk.SortSourceConfigResponse;
import org.apache.inlong.common.pojo.sort.ClusterTagConfig;
import org.apache.inlong.common.pojo.sort.SortConfig;
import org.apache.inlong.common.pojo.sort.SortConfigResponse;
import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.mq.MqClusterConfig;
import org.apache.inlong.common.pojo.sort.mq.PulsarClusterConfig;
import org.apache.inlong.common.pojo.sort.mq.TubeClusterConfig;
import org.apache.inlong.common.pojo.sort.node.NodeConfig;
import org.apache.inlong.common.pojo.sortstandalone.SortClusterResponse;
import org.apache.inlong.common.util.Utils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.plugin.Plugin;
import org.apache.inlong.manager.common.plugin.PluginBinder;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.ClusterConfigEntity;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.SortConfigEntity;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sort.SortStatusInfo;
import org.apache.inlong.manager.pojo.sort.SortStatusRequest;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.core.ConfigLoader;
import org.apache.inlong.manager.service.core.SortClusterService;
import org.apache.inlong.manager.service.core.SortService;
import org.apache.inlong.manager.service.core.SortSourceService;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.node.DataNodeOperator;
import org.apache.inlong.manager.service.node.DataNodeOperatorFactory;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.apache.inlong.manager.workflow.plugin.sort.PollerPlugin;
import org.apache.inlong.manager.workflow.plugin.sort.SortPoller;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Sort service implementation.
 */
@Lazy
@Slf4j
@Service
public class SortServiceImpl implements SortService, PluginBinder {

    private static final int RESPONSE_CODE_SUCCESS = 0;
    private static final int RESPONSE_CODE_NO_UPDATE = 1;
    private static final int RESPONSE_CODE_FAIL = -1;
    private static final int RESPONSE_CODE_REQUEST_PARAMS_ERROR = -101;
    @Lazy
    @Autowired
    private SortSourceService sortSourceService;
    @Lazy
    @Autowired
    private SortClusterService sortClusterService;
    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private ConfigLoader configLoader;
    @Autowired
    private DataNodeOperatorFactory dataNodeOperatorFactory;
    /**
     * key 1: sort cluster name, value : sort config
     */
    private Map<String, byte[]> sortConfigMap = new ConcurrentHashMap<>();
    /**
     * key 1: sort cluster name, value : md5
     */
    private Map<String, String> sortConfigMd5Map = new ConcurrentHashMap<>();
    /**
     * key 1: mq cluster name, value : mq cluster config
     */
    private Map<String, List<MqClusterConfig>> mqClusterConfigMap = new ConcurrentHashMap<>();
    /**
     * key 1: data node name, value : data node config
     */
    private Map<String, NodeConfig> nodeInfoMap = new ConcurrentHashMap<>();
    /**
     * The plugin poller will be initialed after the application starts.
     *
     * @see org.apache.inlong.manager.service.plugin.PluginService#afterPropertiesSet
     */
    private SortPoller sortPoller;

    @PostConstruct
    public void initialize() {
        log.info("create SortServiceImpl for " + SortSourceServiceImpl.class.getSimpleName());
        try {
            reload();;
            setReloadTimer();
        } catch (Throwable t) {
            log.error("initialize SortServiceImpl error", t);
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public void reload() {
        log.debug("start to reload sort config.");
        try {
            reloadMqCluster();
            reloadNodeConfig();
            reloadDataFlowConfig();
        } catch (Throwable t) {
            log.error("fail to reload all sort config", t);
        }
        log.debug("end to reload config");
    }

    private void setReloadTimer() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        long reloadInterval = 60000L;
        executorService.scheduleWithFixedDelay(this::reload, reloadInterval, reloadInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public SortClusterResponse getClusterConfig(String clusterName, String md5) {
        return sortClusterService.getClusterConfig(clusterName, md5);
    }

    @Override
    public SortSourceConfigResponse getSourceConfig(String clusterName, String sortTaskId, String md5) {
        return sortSourceService.getSourceConfig(clusterName, sortTaskId, md5);
    }

    @Override
    public List<SortStatusInfo> listSortStatus(SortStatusRequest request) {
        Preconditions.expectNotNull(sortPoller, "sort status poller not initialized, please try later");

        try {
            List<InlongGroupInfo> groupInfoList = request.getInlongGroupIds().stream()
                    .map(groupId -> {
                        try {
                            return groupService.get(groupId);
                        } catch (Exception e) {
                            log.error("can not get groupId: {}, skip it", groupId, e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            List<InlongStreamInfo> streamInfos = new ArrayList<>();
            groupInfoList.forEach(groupInfo -> {
                streamInfos.addAll(streamService.list(groupInfo.getInlongGroupId()));
            });
            List<SortStatusInfo> statusInfos = sortPoller.pollSortStatus(streamInfos, request.getCredentials());
            log.debug("success to list sort status for request={}, result={}", request, statusInfos);
            return statusInfos;
        } catch (Exception e) {
            log.error("poll sort status error: ", e);
            throw new BusinessException("poll sort status error: " + e.getMessage());
        }
    }

    @Override
    public void acceptPlugin(Plugin plugin) {
        if (plugin instanceof PollerPlugin) {
            PollerPlugin pollerPlugin = (PollerPlugin) plugin;
            sortPoller = pollerPlugin.getSortPoller();
        }
    }

    @Override
    public SortConfigResponse getSortConfig(String clusterName, String md5) {
        if (StringUtils.isBlank(clusterName)) {
            String errMsg = "cluster name is blank, return nothing";
            log.debug(errMsg);
            return SortConfigResponse.builder()
                    .code(RESPONSE_CODE_REQUEST_PARAMS_ERROR)
                    .msg(errMsg)
                    .build();
        }
        if (!sortConfigMap.containsKey(clusterName)) {
            String errMsg = String.format("there is no valid sort config of cluster %s", clusterName);
            log.debug(errMsg);
            return SortConfigResponse.builder()
                    .code(RESPONSE_CODE_FAIL)
                    .msg(errMsg)
                    .build();
        }
        if (sortConfigMd5Map.get(clusterName).equals(md5)) {
            return SortConfigResponse.builder()
                    .code(RESPONSE_CODE_NO_UPDATE)
                    .msg("No update")
                    .md5(md5)
                    .build();
        }
        return SortConfigResponse.builder()
                .code(RESPONSE_CODE_SUCCESS)
                .data(sortConfigMap.get(clusterName))
                .md5(sortConfigMd5Map.get(clusterName))
                .build();
    }

    private void reloadMqCluster() {
        Map<String, List<MqClusterConfig>> tempMqClusterMap = new HashMap<>();
        List<ClusterConfigEntity> clusterConfigEntityList = configLoader.loadAllClusterConfigEntity();
        clusterConfigEntityList.forEach(clusterConfigEntity -> {
            String clusterTag = clusterConfigEntity.getClusterTag();
            if (ClusterType.PULSAR.equals(clusterConfigEntity.getClusterType())) {
                List<PulsarClusterConfig> pulsarClusterConfigs =
                        JsonUtils.parseArray(clusterConfigEntity.getConfigParams(),
                                PulsarClusterConfig.class);
                List<MqClusterConfig> list = new ArrayList<>(pulsarClusterConfigs);
                tempMqClusterMap.putIfAbsent(clusterTag, list);
            } else if (ClusterType.TUBEMQ.equals(clusterConfigEntity.getClusterType())) {
                List<TubeClusterConfig> tubeClusterConfigs =
                        JsonUtils.parseArray(clusterConfigEntity.getConfigParams(), TubeClusterConfig.class);
                List<MqClusterConfig> list = new ArrayList<>(tubeClusterConfigs);
                tempMqClusterMap.putIfAbsent(clusterTag, list);
            }
        });
        mqClusterConfigMap = tempMqClusterMap;
    }

    private void reloadNodeConfig() {
        List<DataNodeEntity> dataNodeEntities = configLoader.loadAllDataNodeEntity();
        Map<String, NodeConfig> task2DataNodeMap = dataNodeEntities.stream()
                .filter(entity -> StringUtils.isNotBlank(entity.getName()))
                .map(entity -> {
                    try {
                        DataNodeOperator operator = dataNodeOperatorFactory.getInstance(entity.getType());
                        return operator.getNodeConfig(entity);
                    } catch (Exception e) {
                        log.error("parse node config error for data node name={}", entity.getName(), e);
                    }
                    return null;
                }).filter(Objects::nonNull)
                .collect(Collectors.toMap(NodeConfig::getNodeName, info -> info));
        nodeInfoMap = task2DataNodeMap;

    }

    private void reloadDataFlowConfig() {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, byte[]> sortConfigs = new HashMap<>();
        Map<String, String> sortConfigMd5s = new HashMap<>();
        Map<String, List<TaskConfig>> temp = new HashMap<>();
        List<SortConfigEntity> sinkConfigEntityList = configLoader.loadAllSortConfigEntity();
        for (SortConfigEntity sortConfigEntity : sinkConfigEntityList) {
            if (StringUtils.isBlank(sortConfigEntity.getSortTaskName())) {
                sortConfigEntity.setSortTaskName(InlongConstants.DEFAULT_TASK);
            }
        }
        Map<String, Map<String, Map<String, List<SortConfigEntity>>>> cluster2SinkMap = sinkConfigEntityList.stream()
                .collect(Collectors.groupingBy(SortConfigEntity::getInlongClusterName,
                        Collectors.groupingBy(SortConfigEntity::getSortTaskName,
                                Collectors.groupingBy(SortConfigEntity::getInlongClusterTag))));
        for (String sortClusterName : cluster2SinkMap.keySet()) {
            List<TaskConfig> map = temp.computeIfAbsent(sortClusterName,
                    v -> new ArrayList<>());
            SortConfig sortConfig = new SortConfig();
            sortConfig.setSortClusterName(sortClusterName);
            Map<String, Map<String, List<SortConfigEntity>>> sortTaskNameMap = cluster2SinkMap.get(sortClusterName);
            for (String sortTaskName : sortTaskNameMap.keySet()) {
                Map<String, List<SortConfigEntity>> clusterTagMap = sortTaskNameMap.get(sortTaskName);
                TaskConfig sortTaskConfig = TaskConfig.builder()
                        .sortTaskName(sortTaskName)
                        .clusterTagConfigs(new ArrayList<>())
                        .nodeConfig(nodeInfoMap.get(sortTaskName))
                        .build();
                for (String clusterTag : clusterTagMap.keySet()) {
                    List<SortConfigEntity> sinkConfigEntities = clusterTagMap.get(clusterTag);
                    List<DataFlowConfig> dataFlowConfigs = sinkConfigEntities.stream().map(v -> {
                        try {
                            return objectMapper.readValue(v.getConfigParams(), DataFlowConfig.class);
                        } catch (Exception e) {
                            log.error("parse data flow config error for sinkId={}", v.getSinkId(), e);
                        }
                        return null;
                    }).filter(Objects::nonNull)
                            .sorted(Comparator.comparingInt(x -> Integer.parseInt(x.getDataflowId())))
                            .collect(Collectors.toList());
                    ClusterTagConfig sortClusterConfig = ClusterTagConfig.builder()
                            .mqClusterConfigs(mqClusterConfigMap.getOrDefault(clusterTag, new ArrayList<>()))
                            .clusterTag(clusterTag)
                            .dataFlowConfigs(dataFlowConfigs)
                            .build();
                    sortTaskConfig.getClusterTagConfigs().add(sortClusterConfig);
                }
                map.add(sortTaskConfig);
            }
            sortConfig.setTasks(temp.get(sortClusterName));
            try {
                String configStr = objectMapper.writeValueAsString(sortConfig);
                sortConfigs.put(sortClusterName, Utils.compressGZip(configStr.getBytes()));
                String md5 = DigestUtils.md5Hex(configStr);
                sortConfigMd5s.put(sortClusterName, md5);
            } catch (Exception e) {
                log.info("parse sort config error for cluster name={}", sortClusterName, e);
            }

        }
        sortConfigMap = sortConfigs;
        sortConfigMd5Map = sortConfigMd5s;
    }

}
