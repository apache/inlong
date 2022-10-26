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

import com.google.gson.Gson;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.pojo.sortstandalone.SortClusterConfig;
import org.apache.inlong.common.pojo.sortstandalone.SortClusterResponse;
import org.apache.inlong.common.pojo.sortstandalone.SortTaskConfig;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sort.standalone.SortIdInfo;
import org.apache.inlong.manager.pojo.sort.standalone.SortSinkInfo;
import org.apache.inlong.manager.pojo.sort.standalone.SortTaskInfo;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.service.core.SortClusterService;
import org.apache.inlong.manager.service.node.DataNodeOperator;
import org.apache.inlong.manager.service.node.DataNodeOperatorFactory;
import org.apache.inlong.manager.service.sink.SinkOperatorFactory;
import org.apache.inlong.manager.service.sink.StreamSinkOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
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
 * Used to cache the sort cluster config and reduce the number of query to database.
 */
@Lazy
@Service
public class SortClusterServiceImpl implements SortClusterService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortClusterServiceImpl.class);

    private static final Gson GSON = new Gson();

    private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 60000;

    private static final int RESPONSE_CODE_SUCCESS = 0;
    private static final int RESPONSE_CODE_NO_UPDATE = 1;
    private static final int RESPONSE_CODE_FAIL = -1;
    private static final int RESPONSE_CODE_REQ_PARAMS_ERROR = -101;

    private static final String KEY_GROUP_ID = "inlongGroupId";
    private static final String KEY_STREAM_ID = "inlongStreamId";

    // key : sort cluster name, value : md5
    private Map<String, String> sortClusterMd5Map = new ConcurrentHashMap<>();
    // key : sort cluster name, value : cluster config
    private Map<String, SortClusterConfig> sortClusterConfigMap = new ConcurrentHashMap<>();
    // key : sort cluster name, value : error log
    private Map<String, String> sortClusterErrorLogMap = new ConcurrentHashMap<>();

    private long reloadInterval;

    @Autowired
    private StreamSinkEntityMapper streamSinkEntityMapper;
    @Autowired
    private SinkOperatorFactory sinkOperatorFactory;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;
    @Autowired
    private DataNodeOperatorFactory dataNodeOperatorFactory;

    @PostConstruct
    public void initialize() {
        LOGGER.info("create repository for " + SortClusterServiceImpl.class.getSimpleName());
        try {
            this.reloadInterval = DEFAULT_HEARTBEAT_INTERVAL_MS;
            reload();
            setReloadTimer();
        } catch (Throwable t) {
            LOGGER.error("Initialize SortClusterConfigRepository error", t);
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public void reload() {
        LOGGER.debug("start to reload sort config");
        try {
            reloadAllClusterConfigV2();
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
        }
        LOGGER.debug("end to reload config");
    }

    @Override
    public SortClusterResponse getClusterConfig(String clusterName, String md5) {
        // check if cluster name is valid or not.
        if (StringUtils.isBlank(clusterName)) {
            String errMsg = "Blank cluster name, return nothing";
            LOGGER.info(errMsg);
            return SortClusterResponse.builder()
                    .msg(errMsg)
                    .code(RESPONSE_CODE_REQ_PARAMS_ERROR)
                    .build();
        }

        // if there is an error
        if (sortClusterErrorLogMap.get(clusterName) != null) {
            return SortClusterResponse.builder()
                    .msg(sortClusterErrorLogMap.get(clusterName))
                    .code(RESPONSE_CODE_FAIL)
                    .build();
        }

        // there is no config, but still return success.
        if (sortClusterConfigMap.get(clusterName) == null) {
            String errMsg = "There is not config for cluster " + clusterName;
            LOGGER.info(errMsg);
            return SortClusterResponse.builder()
                    .msg(errMsg)
                    .code(RESPONSE_CODE_SUCCESS)
                    .build();
        }

        // if the same md5
        if (sortClusterMd5Map.get(clusterName).equals(md5)) {
            return SortClusterResponse.builder()
                    .msg("No update")
                    .code(RESPONSE_CODE_NO_UPDATE)
                    .md5(md5)
                    .build();
        }

        return SortClusterResponse.builder()
                .msg("Success")
                .code(RESPONSE_CODE_SUCCESS)
                .data(sortClusterConfigMap.get(clusterName))
                .md5(sortClusterMd5Map.get(clusterName))
                .build();
    }

    private void reloadAllClusterConfigV2() {
        List<StreamSinkEntity> sinkEntities = streamSinkEntityMapper.selectAllStreamSinks();
        // get all task under a given cluster, has been reduced into cluster and task.
        List<SortTaskInfo> tasks = streamSinkEntityMapper.selectAllTasks();
        Map<String, List<SortTaskInfo>> clusterTaskMap = tasks.stream()
                .filter(dto -> dto.getSortClusterName() != null)
                .collect(Collectors.groupingBy(SortTaskInfo::getSortClusterName));

        // get all stream sinks
        Map<String, List<StreamSink>> task2AllStreams = sinkEntities.stream()
                .filter(entity -> StringUtils.isNotBlank(entity.getInlongClusterName()))
                .map(entity -> {
                    StreamSinkOperator operator = sinkOperatorFactory.getInstance(entity.getSinkType());
                    return operator.getFromEntity(entity);
                })
                .collect(Collectors.groupingBy(StreamSink::getSinkName));

        // get all data nodes and group by node name
        List<DataNodeEntity> dataNodeEntities = dataNodeEntityMapper.selectAllDataNodes();
        Map<String, DataNodeInfo> task2DataNodeMap = dataNodeEntities.stream()
                .filter(entity -> StringUtils.isNotBlank(entity.getName()))
                .map(entity -> {
                    DataNodeOperator operator = dataNodeOperatorFactory.getInstance(entity.getType());
                    return operator.getFromEntity(entity);
                })
                .collect(Collectors.toMap(DataNodeInfo::getName, info -> info));

        Map<String, SortClusterConfig> newConfigMap = new ConcurrentHashMap<>();
        Map<String, String> newMd5Map = new ConcurrentHashMap<>();
        Map<String, String> newErrorLogMap = new ConcurrentHashMap<>();

        clusterTaskMap.forEach((clusterName, taskList) -> {
            try {
                SortClusterConfig config = this.getConfigByClusterNameV2(clusterName,
                        taskList, task2AllStreams, task2DataNodeMap);
                String jsonStr = GSON.toJson(config);
                String md5 = DigestUtils.md5Hex(jsonStr);
                newConfigMap.put(clusterName, config);
                newMd5Map.put(clusterName, md5);
            } catch (Throwable e) {
                // if get config failed, update the err log.
                newErrorLogMap.put(clusterName, e.getMessage());
                LOGGER.error("Failed to update cluster config of {}, error is {}", clusterName, e.getMessage());
            }
        });

        sortClusterErrorLogMap = newErrorLogMap;
        sortClusterConfigMap = newConfigMap;
        sortClusterMd5Map = newMd5Map;
    }

    private SortClusterConfig getConfigByClusterNameV2(
            String clusterName,
            List<SortTaskInfo> tasks,
            Map<String, List<StreamSink>> task2AllStreams,
            Map<String, DataNodeInfo> task2DataNodeMap) {

        List<SortTaskConfig> taskConfigs = tasks.stream()
                .map(task -> {
                    String taskName = task.getSortTaskName();
                    String type = task.getSinkType();
                    String dataNodeName = task.getDataNodeName();
                    DataNodeInfo nodeInfo = task2DataNodeMap.get(dataNodeName);
                    List<StreamSink> streams = task2AllStreams.get(taskName);

                    return SortTaskConfig.builder()
                            .name(taskName)
                            .type(type)
                            .idParams(this.parseIdParamsV2(streams))
                            .sinkParams(this.parseSinkParamsV2(nodeInfo))
                            .build();
                })
                .collect(Collectors.toList());

        return SortClusterConfig.builder()
                .clusterName(clusterName)
                .sortTasks(taskConfigs)
                .build();
    }

    private List<Map<String, String>> parseIdParamsV2(List<StreamSink> streams) {
        return streams.stream()
                .map(streamSink -> {
                    StreamSinkOperator operator = sinkOperatorFactory.getInstance(streamSink.getSinkType());
                    return operator.parse2IdParams(streamSink);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private Map<String, String> parseSinkParamsV2(DataNodeInfo nodeInfo) {
        DataNodeOperator operator = dataNodeOperatorFactory.getInstance(nodeInfo.getType());
        return operator.parse2SinkParams(nodeInfo);
    }

    /**
     * Reload all cluster config.
     * The results including config, md5 and error log, will replace the older ones.
     */
    private void reloadAllClusterConfig() {
        // get all task and group by cluster
        List<SortTaskInfo> tasks = streamSinkEntityMapper.selectAllTasks();
        Map<String, List<SortTaskInfo>> clusterTaskMap = tasks.stream()
                .filter(dto -> dto.getSortClusterName() != null)
                .collect(Collectors.groupingBy(SortTaskInfo::getSortClusterName));

        // get all id params and group by task
        List<SortIdInfo> idParams = streamSinkEntityMapper.selectAllIdParams();
        Map<String, List<SortIdInfo>> taskIdParamMap = idParams.stream()
                .filter(dto -> dto.getSortTaskName() != null)
                .collect(Collectors.groupingBy(SortIdInfo::getSortTaskName));

        // get all sink params and group by data node name
        List<SortSinkInfo> sinkParams = dataNodeEntityMapper.selectAllSinkParams();
        Map<String, SortSinkInfo> taskSinkParamMap = sinkParams.stream()
                .filter(dto -> dto.getName() != null)
                .collect(Collectors.toMap(SortSinkInfo::getName, param -> param));

        // update config of each cluster
        Map<String, SortClusterConfig> newConfigMap = new ConcurrentHashMap<>();
        Map<String, String> newMd5Map = new ConcurrentHashMap<>();
        Map<String, String> newErrorLogMap = new ConcurrentHashMap<>();
        clusterTaskMap.forEach((clusterName, taskList) -> {
            try {
                // get config, then update config map and md5
                SortClusterConfig clusterConfig = getConfigByClusterName(clusterName, taskList, taskIdParamMap,
                        taskSinkParamMap);
                String jsonStr = GSON.toJson(clusterConfig);
                String md5 = DigestUtils.md5Hex(jsonStr);
                newConfigMap.put(clusterName, clusterConfig);
                newMd5Map.put(clusterName, md5);
            } catch (Throwable e) {
                // if get config failed, update the err log.
                newErrorLogMap.put(clusterName, e.getMessage());
                LOGGER.error("Failed to update cluster config of {}, error is {}", clusterName, e.getMessage());
                LOGGER.error(e.getMessage(), e);
            }
        });
        sortClusterErrorLogMap = newErrorLogMap;
        sortClusterConfigMap = newConfigMap;
        sortClusterMd5Map = newMd5Map;
    }

    /**
     * Get the latest config of specific cluster.
     *
     * @param clusterName Cluster name.
     * @param tasks Task in this cluster.
     * @param taskIdParamMap All id params.
     * @param taskSinkParamMap All sink params.
     * @return The sort cluster config of specific cluster.
     */
    private SortClusterConfig getConfigByClusterName(
            String clusterName,
            List<SortTaskInfo> tasks,
            Map<String, List<SortIdInfo>> taskIdParamMap,
            Map<String, SortSinkInfo> taskSinkParamMap) {

        List<SortTaskConfig> taskConfigs = tasks.stream()
                .map(task -> {
                    String taskName = task.getSortTaskName();
                    String type = task.getSinkType();
                    List<SortIdInfo> idParams = taskIdParamMap.get(taskName);
                    SortSinkInfo sinkParams = taskSinkParamMap.get(task.getDataNodeName());
                    return this.getTaskConfig(taskName, type, idParams, sinkParams);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return SortClusterConfig.builder()
                .clusterName(clusterName)
                .sortTasks(taskConfigs)
                .build();
    }

    /**
     * Get task config.
     * <p/>
     * If there is not any id or sink params, throw exception to upper caller.
     *
     * @param taskName Task name.
     * @param type Type of sink.
     * @param idParams Id params.
     * @param sinkParams Sink params.
     * @return Task config.
     */
    private SortTaskConfig getTaskConfig(String taskName, String type, List<SortIdInfo> idParams,
            SortSinkInfo sinkParams) {
        // return null if id params or sink params are empty.
        if (idParams == null || sinkParams == null) {
            return null;
        }

        if (!type.equalsIgnoreCase(sinkParams.getType())) {
            throw new IllegalArgumentException(
                    String.format("task type %s and sink type %s are not identical for task name %s",
                            type, sinkParams.getType(), taskName));
        }

        return SortTaskConfig.builder()
                .name(taskName)
                .type(type)
                .idParams(this.parseIdParams(idParams))
                .sinkParams(this.parseSinkParams(sinkParams))
                .build();
    }

    /**
     * Parse id params from json.
     *
     * @param rowIdParams IdParams in json format.
     * @return List of IdParams.
     */
    private List<Map<String, String>> parseIdParams(List<SortIdInfo> rowIdParams) {
        return rowIdParams.stream()
                .map(row -> {
                    Map<String, String> param = GSON.fromJson(row.getExtParams(), HashMap.class);
                    // put group and stream info
                    param.put(KEY_GROUP_ID, row.getInlongGroupId());
                    param.put(KEY_STREAM_ID, row.getInlongStreamId());
                    return param;
                })
                .collect(Collectors.toList());
    }

    /**
     * Parse sink params from json.
     *
     * @param rowSinkParams Sink params in json format.
     * @return Sink params.
     */
    private Map<String, String> parseSinkParams(SortSinkInfo rowSinkParams) {
        return GSON.fromJson(rowSinkParams.getExtParams(), HashMap.class);
    }

    /**
     * Set reload timer at the beginning of repository.
     */
    private void setReloadTimer() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(this::reload, reloadInterval, reloadInterval, TimeUnit.MILLISECONDS);
    }
}
