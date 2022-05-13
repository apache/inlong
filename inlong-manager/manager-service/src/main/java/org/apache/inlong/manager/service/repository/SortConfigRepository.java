/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.manager.service.repository;

import com.google.gson.Gson;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.pojo.dataproxy.IRepository;
import org.apache.inlong.common.pojo.sortstandalone.SortClusterConfig;
import org.apache.inlong.common.pojo.sortstandalone.SortClusterResponse;
import org.apache.inlong.common.pojo.sortstandalone.SortTaskConfig;
import org.apache.inlong.manager.dao.entity.SortIdParamsDTO;
import org.apache.inlong.manager.dao.entity.SortSinkParamsDTO;
import org.apache.inlong.manager.dao.entity.SortTaskDTO;
import org.apache.inlong.manager.dao.mapper.SortClusterMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Repository
public class SortConfigRepository implements IRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortConfigRepository.class);

    private static final Gson gson = new Gson();

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
    private SortClusterMapper sortClusterMapper;

    @PostConstruct
    public void initialize() {
        LOGGER.info("create repository for " + SortConfigRepository.class.getSimpleName());
        try {
            this.reloadInterval = DEFAULT_HEARTBEAT_INTERVAL_MS;
            reload();
            setReloadTimer();
        } catch (Throwable t) {
            LOGGER.error("Initialize SortClusterConfigRepository error", t);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void reload() {
        LOGGER.debug("start to reload sort config.");
        reloadClusterConfig();
        LOGGER.debug("end to reload config");
    }

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

        // there is no config
        if (sortClusterConfigMap.get(clusterName) == null) {
            String errMsg = "There is not config for cluster " + clusterName;
            LOGGER.info(errMsg);
            return SortClusterResponse.builder()
                    .msg(errMsg)
                    .code(RESPONSE_CODE_REQ_PARAMS_ERROR)
                    .build();
        }

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

    private void reloadClusterConfig() {
        // get all task and group by cluster
        List<SortTaskDTO> tasks = sortClusterMapper.selectAllTasks();
        Map<String, List<SortTaskDTO>> clusterTaskMap =
                tasks.stream().collect(Collectors.groupingBy(SortTaskDTO::getSortClusterName));

        // get all id params and group by task
        List<SortIdParamsDTO> idParams = sortClusterMapper.selectAllIdParams();
        Map<String, List<SortIdParamsDTO>> taskIdParamMap =
                idParams.stream().collect(Collectors.groupingBy(SortIdParamsDTO::getSortTaskName));

        // get all sink params and group by data node name
        List<SortSinkParamsDTO> sinkParams = sortClusterMapper.selectAllSinkParams();
        Map<String, SortSinkParamsDTO> taskSinkParamMap =
                sinkParams.stream().collect(Collectors.toMap(SortSinkParamsDTO::getName, param -> param));

        // update config of each cluster
        Map<String, SortClusterConfig> newConfigMap = new ConcurrentHashMap<>();
        Map<String, String> newMd5Map = new ConcurrentHashMap<>();
        Map<String, String> newErrorlogMap = new ConcurrentHashMap<>();
        clusterTaskMap.forEach((clusterName, taskList) -> {
            try {
                SortClusterConfig clusterConfig =
                        updateConfigByClusterName(clusterName, taskList, taskIdParamMap, taskSinkParamMap);
                String jsonStr = gson.toJson(clusterConfig);
                String md5 = DigestUtils.md5Hex(jsonStr);
                newConfigMap.put(clusterName, clusterConfig);
                newMd5Map.put(clusterName, md5);
            } catch (Throwable e) {
                newErrorlogMap.put(clusterName, e.getMessage());
                LOGGER.error("Failed to update cluster config of {}, error is {}", clusterName, e.getMessage());
                LOGGER.error(e.getMessage(), e);
            }
        });
        sortClusterErrorLogMap = newErrorlogMap;
        sortClusterConfigMap = newConfigMap;
        sortClusterMd5Map = newMd5Map;
    }

    private SortClusterConfig updateConfigByClusterName(
            String clusterName,
            List<SortTaskDTO> tasks,
            Map<String, List<SortIdParamsDTO>> taskIdParamMap,
            Map<String, SortSinkParamsDTO> taskSinkParamMap) {

        List<SortTaskConfig> taskConfigs = tasks.stream()
                .map(task -> {
                    String taskName = task.getSortTaskName();
                    String type = task.getSinkType();
                    List<SortIdParamsDTO> idParams = taskIdParamMap.get(taskName);
                    SortSinkParamsDTO sinkParams = taskSinkParamMap.get(task.getDataNodeName());
                    return this.getTaskConfig(taskName, type, idParams, sinkParams);
                })
                .collect(Collectors.toList());

        return SortClusterConfig.builder()
                .clusterName(clusterName)
                .sortTasks(taskConfigs)
                .build();
    }

    private SortTaskConfig getTaskConfig(
            String taskName,
            String type,
            List<SortIdParamsDTO> idParams,
            SortSinkParamsDTO sinkParams) {

        Optional.ofNullable(idParams)
                .orElseThrow(() -> new IllegalStateException(("There is no any id params of task " + taskName)));
        Optional.ofNullable(sinkParams)
                .orElseThrow(() -> new IllegalStateException("There is no any sink params of task " + taskName));

        if (!type.equalsIgnoreCase(sinkParams.getType())) {
            throw new IllegalArgumentException(
                    String.format("for task %s, task type %s and sink type %s are not identical",
                            taskName, type, sinkParams.getType()));
        }

        return SortTaskConfig.builder()
                .name(taskName)
                .type(type)
                .idParams(this.parseIdParams(idParams))
                .sinkParams(this.parseSinkParams(sinkParams))
                .build();

    }

    private List<Map<String, String>> parseIdParams(List<SortIdParamsDTO> rowIdParams) {
        return rowIdParams.stream()
                        .map(row -> {
                            Map<String, String> param = gson.fromJson(row.getExtParams(), HashMap.class);
                            // put group and stream info
                            param.put(KEY_GROUP_ID, row.getInlongGroupId());
                            param.put(KEY_STREAM_ID, row.getInlongStreamId());
                            return param;
                        })
                        .collect(Collectors.toList());
    }

    private Map<String, String> parseSinkParams(SortSinkParamsDTO rowSinkParams) {
        return gson.fromJson(rowSinkParams.getExtParams(), HashMap.class);
    }

    /**
     * Set reload timer at the beginning of repository.
     */
    private void setReloadTimer() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(this::reload, reloadInterval, reloadInterval, TimeUnit.MILLISECONDS);
    }
}








