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

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.inlong.manager.common.pojo.sort.SortClusterConfigResponse;
import org.apache.inlong.manager.common.pojo.sort.SortClusterConfigResponse.SortClusterConfig;
import org.apache.inlong.manager.common.pojo.sort.SortClusterConfigResponse.SortTaskConfig;
import org.apache.inlong.manager.dao.entity.TaskConfigEntity;
import org.apache.inlong.manager.dao.mapper.SortClusterConfigMapper;
import org.apache.inlong.manager.service.core.SortClusterConfigService;
import org.apache.inlong.manager.service.core.TaskConfigService;
import org.apache.inlong.manager.service.core.TaskIdParamsKafkaService;
import org.apache.inlong.manager.service.core.TaskIdParamsPulsarService;
import org.apache.inlong.manager.service.core.TaskSinkParamsEsService;
import org.apache.inlong.manager.service.core.TaskSinkParamsKafkaService;
import org.apache.inlong.manager.service.core.TaskSinkParamsPulsarService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of tSort Cluster config service layer interface.
 */
@Service
@Slf4j
public class SortClusterConfigServiceImpl implements SortClusterConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortClusterConfigServiceImpl.class);

    @Autowired
    private SortClusterConfigMapper sortClusterConfigMapper;

    @Autowired
    private TaskConfigService configService;

    @Autowired
    private TaskIdParamsPulsarService idParamsPulsarService;

    @Autowired
    private TaskIdParamsKafkaService idParamsKafkaService;

    @Autowired
    private TaskSinkParamsEsService sinkParamsEsService;

    @Autowired
    private TaskSinkParamsKafkaService sinkParamsKafkaService;

    @Autowired
    private TaskSinkParamsPulsarService sinkParamsPulsarService;

    @Override
    public SortClusterConfigResponse get(
            String clusterName,
            String md5) {
        SortClusterConfigResponse response = new SortClusterConfigResponse();
        String retMsg = null;
        // if the cluster name is invalid, return RESP_CODE_REQ_PARAMS_ERROR
        if (isErrorReqParams(clusterName)) {
            retMsg = "Empty cluster name, return RESP_CODE_REQ_PARAMS_ERROR";
            LOGGER.error(retMsg);
            response.setCode(SortClusterConfigResponse.RESP_CODE_REQ_PARAMS_ERROR);
            response.setMsg(retMsg);
            return response;
        }

        // if the cluster config is null, return RESP_CODE_FAIL
        SortClusterConfig clusterConfig = this.getClusterConfig(clusterName);
        if (clusterConfig == null) {
            retMsg = "Cannot find cluster config for cluster, return RESP_CODE_FAIL";
            response.setCode(SortClusterConfigResponse.RESP_CODE_FAIL);
            response.setMsg(retMsg);
            return response;
        }

        // if md5 is the same as last request, return RESP_CODE_NO_UPDATE
        String jsonClusterConfig = JSON.toJSONString(clusterConfig);
        String localMd5 = DigestUtils.md5Hex(jsonClusterConfig);
        if (localMd5.equals(md5)) {
            retMsg = "Same md5 with the last request, return RESP_CODE_NO_UPDATE";
            LOGGER.info(retMsg);
            response.setCode(SortClusterConfigResponse.RESP_CODE_NO_UPDATE);
            response.setMsg(retMsg);
            response.setMd5(md5);
            return response;
        }

        // if md5 changes, return RESP_CODE_SUCC with new cluster config.
        response.setClusterConfig(clusterConfig);
        response.setCode(SortClusterConfigResponse.RESP_CODE_SUCC);
        response.setMd5(localMd5);
        LOGGER.info("Get response successfully {}", jsonClusterConfig);
        return response;
    }

    /**
     * Get cluster config by cluster name.
     * @param clusterName Cluster name.
     * @return Cluster config.
     */
    private SortClusterConfig getClusterConfig(String clusterName) {
        List<String> tasks = sortClusterConfigMapper.selectTasksByClusterName(clusterName);
        // if there is no task, return null.
        if (tasks == null || tasks.size() == 0) {
            LOGGER.error("there is no task for cluster: {}", clusterName);
            return null;
        }
        LOGGER.info("Got task " + tasks);
        SortClusterConfig clusterConfig = new SortClusterConfig();
        List<SortTaskConfig> sortTaskConfig = new ArrayList<>();
        // get task config of each task.
        tasks.forEach(taskName -> {
            SortTaskConfig taskConfig = this.getTaskConfig(taskName);
            LOGGER.info("task : {} has config {}", taskName, taskConfig);
            sortTaskConfig.add(taskConfig);
        });
        clusterConfig.setClusterName(clusterName);
        clusterConfig.setSortTaskConfig(sortTaskConfig);
        return clusterConfig;
    }

    /**
     * Get task config by task name.
     * @param taskName Task name.
     * @return Task config.
     */
    private SortTaskConfig getTaskConfig(String taskName) {
        TaskConfigEntity configEntity = configService.selectByTaskName(taskName);
        Map<String, String> sinkParams = this.getSinkParams(configEntity.getType(), taskName);
        LOGGER.info("Get sink params " + sinkParams);
        List<Map<String, String>> idParams = this.getIdParams(taskName, configEntity);
        LOGGER.info("Get id params " + idParams);
        SortTaskConfig taskConfig = new SortTaskConfig();
        taskConfig.setIdParams(idParams);
        taskConfig.setSinkParams(sinkParams);
        taskConfig.setName(taskName);
        taskConfig.setType(configEntity.getType());
        return taskConfig;
    }

    /**
     * Get sink params from the type of sink and task name.
     * @param type Type of sink.
     * @param taskName Task name.
     * @return Sink params map.
     */
    private Map<String, String> getSinkParams(
            String type,
            String taskName) {
        switch (type) {
            case "hive":
                break;
            case "tube":
                break;
            case "kafka":
                return sinkParamsKafkaService.selectByTaskName(taskName);
            case "pulsar":
                return sinkParamsPulsarService.selectByTaskName(taskName);
            case "es":
                return sinkParamsEsService.selectByTaskName(taskName);
            default:
                LOGGER.error("Unknown sink type: {}", type);
                break;
        }
        LOGGER.error("Return empty map in getSinkParams");
        return new HashMap<>(0);
    }

    /**
     * Get id params of each task.
     * @param taskName Task name.
     * @param configEntity Common configs.
     * @return List of id params map.
     */
    private List<Map<String, String>> getIdParams(
            String taskName,
            TaskConfigEntity configEntity) {
        List<Map<String, String>> idParams = new ArrayList<>();
        this.addKafkaIdParam(idParams, taskName, configEntity);
        this.addPulsarIdParam(idParams, taskName, configEntity);
        return idParams;
    }

    /**
     * Add inlong gourp id and stream id to kafka id param, and put param to the list.
     * @param idParams List of all id params.
     * @param taskName Task name.
     * @param configEntity common config of task.
     */
    private void addKafkaIdParam(
            List<Map<String, String>> idParams,
            String taskName,
            TaskConfigEntity configEntity) {
        Map<String, String> params = idParamsKafkaService.selectByTaskName(taskName);
        if (params == null) {
            LOGGER.debug("Get empty kafka id params");
            return;
        }
        params.put("type", "kafka");
        params.put("inlongGroupId", configEntity.getInlongGroupId());
        params.put("inlongStreamId", configEntity.getInlongStreamId());
        LOGGER.debug("Get kafka id params " + params);
        idParams.add(params);
    }

    /**
     * Add inlong gourp id and stream id to pulsar id param, and put param to the list.
     * @param idParams List of all id params.
     * @param taskName Task name.
     * @param configEntity common config of task.
     */
    private void addPulsarIdParam(
            List<Map<String, String>> idParams,
            String taskName,
            TaskConfigEntity configEntity) {
        Map<String, String> params = idParamsPulsarService.selectByTaskName(taskName);
        if (params == null) {
            LOGGER.debug("Get empty pulsar id params");
            return;
        }
        params.put("type", "pulsar");
        params.put("inlongGroupId", configEntity.getInlongGroupId());
        params.put("inlongStreamId", configEntity.getInlongStreamId());
        LOGGER.debug("Get pulsar id params " + params);
        idParams.add(params);
    }

    private boolean isErrorReqParams(String clusterName) {
        if (clusterName == null || clusterName.isEmpty()) {
            return true;
        }
        return false;
    }

}
