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

    private static final Gson gson = new Gson();

    @Override
    public SortClusterConfigResponse get(
            String clusterName,
            String md5) {
        SortClusterConfigResponse response = new SortClusterConfigResponse();
        response.setResult(true);

        // if the cluster name is invalid, return RESP_CODE_REQ_PARAMS_ERROR
        if (isErrorReqParams(clusterName)) {
            response.setErrCode(SortClusterConfigResponse.RESP_CODE_REQ_PARAMS_ERROR);
            response.setResult(false);
            System.out.println("empty cluster name");
            return response;
        }

        // if the cluster config is null, return RESP_CODE_FAIL
        SortClusterConfig clusterConfig = this.getClusterConfig(clusterName);
        if (clusterConfig == null) {
            response.setErrCode(SortClusterConfigResponse.RESP_CODE_FAIL);
            response.setResult(false);
            return response;
        }

        // if md5 is the same as last request, return RESP_CODE_NO_UPDATE
        String jsonClusterConfig = gson.toJson(clusterConfig);
        String localMd5 = DigestUtils.md5Hex(jsonClusterConfig);
        if (localMd5.equals(md5)) {
            response.setErrCode(SortClusterConfigResponse.RESP_CODE_NO_UPDATE);
            return response;
        }

        // if md5 changes, return RESP_CODE_SUCC with new cluster config.
        response.setClusterConfig(clusterConfig);
        response.setErrCode(SortClusterConfigResponse.RESP_CODE_SUCC);
        response.setMd5(md5);
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
            System.out.println("got empty tasks");
            return null;
        }

        SortClusterConfig clusterConfig = new SortClusterConfig();
        List<SortTaskConfig> sortTaskConfig = new ArrayList<>();
        // get task config of each task.
        tasks.forEach(taskName -> {
            SortTaskConfig taskConfig = this.getTaskConfig(taskName);
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
        List<Map<String, String>> idParams = this.getIdParams(taskName, configEntity);
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
        this.addIdParam(idParams, idParamsKafkaService.selectByTaskName(taskName), configEntity);
        this.addIdParam(idParams, idParamsPulsarService.selectByTaskName(taskName), configEntity);
        return idParams;
    }

    /**
     * Add inlong gourp id and stream id to each id param, and put param to the list.
     * @param idParams List of id params.
     * @param params Single params of one id.
     * @param configEntity Common config of each id params.
     */
    private void addIdParam(
            List<Map<String, String>> idParams,
            Map<String, String> params,
            TaskConfigEntity configEntity) {
        if (params == null) {
            return;
        }
        params.put("inlongGroupId", configEntity.getInlongGroupId());
        params.put("inlongStreamId", configEntity.getInlongStreamId());
        idParams.add(params);
    }

    private boolean isErrorReqParams(String clusterName) {
        if (clusterName == null || clusterName.isEmpty()) {
            return true;
        }
        return false;
    }

}
