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

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.sort.SortClusterConfigResponse;
import org.apache.inlong.manager.common.pojo.sort.SortSourceConfigResponse;
import org.apache.inlong.manager.dao.entity.SortClusterConfigEntity;
import org.apache.inlong.manager.common.pojo.sort.SortClusterConfigResponse.SinkType;
import org.apache.inlong.manager.common.pojo.sort.SortClusterConfigResponse.SortTaskConfig;
import org.apache.inlong.manager.service.core.SortClusterConfigService;
import org.apache.inlong.manager.service.core.SortTaskIdParamService;
import org.apache.inlong.manager.service.core.SortService;
import org.apache.inlong.manager.service.core.SortTaskSinkParamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Sort service implementation. */
@Service
public class SortServiceImpl implements SortService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortServiceImpl.class);

    @Autowired private SortClusterConfigService sortClusterConfigService;

    @Autowired private SortTaskIdParamService sortTaskIdParamService;

    @Autowired private SortTaskSinkParamService sortTaskSinkParamService;

    @Override
    public SortClusterConfigResponse getClusterConfig(String clusterName, String md5) {
        LOGGER.info("start getClusterConfig");

        // check if cluster name is valid or not.
        if (StringUtils.isBlank(clusterName)) {
            String errMsg = "Blank cluster name, return nothing";
            LOGGER.info(errMsg);
            return SortClusterConfigResponse.builder().msg(errMsg).build();
        }

        // check if there is any task.
        List<SortClusterConfigEntity> tasks = sortClusterConfigService.selectTasksByClusterName(clusterName);

        if (tasks == null || tasks.isEmpty()) {
            String errMsg = "There is not any task for cluster" + clusterName;
            LOGGER.info(errMsg);
            return SortClusterConfigResponse.builder().msg(errMsg).build();
        }

        // add task configs
        List<SortTaskConfig> taskConfigs = new ArrayList<>();
        try {
            tasks.forEach(clusterConfig -> taskConfigs.add(this.getTaskConfig(clusterConfig)));
        } catch (IllegalArgumentException ex) {
            String errMsg = "Got illegal sink type from db, " + ex.getMessage();
            LOGGER.info(errMsg);
            return SortClusterConfigResponse.builder().msg(errMsg).build();
        }

        return SortClusterConfigResponse.builder().tasks(taskConfigs).msg("success").build();
    }

    @Override
    public SortSourceConfigResponse getSourceConfig(String clusterName, String sortTaskId, String md5) {
        return null;
    }

    private SortTaskConfig getTaskConfig(SortClusterConfigEntity clusterConfig) {
        SinkType sinkType = SinkType.valueOf(clusterConfig.getSinkType().toUpperCase());
        List<Map<String, String>> idParams =
                sortTaskIdParamService.selectByTaskName(clusterConfig.getTaskName());
        Map<String, String> sinkParams =
                sortTaskSinkParamService
                        .selectByTaskNameAndType(clusterConfig.getTaskName(), clusterConfig.getSinkType());
        return SortTaskConfig.builder()
                .taskName(clusterConfig.getTaskName())
                .sinkType(sinkType)
                .idParams(idParams)
                .sinkParams(sinkParams)
                .build();
    }
}
