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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.sort.SortClusterConfigResponse;
import org.apache.inlong.manager.dao.entity.SortClusterConfgiEntity;
import org.apache.inlong.manager.service.core.SortClusterConfigService;
import org.apache.inlong.manager.service.core.SortService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Sort service implementation.
 */
@Slf4j
@Service
public class SortServiceImpl implements SortService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortServiceImpl.class);

    @Autowired
    private SortClusterConfigService sortClusterConfigService;

    @Override
    public SortClusterConfigResponse getClusterConfig(String clusterName, String md5) {
        LOGGER.info("start getClusterConfig");

        if (StringUtils.isBlank(clusterName)) {
            String errMsg = "Blank cluster name";
            LOGGER.info(errMsg);
            return SortClusterConfigResponse.builder()
                    .msg(errMsg).build();
        }

        List<SortClusterConfgiEntity> tasks = sortClusterConfigService.selectTasksByClusterName(clusterName);
        if (tasks == null || tasks.isEmpty()) {
            String errMsg = "There is not any task for cluster" + clusterName;
            LOGGER.info(errMsg);
            return SortClusterConfigResponse.builder()
                    .msg(errMsg).build();
        }

        return SortClusterConfigResponse.builder()
                .msg("success").build();
    }

}
