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

package org.apache.inlong.manager.service.cluster;

import org.apache.inlong.common.pojo.sort.mq.PulsarClusterConfig;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.ClusterConfigEntity;
import org.apache.inlong.manager.dao.mapper.ClusterConfigEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Cluster config service layer implementation
 */
@Service
public class ClusterConfigServiceImpl implements ClusterConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfigServiceImpl.class);

    @Autowired
    private ClusterConfigEntityMapper clusterConfigEntityMapper;
    @Autowired
    private InlongClusterService clusterService;

    @Override
    public boolean refresh(String clusterTag, String operator) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(clusterTag, ClusterType.PULSAR);
            List<PulsarClusterConfig> list = new ArrayList<>();
            ClusterConfigEntity existEntity = clusterConfigEntityMapper.selectByClusterTag(clusterTag);
            for (ClusterInfo clusterInfo : clusterInfos) {
                PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;
                PulsarClusterConfig pulsarClusterConfig = CommonBeanUtils.copyProperties(pulsarCluster,
                        PulsarClusterConfig::new);
                pulsarClusterConfig.setVersion(pulsarCluster.getVersion());
                pulsarClusterConfig.setClusterName(pulsarCluster.getName());
                pulsarClusterConfig.setServiceUrl(pulsarCluster.getUrl());
                list.add(pulsarClusterConfig);
            }
            ClusterConfigEntity clusterConfigEntity = existEntity == null ? new ClusterConfigEntity() : existEntity;
            clusterConfigEntity.setConfigParams(objectMapper.writeValueAsString(list));
            clusterConfigEntity.setClusterTag(clusterTag);
            clusterConfigEntity.setClusterType(ClusterType.PULSAR);
            clusterConfigEntity.setModifier(operator);
            if (existEntity == null) {
                clusterConfigEntity.setCreator(operator);
                clusterConfigEntityMapper.insert(clusterConfigEntity);
            } else {
                clusterConfigEntityMapper.updateByIdSelective(clusterConfigEntity);
            }
        } catch (Exception e) {
            String errMsg =
                    String.format("push cluster config failed for cluster Tag=%s", clusterTag);
            LOGGER.error(errMsg, e);
            throw new WorkflowListenerException(errMsg);
        }
        return true;
    }

}
