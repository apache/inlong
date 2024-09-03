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

import org.apache.inlong.common.pojo.sort.mq.MqClusterConfig;
import org.apache.inlong.common.pojo.sort.mq.PulsarClusterConfig;
import org.apache.inlong.common.pojo.sort.mq.TubeClusterConfig;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.ClusterConfigEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.mapper.ClusterConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.cluster.tubemq.TubeClusterInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Cluster config service layer implementation
 */
@Service
public class ClusterConfigServiceImpl implements ClusterConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfigServiceImpl.class);

    @Autowired
    private ClusterConfigEntityMapper clusterConfigEntityMapper;
    @Autowired
    private InlongClusterEntityMapper clusterEntityMapper;
    @Autowired
    private InlongClusterOperatorFactory clusterOperatorFactory;

    @Override
    public boolean refresh(String clusterTag, String operator) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ClusterPageRequest request = ClusterPageRequest.builder()
                    .clusterTag(clusterTag)
                    .typeList(Arrays.asList(ClusterType.TUBEMQ, ClusterType.PULSAR, ClusterType.KAFKA))
                    .build();
            List<InlongClusterEntity> clusterEntityList = clusterEntityMapper.selectByCondition(request);
            if (CollectionUtils.isEmpty(clusterEntityList)) {
                throw new BusinessException("Current cluster tag not contain MQ clusters");
            }
            List<String> typeList = clusterEntityList.stream().map(InlongClusterEntity::getType).distinct().collect(
                    Collectors.toList());
            if (CollectionUtils.isNotEmpty(typeList) && typeList.size() > 1) {
                throw new BusinessException("Current cluster tag can not contain multiple MQ types");
            }
            ClusterConfigEntity existEntity = clusterConfigEntityMapper.selectByClusterTag(clusterTag);
            ClusterConfigEntity clusterConfigEntity = existEntity == null ? new ClusterConfigEntity() : existEntity;
            if (CollectionUtils.isNotEmpty(clusterEntityList)) {
                String clusterType = clusterEntityList.get(0).getType();
                InlongClusterOperator clusterOperator = clusterOperatorFactory.getInstance(clusterType);
                List<MqClusterConfig> list = new ArrayList<>();
                for (InlongClusterEntity clusterInfo : clusterEntityList) {
                    switch (clusterType) {
                        case ClusterType.PULSAR:
                            PulsarClusterInfo pulsarCluster =
                                    (PulsarClusterInfo) clusterOperator.getFromEntity(clusterInfo);
                            PulsarClusterConfig pulsarClusterConfig = CommonBeanUtils.copyProperties(pulsarCluster,
                                    PulsarClusterConfig::new, true);
                            pulsarClusterConfig.setVersion(pulsarCluster.getVersion());
                            pulsarClusterConfig.setClusterName(pulsarCluster.getName());
                            pulsarClusterConfig.setServiceUrl(pulsarCluster.getUrl());
                            list.add(pulsarClusterConfig);
                            break;
                        case ClusterType.TUBEMQ:
                            TubeClusterInfo tubeClusterInfo =
                                    (TubeClusterInfo) clusterOperator.getFromEntity(clusterInfo);
                            TubeClusterConfig tubeClusterConfig = CommonBeanUtils.copyProperties(tubeClusterInfo,
                                    TubeClusterConfig::new, true);
                            tubeClusterConfig.setVersion(tubeClusterInfo.getVersion());
                            tubeClusterConfig.setClusterName(tubeClusterInfo.getName());
                            tubeClusterConfig.setMasterAddress(tubeClusterInfo.getUrl());
                            list.add(tubeClusterConfig);
                            break;
                        default:
                            throw new BusinessException(
                                    String.format(ErrorCodeEnum.MQ_TYPE_NOT_SUPPORTED.getMessage(), clusterType));
                    }
                }
                clusterConfigEntity.setConfigParams(objectMapper.writeValueAsString(list));
                clusterConfigEntity.setClusterType(clusterType);
            }
            clusterConfigEntity.setClusterTag(clusterTag);
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
