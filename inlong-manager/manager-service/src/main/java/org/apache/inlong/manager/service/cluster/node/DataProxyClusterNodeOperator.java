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

package org.apache.inlong.manager.service.cluster.node;

import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.pojo.cluster.dataproxy.DataProxyClusterNodeDTO;
import org.apache.inlong.manager.pojo.cluster.dataproxy.DataProxyClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.dataproxy.DataProxyClusterNodeResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Data proxy cluster node operator.
 */
@Slf4j
@Service
public class DataProxyClusterNodeOperator extends AbstractClusterNodeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataProxyClusterNodeOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String clusterNodeType) {
        return getClusterNodeType().equals(clusterNodeType);
    }

    @Override
    public String getClusterNodeType() {
        return ClusterType.DATAPROXY;
    }

    @Override
    public ClusterNodeResponse getFromEntity(InlongClusterNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        DataProxyClusterNodeResponse dataProxyClusterNodeResponse = new DataProxyClusterNodeResponse();
        CommonBeanUtils.copyProperties(entity, dataProxyClusterNodeResponse);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            DataProxyClusterNodeDTO dto = DataProxyClusterNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, dataProxyClusterNodeResponse);
        }

        LOGGER.debug("success to get data proxy cluster node info from entity");
        return dataProxyClusterNodeResponse;
    }

    @Override
    protected void setTargetEntity(ClusterNodeRequest request, InlongClusterNodeEntity targetEntity) {
        DataProxyClusterNodeRequest dataProxyClusterNodeRequest = (DataProxyClusterNodeRequest) request;
        CommonBeanUtils.copyProperties(dataProxyClusterNodeRequest, targetEntity, true);
        try {
            DataProxyClusterNodeDTO dto = DataProxyClusterNodeDTO.getFromRequest(dataProxyClusterNodeRequest,
                    targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
            LOGGER.debug("success to set entity for data proxy cluster node");
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT,
                    String.format("serialize extParams of Data proxy ClusterNode failure: %s", e.getMessage()));
        }
    }

}
