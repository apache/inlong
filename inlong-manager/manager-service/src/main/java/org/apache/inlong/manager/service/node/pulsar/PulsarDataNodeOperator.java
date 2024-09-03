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

package org.apache.inlong.manager.service.node.pulsar;

import org.apache.inlong.common.pojo.sort.node.NodeConfig;
import org.apache.inlong.common.pojo.sort.node.PulsarNodeConfig;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeDTO;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeInfo;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeRequest;
import org.apache.inlong.manager.service.node.AbstractDataNodeOperator;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

/**
 * Pulsar data node operator
 */
@Service
public class PulsarDataNodeOperator extends AbstractDataNodeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarDataNodeOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RestTemplate restTemplate;

    @Override
    public Boolean accept(String dataNodeType) {
        return getDataNodeType().equals(dataNodeType);
    }

    @Override
    public String getDataNodeType() {
        return DataNodeType.PULSAR;
    }

    @Override
    public DataNodeInfo getFromEntity(DataNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_NOT_FOUND);
        }

        PulsarDataNodeInfo pulsarDataNodeInfo = new PulsarDataNodeInfo();
        CommonBeanUtils.copyProperties(entity, pulsarDataNodeInfo);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            PulsarDataNodeDTO dto = PulsarDataNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, pulsarDataNodeInfo);
        }
        return pulsarDataNodeInfo;
    }

    @Override
    protected void setTargetEntity(DataNodeRequest request, DataNodeEntity targetEntity) {
        PulsarDataNodeRequest nodeRequest = (PulsarDataNodeRequest) request;
        CommonBeanUtils.copyProperties(nodeRequest, targetEntity, true);
        try {
            PulsarDataNodeDTO dto = PulsarDataNodeDTO.getFromRequest(nodeRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("Failed to build extParams for pulsar node: %s", e.getMessage()));
        }
    }

    @Override
    public Boolean testConnection(DataNodeRequest request) {
        PulsarDataNodeRequest pulsarDataNodeRequest = (PulsarDataNodeRequest) request;
        String adminUrl = pulsarDataNodeRequest.getAdminUrl();
        String token = pulsarDataNodeRequest.getToken();
        Preconditions.expectNotBlank(adminUrl, ErrorCodeEnum.INVALID_PARAMETER, "connection admin urlcannot be empty");
        if (getPulsarConnection(adminUrl, token)) {
            LOGGER.info("pulsar  connection success for adminUrl={}, token={}",
                    adminUrl, token);
        }
        return true;
    }

    private boolean getPulsarConnection(String adminUrl, String token) {

        PulsarClusterInfo pulsarClusterInfo = PulsarClusterInfo.builder().adminUrl(adminUrl)
                .token(token).build();
        try {
            // test connect for pulsar adminUrl
            PulsarUtils.getTenants(restTemplate, pulsarClusterInfo);
            return true;
        } catch (Exception e) {
            String errMsg = String.format("Pulsar connection failed for AdminUrl=%s", pulsarClusterInfo.getAdminUrl());
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    @Override
    public NodeConfig getNodeConfig(DataNodeEntity dataNodeEntity) {
        DataNodeInfo dataNodeInfo = this.getFromEntity(dataNodeEntity);
        PulsarNodeConfig pulsarNodeConfig = CommonBeanUtils.copyProperties(dataNodeInfo, PulsarNodeConfig::new);
        PulsarDataNodeDTO dto = PulsarDataNodeDTO.getFromJson(dataNodeEntity.getExtParams());
        CommonBeanUtils.copyProperties(dto, pulsarNodeConfig);
        pulsarNodeConfig.setNodeName(dataNodeInfo.getName());
        return pulsarNodeConfig;
    }

}
