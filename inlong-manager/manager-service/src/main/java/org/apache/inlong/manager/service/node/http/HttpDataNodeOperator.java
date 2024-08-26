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

package org.apache.inlong.manager.service.node.http;

import org.apache.inlong.common.pojo.sort.node.HttpNodeConfig;
import org.apache.inlong.common.pojo.sort.node.NodeConfig;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.http.HttpDataNodeDTO;
import org.apache.inlong.manager.pojo.node.http.HttpDataNodeInfo;
import org.apache.inlong.manager.pojo.node.http.HttpDataNodeRequest;
import org.apache.inlong.manager.service.node.AbstractDataNodeOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HttpDataNodeOperator extends AbstractDataNodeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpDataNodeOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    protected void setTargetEntity(DataNodeRequest request, DataNodeEntity targetEntity) {
        HttpDataNodeRequest httpDataNodeRequest = (HttpDataNodeRequest) request;
        CommonBeanUtils.copyProperties(httpDataNodeRequest, targetEntity, true);
        try {
            HttpDataNodeDTO dto = HttpDataNodeDTO.getFromRequest(httpDataNodeRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("Failed to build extParams for Cloud log service node: %s", e.getMessage()));
        }
    }

    @Override
    public Boolean accept(String dataNodeType) {
        return DataNodeType.HTTP.equals(dataNodeType);
    }

    @Override
    public String getDataNodeType() {
        return DataNodeType.HTTP;
    }

    @Override
    public DataNodeInfo getFromEntity(DataNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_NOT_FOUND);
        }
        HttpDataNodeInfo info = new HttpDataNodeInfo();
        CommonBeanUtils.copyProperties(entity, info);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            HttpDataNodeDTO dto = HttpDataNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, info);
        }
        return info;
    }

    @Override
    public NodeConfig getNodeConfig(DataNodeEntity dataNodeEntity) {
        DataNodeInfo dataNodeInfo = this.getFromEntity(dataNodeEntity);
        HttpNodeConfig httpNodeConfig = CommonBeanUtils.copyProperties(dataNodeInfo, HttpNodeConfig::new);
        HttpDataNodeDTO dto = HttpDataNodeDTO.getFromJson(dataNodeEntity.getExtParams());
        CommonBeanUtils.copyProperties(dto, httpNodeConfig);
        httpNodeConfig.setPassword(dataNodeEntity.getToken());
        httpNodeConfig.setNodeName(dataNodeInfo.getName());
        return httpNodeConfig;
    }
}
