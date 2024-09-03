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

package org.apache.inlong.manager.service.node.cls;

import org.apache.inlong.common.pojo.sort.node.ClsNodeConfig;
import org.apache.inlong.common.pojo.sort.node.NodeConfig;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.cls.ClsDataNodeDTO;
import org.apache.inlong.manager.pojo.node.cls.ClsDataNodeInfo;
import org.apache.inlong.manager.pojo.node.cls.ClsDataNodeRequest;
import org.apache.inlong.manager.service.node.AbstractDataNodeOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencentcloudapi.cls.v20201016.ClsClient;
import com.tencentcloudapi.cls.v20201016.models.DescribeTopicsRequest;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClsDataNodeOperator extends AbstractDataNodeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClsDataNodeOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    protected void setTargetEntity(DataNodeRequest request, DataNodeEntity targetEntity) {
        ClsDataNodeRequest clsDataNodeRequest = (ClsDataNodeRequest) request;
        CommonBeanUtils.copyProperties(clsDataNodeRequest, targetEntity, true);
        try {
            ClsDataNodeDTO dto = ClsDataNodeDTO.getFromRequest(clsDataNodeRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("Failed to build extParams for Cloud log service node: %s", e.getMessage()));
        }
    }

    @Override
    public Boolean accept(String dataNodeType) {
        return DataNodeType.CLS.equals(dataNodeType);
    }

    @Override
    public String getDataNodeType() {
        return DataNodeType.CLS;
    }

    @Override
    public DataNodeInfo getFromEntity(DataNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_NOT_FOUND);
        }
        ClsDataNodeInfo info = new ClsDataNodeInfo();
        CommonBeanUtils.copyProperties(entity, info);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            ClsDataNodeDTO dto = ClsDataNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, info);
        }
        return info;
    }

    @Override
    public Boolean testConnection(DataNodeRequest request) {
        ClsDataNodeRequest dataNodeRequest = (ClsDataNodeRequest) request;
        Credential cred = new Credential(dataNodeRequest.getManageSecretId(), dataNodeRequest.getManageSecretId());
        HttpProfile httpProfile = new HttpProfile();
        httpProfile.setEndpoint(dataNodeRequest.getEndpoint());
        ClientProfile clientProfile = new ClientProfile();
        clientProfile.setHttpProfile(httpProfile);
        ClsClient client = new ClsClient(cred, dataNodeRequest.getRegion(), clientProfile);
        DescribeTopicsRequest req = new DescribeTopicsRequest();
        try {
            client.DescribeTopics(req);
        } catch (Exception e) {
            String errMsg = String.format("connect tencent cloud error endPoint = %s secretId = %s secretKey = %s",
                    dataNodeRequest.getEndpoint(), dataNodeRequest.getManageSecretId(),
                    dataNodeRequest.getManageSecretKey());
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
        return true;
    }

    @Override
    public NodeConfig getNodeConfig(DataNodeEntity dataNodeEntity) {
        DataNodeInfo dataNodeInfo = this.getFromEntity(dataNodeEntity);
        ClsNodeConfig clsNodeConfig = CommonBeanUtils.copyProperties(dataNodeInfo, ClsNodeConfig::new);
        ClsDataNodeDTO dto = ClsDataNodeDTO.getFromJson(dataNodeEntity.getExtParams());
        CommonBeanUtils.copyProperties(dto, clsNodeConfig);
        clsNodeConfig.setNodeName(dataNodeInfo.getName());
        return clsNodeConfig;
    }
}
