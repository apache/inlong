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

package org.apache.inlong.manager.service.node.doris;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.doris.DorisDataNodeDTO;
import org.apache.inlong.manager.pojo.node.doris.DorisDataNodeInfo;
import org.apache.inlong.manager.pojo.node.doris.DorisDataNodeRequest;
import org.apache.inlong.manager.pojo.node.mysql.MySQLDataNodeDTO;
import org.apache.inlong.manager.service.node.AbstractDataNodeOperator;
import org.apache.inlong.manager.service.resource.sink.mysql.MySQLJdbcUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;

@Service
public class DorisDataNodeOperator extends AbstractDataNodeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DorisDataNodeOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String dataNodeType) {
        return getDataNodeType().equals(dataNodeType);
    }

    @Override
    public String getDataNodeType() {
        return DataNodeType.DORIS;
    }

    @Override
    public DataNodeInfo getFromEntity(DataNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_NOT_FOUND);
        }
        DorisDataNodeInfo dorisDataNodeInfo = new DorisDataNodeInfo();
        CommonBeanUtils.copyProperties(entity, dorisDataNodeInfo);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            DorisDataNodeDTO dto = DorisDataNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, dorisDataNodeInfo);
        }
        return dorisDataNodeInfo;
    }

    @Override
    protected void setTargetEntity(DataNodeRequest request, DataNodeEntity targetEntity) {
        DorisDataNodeRequest nodeRequest = (DorisDataNodeRequest) request;
        CommonBeanUtils.copyProperties(nodeRequest, targetEntity, true);
        try {
            DorisDataNodeDTO dto = DorisDataNodeDTO.getFromRequest(nodeRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("Failed to build extParams for Doris node: %s", e.getMessage()));
        }
    }

    @Override
    public Boolean testConnection(DataNodeRequest request) {
        String jdbcUrl = MySQLDataNodeDTO.convertToJdbcurl(request.getUrl());
        String username = request.getUsername();
        String password = request.getToken();
        Preconditions.expectNotBlank(jdbcUrl, ErrorCodeEnum.INVALID_PARAMETER, "connection jdbcUrl cannot be empty");
        try (Connection ignored = MySQLJdbcUtils.getConnection(jdbcUrl, username, password)) {
            LOGGER.info("doris connection not null - connection success for jdbcUrl={}, username={}, password={}",
                    jdbcUrl, username, password);
            return true;
        } catch (Exception e) {
            String errMsg = String.format("doris connection failed for jdbcUrl=%s, username=%s, password=%s", jdbcUrl,
                    username, password);
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }
}
