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

package org.apache.inlong.manager.service.source.sql;

import org.apache.inlong.common.pojo.agent.DataConfig;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.pojo.node.sql.SqlDataNodeInfo;
import org.apache.inlong.manager.pojo.source.DataAddTaskDTO;
import org.apache.inlong.manager.pojo.source.DataAddTaskRequest;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.sql.SqlDataAddTaskRequest;
import org.apache.inlong.manager.pojo.source.sql.SqlSource;
import org.apache.inlong.manager.pojo.source.sql.SqlSourceDTO;
import org.apache.inlong.manager.pojo.source.sql.SqlSourceRequest;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.service.source.AbstractSourceOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Sql source operator, such as get or set Sql source info.
 */
@Service
public class SqlSourceOperator extends AbstractSourceOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlSourceOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private StreamSourceEntityMapper sourceMapper;
    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.SQL.equals(sourceType);
    }

    @Override
    protected String getSourceType() {
        return SourceType.SQL;
    }

    @Override
    protected void setTargetEntity(SourceRequest request, StreamSourceEntity targetEntity) {
        SqlSourceRequest sourceRequest = (SqlSourceRequest) request;
        try {
            CommonBeanUtils.copyProperties(sourceRequest, targetEntity, true);
            SqlSourceDTO dto = SqlSourceDTO.getFromRequest(sourceRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("serialize extParams of Sql SourceDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSource getFromEntity(StreamSourceEntity entity) {
        SqlSource source = new SqlSource();
        if (entity == null) {
            return source;
        }

        SqlSourceDTO dto = SqlSourceDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, source, true);
        CommonBeanUtils.copyProperties(dto, source, true);

        List<StreamField> sourceFields = super.getSourceFields(entity.getId());
        source.setFieldList(sourceFields);

        List<StreamSourceEntity> dataAddTaskList = sourceMapper.selectByTaskMapId(entity.getId());
        source.setDataAddTaskList(dataAddTaskList.stream().map(subEntity -> DataAddTaskDTO.builder()
                .id(subEntity.getId())
                .taskMapId(entity.getId())
                .agentIp(subEntity.getAgentIp())
                .status(subEntity.getStatus()).build())
                .collect(Collectors.toList()));
        return source;
    }

    @Override
    public String getExtParams(StreamSourceEntity sourceEntity) {
        SqlSourceDTO sqlSourceDTO = SqlSourceDTO.getFromJson(sourceEntity.getExtParams());
        if (Objects.nonNull(sqlSourceDTO) && StringUtils.isNotBlank(sourceEntity.getDataNodeName())) {
            SqlDataNodeInfo dataNodeInfo =
                    (SqlDataNodeInfo) dataNodeService.getByKeyWithoutTenant(sourceEntity.getDataNodeName(),
                            DataNodeType.SQL);
            sqlSourceDTO.setJdbcUrl(dataNodeInfo.getUrl());
            sqlSourceDTO.setUsername(dataNodeInfo.getUsername());
            sqlSourceDTO.setJdbcPassword(dataNodeInfo.getToken());
            return JsonUtils.toJsonString(sqlSourceDTO);
        }
        return sourceEntity.getExtParams();
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public Integer addDataAddTask(DataAddTaskRequest request, String operator) {
        try {
            SqlDataAddTaskRequest sourceRequest = (SqlDataAddTaskRequest) request;
            StreamSourceEntity sourceEntity = sourceMapper.selectById(request.getSourceId());
            SqlSourceDTO dto = SqlSourceDTO.getFromJson(sourceEntity.getExtParams());
            dto.setDataTimeFrom(sourceRequest.getDataTimeFrom());
            dto.setDataTimeTo(sourceRequest.getDataTimeTo());
            dto.setRetry(true);
            if (request.getIncreaseAuditVersion()) {
                dto.setAuditVersion(request.getAuditVersion());
            }
            StreamSourceEntity dataAddTaskEntity =
                    CommonBeanUtils.copyProperties(sourceEntity, StreamSourceEntity::new);
            dataAddTaskEntity.setId(null);
            dataAddTaskEntity.setSourceName(
                    sourceEntity.getSourceName() + "-" + request.getAuditVersion() + "-" + sourceEntity.getId());
            dataAddTaskEntity.setExtParams(objectMapper.writeValueAsString(dto));
            dataAddTaskEntity.setTaskMapId(sourceEntity.getId());
            Integer id = sourceMapper.insert(dataAddTaskEntity);
            SourceRequest dataAddTaskRequest =
                    CommonBeanUtils.copyProperties(dataAddTaskEntity, SourceRequest::new, true);
            updateAgentTaskConfig(dataAddTaskRequest, operator);
            return id;
        } catch (Exception e) {
            LOGGER.error("serialize extParams of Sql SourceDTO failure: ", e);
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("serialize extParams of Sql SourceDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public String updateDataConfig(String extParams, InlongStreamEntity streamEntity, DataConfig dataConfig) {
        String dataSeparator = String.valueOf((char) Integer.parseInt(streamEntity.getDataSeparator()));
        SqlSourceDTO sqlSourceDTO = JsonUtils.parseObject(extParams, SqlSourceDTO.class);
        if (Objects.nonNull(sqlSourceDTO)) {
            sqlSourceDTO.setDataSeparator(dataSeparator);
            dataConfig.setAuditVersion(sqlSourceDTO.getAuditVersion());
            extParams = JsonUtils.toJsonString(sqlSourceDTO);
        }
        return extParams;
    }
}
