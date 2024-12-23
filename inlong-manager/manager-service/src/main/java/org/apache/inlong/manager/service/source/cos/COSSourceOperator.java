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

package org.apache.inlong.manager.service.source.cos;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.pojo.node.cos.COSDataNodeInfo;
import org.apache.inlong.manager.pojo.source.DataAddTaskDTO;
import org.apache.inlong.manager.pojo.source.DataAddTaskRequest;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.cos.COSDataAddTaskRequest;
import org.apache.inlong.manager.pojo.source.cos.COSSource;
import org.apache.inlong.manager.pojo.source.cos.COSSourceDTO;
import org.apache.inlong.manager.pojo.source.cos.COSSourceRequest;
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
 * COS source operator, such as get or set COS source info.
 */
@Service
public class COSSourceOperator extends AbstractSourceOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(COSSourceOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private StreamSourceEntityMapper sourceMapper;

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.COS.equals(sourceType);
    }

    @Override
    protected String getSourceType() {
        return SourceType.COS;
    }

    @Override
    protected void setTargetEntity(SourceRequest request, StreamSourceEntity targetEntity) {
        COSSourceRequest sourceRequest = (COSSourceRequest) request;
        try {
            CommonBeanUtils.copyProperties(sourceRequest, targetEntity, true);
            COSSourceDTO dto = COSSourceDTO.getFromRequest(sourceRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("serialize extParams of COS SourceDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSource getFromEntity(StreamSourceEntity entity) {
        COSSource source = new COSSource();
        if (entity == null) {
            return source;
        }

        COSSourceDTO dto = COSSourceDTO.getFromJson(entity.getExtParams());
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
        COSSourceDTO cosSourceDTO = COSSourceDTO.getFromJson(sourceEntity.getExtParams());
        if (Objects.nonNull(cosSourceDTO) && StringUtils.isNotBlank(sourceEntity.getDataNodeName())) {
            COSDataNodeInfo dataNodeInfo =
                    (COSDataNodeInfo) dataNodeService.getByKeyWithoutTenant(sourceEntity.getDataNodeName(),
                            DataNodeType.COS);
            CommonBeanUtils.copyProperties(dataNodeInfo, cosSourceDTO, true);
            return JsonUtils.toJsonString(cosSourceDTO);
        }
        return sourceEntity.getExtParams();
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public Integer addDataAddTask(DataAddTaskRequest request, String operator) {
        try {
            COSDataAddTaskRequest sourceRequest = (COSDataAddTaskRequest) request;
            StreamSourceEntity sourceEntity = sourceMapper.selectById(request.getSourceId());
            COSSourceDTO dto = COSSourceDTO.getFromJson(sourceEntity.getExtParams());
            dto.setDataTimeFrom(sourceRequest.getDataTimeFrom());
            dto.setDataTimeTo(sourceRequest.getDataTimeTo());
            dto.setRetry(true);
            if (request.getIncreaseAuditVersion()) {
                dto.setAuditVersion(request.getAuditVersion());
            }
            dto.setFilterStreams(sourceRequest.getFilterStreams());
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
            LOGGER.error("serialize extParams of COS SourceDTO failure: ", e);
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("serialize extParams of COS SourceDTO failure: %s", e.getMessage()));
        }
    }

}
