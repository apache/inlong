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

package org.apache.inlong.manager.service.source.file;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.pojo.source.DataAddTaskDTO;
import org.apache.inlong.manager.pojo.source.DataAddTaskRequest;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.file.FileDataAddTaskRequest;
import org.apache.inlong.manager.pojo.source.file.FileSource;
import org.apache.inlong.manager.pojo.source.file.FileSourceDTO;
import org.apache.inlong.manager.pojo.source.file.FileSourceRequest;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.service.source.AbstractSourceOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

/**
 * File source operator, such as get or set file source info.
 */
@Service
public class FileSourceOperator extends AbstractSourceOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSourceOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private StreamSourceEntityMapper sourceMapper;

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.FILE.equals(sourceType);
    }

    @Override
    protected String getSourceType() {
        return SourceType.FILE;
    }

    @Override
    protected void setTargetEntity(SourceRequest request, StreamSourceEntity targetEntity) {
        FileSourceRequest sourceRequest = (FileSourceRequest) request;
        try {
            CommonBeanUtils.copyProperties(sourceRequest, targetEntity, true);
            FileSourceDTO dto = FileSourceDTO.getFromRequest(sourceRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("serialize extParams of File SourceDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSource getFromEntity(StreamSourceEntity entity) {
        FileSource source = new FileSource();
        if (entity == null) {
            return source;
        }

        FileSourceDTO dto = FileSourceDTO.getFromJson(entity.getExtParams());
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
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public Integer addDataAddTask(DataAddTaskRequest request, String operator) {
        FileDataAddTaskRequest sourceRequest = (FileDataAddTaskRequest) request;
        StreamSourceEntity sourceEntity = sourceMapper.selectById(request.getSourceId());
        try {
            List<StreamSourceEntity> dataAddTaskList = sourceMapper.selectByTaskMapId(sourceEntity.getId());
            FileSourceDTO dto = FileSourceDTO.getFromJson(sourceEntity.getExtParams());
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
            LOGGER.error("serialize extParams of File SourceDTO failure: ", e);
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("serialize extParams of File SourceDTO failure: %s", e.getMessage()));
        }
    }

}
