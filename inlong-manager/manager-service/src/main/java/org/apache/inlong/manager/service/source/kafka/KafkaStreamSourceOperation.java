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

package org.apache.inlong.manager.service.source.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceDTO;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceRequest;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.service.source.StreamSourceOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.function.Supplier;

/**
 * kafka stream source operation.
 */
@Service
public class KafkaStreamSourceOperation implements StreamSourceOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamSourceOperation.class);
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private StreamSourceEntityMapper sourceMapper;

    @Override
    public Boolean accept(SourceType sourceType) {
        return SourceType.KAFKA == sourceType;
    }

    @Override
    public Integer saveOpt(SourceRequest request, String operator) {
        String sourceType = request.getSourceType();
        Preconditions.checkTrue(Constant.SOURCE_KAFKA.equals(sourceType),
                ErrorCodeEnum.SOURCE_TYPE_NOT_SUPPORT.getMessage() + ": " + sourceType);
        KafkaSourceRequest sourceRequest = (KafkaSourceRequest) request;
        StreamSourceEntity entity = CommonBeanUtils.copyProperties(sourceRequest, StreamSourceEntity::new);
        entity.setStatus(EntityStatus.SOURCE_NEW.getCode());
        entity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
        entity.setCreator(operator);
        entity.setModifier(operator);
        Date now = new Date();
        entity.setCreateTime(now);
        entity.setModifyTime(now);
        // get the ext params
        KafkaSourceDTO dto = KafkaSourceDTO.getFromRequest(sourceRequest);
        try {
            entity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_SAVE_FAILED);
        }
        sourceMapper.insert(entity);
        Integer sourceId = entity.getId();
        request.setId(sourceId);
        return sourceId;
    }

    @Override
    public SourceResponse getById(@NotNull String sourceType, @NotNull Integer id) {
        StreamSourceEntity entity = sourceMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());
        String existType = entity.getSourceType();
        Preconditions.checkTrue(Constant.SOURCE_KAFKA.equals(existType),
                String.format(Constant.SOURCE_TYPE_NOT_SAME, Constant.SOURCE_KAFKA, existType));

        return this.getFromEntity(entity, BinlogSourceResponse::new);
    }

    @Override
    public <T> T getFromEntity(StreamSourceEntity entity, Supplier<T> target) {
        T result = target.get();
        if (entity == null) {
            return result;
        }
        String existType = entity.getSourceType();
        Preconditions.checkTrue(Constant.SOURCE_KAFKA.equals(existType),
                String.format(Constant.SOURCE_TYPE_NOT_SAME, Constant.SOURCE_KAFKA, existType));
        KafkaSourceDTO dto = KafkaSourceDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, result, true);
        CommonBeanUtils.copyProperties(dto, result, true);
        return result;
    }

    @Override
    public PageInfo<? extends SourceListResponse> getPageInfo(Page<StreamSourceEntity> entityPage) {
        if (CollectionUtils.isEmpty(entityPage)) {
            return new PageInfo<>();
        }
        return entityPage.toPageInfo(entity -> this.getFromEntity(entity, KafkaSourceListResponse::new));
    }

    @Override
    public void updateOpt(SourceRequest request, String operator) {
        String sourceType = request.getSourceType();
        Preconditions.checkTrue(Constant.SOURCE_KAFKA.equals(sourceType),
                String.format(Constant.SOURCE_TYPE_NOT_SAME, Constant.SOURCE_KAFKA, sourceType));
        StreamSourceEntity entity = sourceMapper.selectByPrimaryKey(request.getId());
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());
        KafkaSourceRequest sourceRequest = (KafkaSourceRequest) request;
        CommonBeanUtils.copyProperties(sourceRequest, entity, true);
        try {
            KafkaSourceDTO dto = KafkaSourceDTO.getFromRequest(sourceRequest);
            entity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage());
        }
        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(EntityStatus.GROUP_CONFIG_ING.getCode());
        entity.setModifier(operator);
        entity.setModifyTime(new Date());
        sourceMapper.updateByPrimaryKeySelective(entity);
        LOGGER.info("success to update source of type={}", sourceType);
    }
}
