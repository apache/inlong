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

package org.apache.inlong.manager.service.sink.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkDTO;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.service.sink.StreamSinkOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

/**
 * Kafka sink operation
 */
@Service
public class KafkaStreamSinkOperation implements StreamSinkOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamSinkOperation.class);

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private StreamSinkEntityMapper sinkMapper;
    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;

    @Override
    public Boolean accept(SinkType sinkType) {
        return SinkType.KAFKA.equals(sinkType);
    }

    @Override
    public Integer saveOpt(SinkRequest request, String operator) {
        String sinkType = request.getSinkType();
        Preconditions.checkTrue(Constant.SINK_KAFKA.equals(sinkType),
                ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage() + ": " + sinkType);

        KafkaSinkRequest kafkaSinkRequest = (KafkaSinkRequest) request;
        StreamSinkEntity entity = CommonBeanUtils.copyProperties(kafkaSinkRequest, StreamSinkEntity::new);
        entity.setStatus(EntityStatus.SINK_NEW.getCode());
        entity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
        entity.setCreator(operator);
        entity.setModifier(operator);
        Date now = new Date();
        entity.setCreateTime(now);
        entity.setModifyTime(now);

        // get the ext params
        KafkaSinkDTO dto = KafkaSinkDTO.getFromRequest(kafkaSinkRequest);
        try {
            entity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED);
        }
        sinkMapper.insert(entity);
        Integer sinkId = entity.getId();
        request.setId(sinkId);
        this.saveFieldOpt(request);
        return sinkId;
    }

    @Override
    public void saveFieldOpt(SinkRequest request) {
        List<SinkFieldRequest> fieldList = request.getFieldList();
        LOGGER.info("begin to save field={}", fieldList);
        if (CollectionUtils.isEmpty(fieldList)) {
            return;
        }

        int size = fieldList.size();
        List<StreamSinkFieldEntity> entityList = new ArrayList<>(size);
        String groupId = request.getInlongGroupId();
        String streamId = request.getInlongStreamId();
        String sinkType = request.getSinkType();
        Integer sinkId = request.getId();
        for (SinkFieldRequest fieldInfo : fieldList) {
            StreamSinkFieldEntity fieldEntity = CommonBeanUtils.copyProperties(fieldInfo, StreamSinkFieldEntity::new);
            if (StringUtils.isEmpty(fieldEntity.getFieldComment())) {
                fieldEntity.setFieldComment(fieldEntity.getFieldName());
            }
            fieldEntity.setInlongGroupId(groupId);
            fieldEntity.setInlongStreamId(streamId);
            fieldEntity.setSinkType(sinkType);
            fieldEntity.setSinkId(sinkId);
            fieldEntity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
            entityList.add(fieldEntity);
        }

        sinkFieldMapper.insertAll(entityList);
        LOGGER.info("success to save field");
    }

    @Override
    public SinkResponse getById(@NotNull String sinkType, @NotNull Integer id) {
        StreamSinkEntity entity = sinkMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SINK_INFO_NOT_FOUND.getMessage());
        String existType = entity.getSinkType();
        Preconditions.checkTrue(Constant.SINK_KAFKA.equals(existType),
                String.format(Constant.SINK_TYPE_NOT_SAME, Constant.SINK_KAFKA, existType));
        return this.getFromEntity(entity, KafkaSinkResponse::new);
    }

    @Override
    public <T> T getFromEntity(StreamSinkEntity entity, Supplier<T> target) {
        T result = target.get();
        if (entity == null) {
            return result;
        }
        String existType = entity.getSinkType();
        Preconditions.checkTrue(Constant.SINK_KAFKA.equals(existType),
                String.format(Constant.SINK_TYPE_NOT_SAME, Constant.SINK_KAFKA, existType));

        KafkaSinkDTO dto = KafkaSinkDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, result, true);
        CommonBeanUtils.copyProperties(dto, result, true);

        return result;
    }

    @Override
    public PageInfo<? extends SinkListResponse> getPageInfo(Page<StreamSinkEntity> entityPage) {
        if (CollectionUtils.isEmpty(entityPage)) {
            return new PageInfo<>();
        }
        return entityPage.toPageInfo(entity -> this.getFromEntity(entity, KafkaSinkListResponse::new));
    }

    @Override
    public void updateOpt(SinkRequest request, String operator) {
        String sinkType = request.getSinkType();
        Preconditions.checkTrue(Constant.SINK_KAFKA.equals(sinkType),
                String.format(Constant.SINK_TYPE_NOT_SAME, Constant.SINK_KAFKA, sinkType));

        StreamSinkEntity entity = sinkMapper.selectByPrimaryKey(request.getId());
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SINK_INFO_NOT_FOUND.getMessage());
        KafkaSinkRequest kafkaSinkRequest = (KafkaSinkRequest) request;
        CommonBeanUtils.copyProperties(kafkaSinkRequest, entity, true);
        try {
            KafkaSinkDTO dto = KafkaSinkDTO.getFromRequest(kafkaSinkRequest);
            entity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT.getMessage());
        }

        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(EntityStatus.GROUP_CONFIG_ING.getCode());
        entity.setModifier(operator);
        entity.setModifyTime(new Date());
        sinkMapper.updateByPrimaryKeySelective(entity);

        boolean onlyAdd = EntityStatus.SINK_CONFIG_SUCCESSFUL.getCode().equals(entity.getPreviousStatus());
        this.updateFieldOpt(onlyAdd, kafkaSinkRequest);

        LOGGER.info("success to update sink of type={}", sinkType);
    }

    @Override
    public void updateFieldOpt(Boolean onlyAdd, SinkRequest request) {
        Integer sinkId = request.getId();
        List<SinkFieldRequest> fieldRequestList = request.getFieldList();
        if (CollectionUtils.isEmpty(fieldRequestList)) {
            return;
        }
        if (onlyAdd) {
            List<StreamSinkFieldEntity> existsFieldList = sinkFieldMapper.selectBySinkId(sinkId);
            if (existsFieldList.size() > fieldRequestList.size()) {
                throw new BusinessException(ErrorCodeEnum.SINK_FIELD_UPDATE_NOT_ALLOWED);
            }
            for (int i = 0; i < existsFieldList.size(); i++) {
                if (!existsFieldList.get(i).getFieldName().equals(fieldRequestList.get(i).getFieldName())) {
                    throw new BusinessException(ErrorCodeEnum.SINK_FIELD_UPDATE_NOT_ALLOWED);
                }
            }
        }
        // First physically delete the existing fields
        sinkFieldMapper.deleteAll(sinkId);
        // Then batch save the sink fields
        this.saveFieldOpt(request);
        LOGGER.info("success to update field");
    }

}
