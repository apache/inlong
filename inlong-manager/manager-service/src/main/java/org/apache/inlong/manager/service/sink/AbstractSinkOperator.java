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

package org.apache.inlong.manager.service.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.sink.SinkField;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.StreamSink;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Default operation of stream sink.
 */
public abstract class AbstractSinkOperator implements StreamSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSinkOperator.class);

    @Autowired
    protected ObjectMapper objectMapper;
    @Autowired
    protected StreamSinkEntityMapper sinkMapper;
    @Autowired
    protected StreamSinkFieldEntityMapper sinkFieldMapper;

    /**
     * Setting the parameters of the latest entity.
     *
     * @param request sink request
     * @param targetEntity entity object which will set the new parameters.
     */
    protected abstract void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity);

    /**
     * Getting the sink type.
     *
     * @return sink type string.
     */
    protected abstract String getSinkType();

    @Override
    public Integer saveOpt(SinkRequest request, String operator) {
        StreamSinkEntity entity = CommonBeanUtils.copyProperties(request, StreamSinkEntity::new);
        entity.setStatus(SinkStatus.NEW.getCode());
        entity.setIsDeleted(InlongConstants.UN_DELETED);
        entity.setCreator(operator);
        entity.setModifier(operator);
        Date now = new Date();
        entity.setCreateTime(now);
        entity.setModifyTime(now);
        entity.setVersion(1);

        // get the ext params
        setTargetEntity(request, entity);
        sinkMapper.insert(entity);
        Integer sinkId = entity.getId();
        request.setId(sinkId);
        this.saveFieldOpt(request);
        return sinkId;
    }

    @Override
    public List<SinkField> getSinkFields(Integer sinkId) {
        List<StreamSinkFieldEntity> sinkFieldEntities = sinkFieldMapper.selectBySinkId(sinkId);
        return CommonBeanUtils.copyListProperties(sinkFieldEntities, SinkField::new);
    }

    @Override
    public PageInfo<? extends StreamSink> getPageInfo(Page<StreamSinkEntity> entityPage) {
        if (CollectionUtils.isEmpty(entityPage)) {
            return new PageInfo<>();
        }
        return entityPage.toPageInfo(this::getFromEntity);
    }

    @Override
    public void updateOpt(SinkRequest request, String operator) {
        StreamSinkEntity entity = sinkMapper.selectByPrimaryKey(request.getId());
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SINK_INFO_NOT_FOUND.getMessage());
        if (!entity.getVersion().equals(request.getVersion())) {
            LOGGER.warn("sink information has already updated, please reload sink information and update.");
            throw new BusinessException(ErrorCodeEnum.SINK_UPDATE_FAILED);
        }
        CommonBeanUtils.copyProperties(request, entity, true);
        setTargetEntity(request, entity);
        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(SinkStatus.CONFIG_ING.getCode());
        entity.setModifier(operator);
        entity.setModifyTime(new Date());
        sinkMapper.updateByPrimaryKeySelective(entity);

        boolean onlyAdd = SinkStatus.CONFIG_SUCCESSFUL.getCode().equals(entity.getPreviousStatus());
        this.updateFieldOpt(onlyAdd, request);

        LOGGER.info("success to update sink of type={}", request.getSinkType());
    }

    @Override
    public void updateFieldOpt(Boolean onlyAdd, SinkRequest request) {
        Integer sinkId = request.getId();
        List<SinkField> fieldRequestList = request.getSinkFieldList();
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
        LOGGER.info("success to update sink field");
    }

    protected void saveFieldOpt(SinkRequest request) {
        List<SinkField> fieldList = request.getSinkFieldList();
        LOGGER.info("begin to save sink fields={}", fieldList);
        if (CollectionUtils.isEmpty(fieldList)) {
            return;
        }

        int size = fieldList.size();
        List<StreamSinkFieldEntity> entityList = new ArrayList<>(size);
        String groupId = request.getInlongGroupId();
        String streamId = request.getInlongStreamId();
        String sinkType = request.getSinkType();
        Integer sinkId = request.getId();
        for (SinkField fieldInfo : fieldList) {
            this.checkFieldInfo(fieldInfo);
            StreamSinkFieldEntity fieldEntity = CommonBeanUtils.copyProperties(fieldInfo, StreamSinkFieldEntity::new);
            if (StringUtils.isEmpty(fieldEntity.getFieldComment())) {
                fieldEntity.setFieldComment(fieldEntity.getFieldName());
            }
            fieldEntity.setInlongGroupId(groupId);
            fieldEntity.setInlongStreamId(streamId);
            fieldEntity.setSinkType(sinkType);
            fieldEntity.setSinkId(sinkId);
            fieldEntity.setIsDeleted(InlongConstants.UN_DELETED);
            entityList.add(fieldEntity);
        }

        sinkFieldMapper.insertAll(entityList);
        LOGGER.info("success to save sink fields");
    }

    /**
     * Check the validity of sink fields.
     */
    protected void checkFieldInfo(SinkField fieldInfo) {

    }

}
