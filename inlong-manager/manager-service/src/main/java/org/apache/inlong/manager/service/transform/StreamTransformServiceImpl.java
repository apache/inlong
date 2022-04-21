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

package org.apache.inlong.manager.service.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.transform.TransformRequest;
import org.apache.inlong.manager.common.pojo.transform.TransformResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StreamTransformEntity;
import org.apache.inlong.manager.dao.mapper.StreamTransformEntityMapper;
import org.apache.inlong.manager.service.CommonOperateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of transform service interface
 */
@Service
@Slf4j
public class StreamTransformServiceImpl implements StreamTransformService {

    @Autowired
    protected StreamTransformEntityMapper transformEntityMapper;
    @Autowired
    protected CommonOperateService commonOperateService;

    @Override
    @Transactional(rollbackFor = Throwable.class, propagation = Propagation.REQUIRES_NEW)
    public Integer save(TransformRequest transformRequest, String operator) {
        log.info("begin to save transform info: {}", transformRequest);
        this.checkParams(transformRequest);

        // Check whether the transform can be added
        final String groupId = transformRequest.getInlongGroupId();
        final String streamId = transformRequest.getInlongStreamId();
        final String transformName = transformRequest.getTransformName();
        commonOperateService.checkGroupStatus(groupId, operator);

        List<StreamTransformEntity> transformEntities = transformEntityMapper.selectByRelatedId(groupId,
                streamId, transformName);
        if (CollectionUtils.isNotEmpty(transformEntities)) {
            String err = "stream transform already exists with groupId=%s, streamId=%s, transformName=%s";
            throw new BusinessException(String.format(err, groupId, streamId, transformName));
        }
        StreamTransformEntity transformEntity = CommonBeanUtils.copyProperties(transformRequest,
                StreamTransformEntity::new);
        transformEntity.setCreator(operator);
        transformEntity.setModifier(operator);
        Date now = new Date();
        transformEntity.setCreateTime(now);
        transformEntity.setModifyTime(now);
        transformEntityMapper.insertSelective(transformEntity);
        return transformEntity.getId();
    }

    @Override
    public List<TransformResponse> listTransform(String groupId, String streamId) {
        log.info("begin to fetch transform info by groupId={} and streamId={} ", groupId, streamId);
        Preconditions.checkNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
        List<StreamTransformEntity> transformEntities = transformEntityMapper.selectByRelatedId(groupId, streamId,
                null);
        if (CollectionUtils.isEmpty(transformEntities)) {
            return Collections.emptyList();
        }
        return transformEntities.stream()
                .map(entity -> CommonBeanUtils.copyProperties(entity, TransformResponse::new))
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, propagation = Propagation.REQUIRES_NEW)
    public boolean update(TransformRequest transformRequest, String operator) {
        log.info("begin to update transform info: {}", transformRequest);
        this.checkParams(transformRequest);
        // Check whether the transform can be modified
        String groupId = transformRequest.getInlongGroupId();
        commonOperateService.checkGroupStatus(groupId, operator);
        Preconditions.checkNotNull(transformRequest.getId(), ErrorCodeEnum.ID_IS_EMPTY.getMessage());
        StreamTransformEntity transformEntity = CommonBeanUtils.copyProperties(transformRequest,
                StreamTransformEntity::new);
        transformEntity.setModifier(operator);
        transformEntity.setVersion(transformEntity.getVersion() + 1);
        Date now = new Date();
        transformEntity.setModifyTime(now);
        return transformEntityMapper.updateByIdSelective(transformEntity) == transformEntity.getId();
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, propagation = Propagation.REQUIRES_NEW)
    public boolean delete(String groupId, String streamId, String transformName, String operator) {
        log.info("begin to delete source by groupId={} streamId={}, transformName={}", groupId, streamId,
                transformName);
        Preconditions.checkNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
        Preconditions.checkNotNull(streamId, ErrorCodeEnum.STREAM_ID_IS_EMPTY.getMessage());
        commonOperateService.checkGroupStatus(groupId, operator);
        Date now = new Date();
        List<StreamTransformEntity> entityList = transformEntityMapper.selectByRelatedId(groupId, streamId,
                transformName);
        if (CollectionUtils.isNotEmpty(entityList)) {
            for (StreamTransformEntity entity : entityList) {
                Integer id = entity.getId();
                entity.setVersion(entity.getVersion() + 1);
                entity.setIsDeleted(id);
                entity.setModifier(operator);
                entity.setModifyTime(now);
                transformEntityMapper.updateByIdSelective(entity);
            }
        }
        log.info("success to logic delete transform by groupId={}, streamId={}, transformName={}", groupId, streamId,
                transformName);
        return true;
    }

    private void checkParams(TransformRequest request) {
        Preconditions.checkNotNull(request, ErrorCodeEnum.REQUEST_IS_EMPTY.getMessage());
        String groupId = request.getInlongGroupId();
        Preconditions.checkNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
        String streamId = request.getInlongStreamId();
        Preconditions.checkNotNull(streamId, ErrorCodeEnum.STREAM_ID_IS_EMPTY.getMessage());
        String transformType = request.getTransformType();
        Preconditions.checkNotNull(transformType, ErrorCodeEnum.TRANSFORM_TYPE_IS_NULL.getMessage());
        String transformName = request.getTransformName();
        Preconditions.checkNotNull(transformName, ErrorCodeEnum.TRANSFORM_NAME_IS_NULL.getMessage());
    }
}
