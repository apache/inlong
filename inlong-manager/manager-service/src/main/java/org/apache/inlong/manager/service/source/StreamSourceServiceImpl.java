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

package org.apache.inlong.manager.service.source;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.enums.SourceState;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourcePageRequest;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.service.CommonOperateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Implementation of source service interface
 */
@Service
public class StreamSourceServiceImpl implements StreamSourceService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamSourceServiceImpl.class);

    @Autowired
    private SourceOperationFactory operationFactory;
    @Autowired
    private StreamSourceEntityMapper sourceMapper;
    @Autowired
    private CommonOperateService commonOperateService;

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Integer save(SourceRequest request, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to save source info=" + request);
        }
        this.checkParams(request);

        // Check if it can be added
        String groupId = request.getInlongGroupId();
        InlongGroupEntity groupEntity = commonOperateService.checkGroupStatus(groupId, operator);

        // According to the source type, save source information
        String sourceType = request.getSourceType();
        StreamSourceOperation operation = operationFactory.getInstance(SourceType.forType(sourceType));
        int id = operation.saveOpt(request, groupEntity.getStatus(), operator);

        LOGGER.info("success to save source info");
        return id;
    }

    @Override
    public SourceResponse get(Integer id, String sourceType) {
        LOGGER.debug("begin to get source by id={}, sourceType={}", id, sourceType);
        StreamSourceOperation operation = operationFactory.getInstance(SourceType.forType(sourceType));
        SourceResponse sourceResponse = operation.getById(id);
        LOGGER.debug("success to get source info");
        return sourceResponse;
    }

    @Override
    public Integer getCount(String groupId, String streamId) {
        Integer count = sourceMapper.selectCount(groupId, streamId);
        LOGGER.debug("source count={} with groupId={}, streamId={}", count, groupId, streamId);
        return count;
    }

    @Override
    public List<SourceResponse> listSource(String groupId, String streamId) {
        LOGGER.debug("begin to list source by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);

        List<StreamSourceEntity> entityList = sourceMapper.selectByRelatedId(groupId, streamId);
        if (CollectionUtils.isEmpty(entityList)) {
            return Collections.emptyList();
        }
        List<SourceResponse> responseList = new ArrayList<>();
        entityList.forEach(entity -> responseList.add(this.get(entity.getId(), entity.getSourceType())));

        LOGGER.info("success to list source");
        return responseList;
    }

    @Override
    public PageInfo<? extends SourceListResponse> listByCondition(SourcePageRequest request) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to list source page by " + request);
        }
        Preconditions.checkNotNull(request.getInlongGroupId(), Constant.GROUP_ID_IS_EMPTY);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        List<StreamSourceEntity> entityPage = sourceMapper.selectByCondition(request);

        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        Map<SourceType, Page<StreamSourceEntity>> sourceMap = Maps.newHashMap();
        for (StreamSourceEntity streamSource : entityPage) {
            SourceType sourceType = SourceType.forType(streamSource.getSourceType());
            sourceMap.computeIfAbsent(sourceType, k -> new Page<>()).add(streamSource);
        }
        List<SourceListResponse> sourceListResponses = Lists.newArrayList();
        for (Map.Entry<SourceType, Page<StreamSourceEntity>> entry : sourceMap.entrySet()) {
            SourceType sourceType = entry.getKey();
            StreamSourceOperation operation = operationFactory.getInstance(sourceType);
            PageInfo<? extends SourceListResponse> pageInfo = operation.getPageInfo(entry.getValue());
            sourceListResponses.addAll(pageInfo.getList());
        }
        PageInfo<? extends SourceListResponse> pageInfo = PageInfo.of(sourceListResponses);
        LOGGER.debug("success to list source page");
        return pageInfo;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public boolean update(SourceRequest request, String operator) {
        LOGGER.debug("begin to update source info=" + request);
        this.checkParams(request);
        Preconditions.checkNotNull(request.getId(), Constant.ID_IS_EMPTY);

        // Check if it can be modified
        String groupId = request.getInlongGroupId();
        commonOperateService.checkGroupStatus(groupId, operator);

        String sourceType = request.getSourceType();
        StreamSourceOperation operation = operationFactory.getInstance(SourceType.forType(sourceType));
        operation.updateOpt(request, operator);

        LOGGER.info("success to update source info");
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public boolean updateStatus(Integer id, Integer targetStatus, String operator) {
        sourceMapper.updateStatus(id, targetStatus);
        LOGGER.info("success to update source status={} for id={} by {}", targetStatus, id, operator);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public boolean updateStatus(String groupId, String streamId, Integer targetStatus, String operator) {
        sourceMapper.updateStatusByRelatedId(groupId, streamId, targetStatus);
        LOGGER.info("success to update source status={} for groupId={}, streamId={} by {}",
                targetStatus, groupId, streamId, operator);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public boolean delete(Integer id, String sourceType, String operator) {
        LOGGER.info("begin to delete source by id={}, sourceType={}", id, sourceType);
        Preconditions.checkNotNull(id, Constant.ID_IS_EMPTY);

        StreamSourceEntity entity = sourceMapper.selectByIdForUpdate(id);
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());
        commonOperateService.checkGroupStatus(entity.getInlongGroupId(), operator);

        StreamSourceOperation operation = operationFactory.getInstance(SourceType.forType(sourceType));
        SourceRequest sourceRequest = new SourceRequest();
        CommonBeanUtils.copyProperties(entity, sourceRequest, true);
        operation.deleteOpt(sourceRequest, operator);

        LOGGER.info("success to delete source info:{}", entity);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public boolean restart(Integer id, String sourceType, String operator) {
        LOGGER.info("begin to restart source by id={}, sourceType={}", id, sourceType);
        StreamSourceEntity entity = sourceMapper.selectByIdForUpdate(id);
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());
        commonOperateService.checkGroupStatus(entity.getInlongGroupId(), operator);

        StreamSourceOperation operation = operationFactory.getInstance(SourceType.forType(sourceType));
        SourceRequest sourceRequest = new SourceRequest();
        CommonBeanUtils.copyProperties(entity, sourceRequest, true);
        operation.restartOpt(sourceRequest, operator);

        LOGGER.info("success to restart source info:{}", entity);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public boolean stop(Integer id, String sourceType, String operator) {
        LOGGER.info("begin to stop source by id={}, sourceType={}", id, sourceType);
        StreamSourceEntity entity = sourceMapper.selectByIdForUpdate(id);
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());
        commonOperateService.checkGroupStatus(entity.getInlongGroupId(), operator);

        StreamSourceOperation operation = operationFactory.getInstance(SourceType.forType(sourceType));
        SourceRequest sourceRequest = new SourceRequest();
        CommonBeanUtils.copyProperties(entity, sourceRequest, true);
        operation.stopOpt(sourceRequest, operator);

        LOGGER.info("success to stop source info:{}", entity);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public boolean logicDeleteAll(String groupId, String streamId, String operator) {
        LOGGER.info("begin to logic delete all source info by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, Constant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        InlongGroupEntity groupEntity = commonOperateService.checkGroupStatus(groupId, operator);
        Integer nextStatus;
        if (GroupState.CONFIG_SUCCESSFUL.getCode().equals(groupEntity.getStatus())) {
            nextStatus = SourceState.TO_BE_ISSUED_DELETE.getCode();
        } else {
            nextStatus = SourceState.SOURCE_DISABLE.getCode();
        }
        Date now = new Date();
        List<StreamSourceEntity> entityList = sourceMapper.selectByRelatedId(groupId, streamId);
        if (CollectionUtils.isNotEmpty(entityList)) {
            for (StreamSourceEntity entity : entityList) {
                Integer id = entity.getId();
                entity.setPreviousStatus(entity.getStatus());
                entity.setStatus(nextStatus);
                entity.setIsDeleted(id);
                entity.setModifier(operator);
                entity.setModifyTime(now);

                sourceMapper.updateByPrimaryKeySelective(entity);
            }
        }

        LOGGER.info("success to logic delete all source by groupId={}, streamId={}", groupId, streamId);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public boolean deleteAll(String groupId, String streamId, String operator) {
        LOGGER.info("begin to delete all source by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, Constant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        commonOperateService.checkGroupStatus(groupId, operator);
        sourceMapper.deleteByRelatedId(groupId, streamId);
        LOGGER.info("success to delete all source by groupId={}, streamId={}", groupId, streamId);
        return true;
    }

    @Override
    public List<String> getSourceTypeList(String groupId, String streamId) {
        LOGGER.debug("begin to get source type list by groupId={}, streamId={}", groupId, streamId);
        if (StringUtils.isEmpty(streamId)) {
            return Collections.emptyList();
        }

        List<String> resultList = sourceMapper.selectSourceType(groupId, streamId);
        LOGGER.debug("success to get source type list, result sourceType={}", resultList);
        return resultList;
    }

    private void checkParams(SourceRequest request) {
        Preconditions.checkNotNull(request, Constant.REQUEST_IS_EMPTY);
        String groupId = request.getInlongGroupId();
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        String streamId = request.getInlongStreamId();
        Preconditions.checkNotNull(streamId, Constant.STREAM_ID_IS_EMPTY);
        String sourceType = request.getSourceType();
        Preconditions.checkNotNull(sourceType, Constant.SOURCE_TYPE_IS_EMPTY);
    }

}
