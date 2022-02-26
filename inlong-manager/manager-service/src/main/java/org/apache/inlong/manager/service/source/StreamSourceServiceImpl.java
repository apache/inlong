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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.pojo.agent.TaskSnapshotRequest;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SourceState;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourcePageRequest;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.service.CommonOperateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
    @Autowired
    private SourceSnapshotOperation heartbeatOperation;

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Integer save(SourceRequest request, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to save source info=" + request);
        }
        this.checkParams(request);

        // Check if it can be added
        String groupId = request.getInlongGroupId();
        commonOperateService.checkGroupStatus(groupId, operator);

        // Make sure that there is no source info with the current groupId and streamId
        String streamId = request.getInlongStreamId();
        String sourceType = request.getSourceType();
        List<StreamSourceEntity> sourceExist = sourceMapper.selectByIdAndType(groupId, streamId, sourceType);
        Preconditions.checkEmpty(sourceExist, ErrorCodeEnum.SOURCE_ALREADY_EXISTS.getMessage());

        // According to the source type, save source information
        StreamSourceOperation operation = operationFactory.getInstance(SourceType.getType(sourceType));
        int id = operation.saveOpt(request, operator);

        LOGGER.info("success to save source info");
        return id;
    }

    @Override
    public SourceResponse get(Integer id, String sourceType) {
        LOGGER.debug("begin to get source by id={}, sourceType={}", id, sourceType);
        StreamSourceOperation operation = operationFactory.getInstance(SourceType.getType(sourceType));
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

        List<StreamSourceEntity> entityList = sourceMapper.selectByIdentifier(groupId, streamId);
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
        String sourceType = request.getSourceType();
        Preconditions.checkNotNull(sourceType, Constant.SOURCE_TYPE_IS_EMPTY);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<StreamSourceEntity> entityPage = (Page<StreamSourceEntity>) sourceMapper.selectByCondition(request);

        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        StreamSourceOperation operation = operationFactory.getInstance(SourceType.getType(sourceType));
        PageInfo<? extends SourceListResponse> pageInfo = operation.getPageInfo(entityPage);
        pageInfo.setTotal(entityPage.getTotal());

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
        StreamSourceOperation operation = operationFactory.getInstance(SourceType.getType(sourceType));
        operation.updateOpt(request, operator);

        LOGGER.info("success to update source info");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean delete(Integer id, String sourceType, String operator) {
        LOGGER.info("begin to delete source by id={}, sourceType={}", id, sourceType);
        Preconditions.checkNotNull(id, Constant.ID_IS_EMPTY);
        // Preconditions.checkNotNull(sourceType, Constant.SOURCE_TYPE_IS_EMPTY);

        StreamSourceEntity entity = sourceMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());
        commonOperateService.checkGroupStatus(entity.getInlongGroupId(), operator);

        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(SourceState.SOURCE_DEL.getCode());
        entity.setIsDeleted(id);
        entity.setModifier(operator);
        entity.setModifyTime(new Date());
        sourceMapper.updateByPrimaryKeySelective(entity);

        LOGGER.info("success to delete source info");
        return true;
    }

    @Override
    public void updateStatus(int id, int status, String log) {
        StreamSourceEntity entity = new StreamSourceEntity();
        entity.setId(id);
        entity.setStatus(status);
        sourceMapper.updateStatus(entity);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public boolean logicDeleteAll(String groupId, String streamId, String operator) {
        LOGGER.info("begin to logic delete all source info by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, Constant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        commonOperateService.checkGroupStatus(groupId, operator);

        Date now = new Date();
        List<StreamSourceEntity> entityList = sourceMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isNotEmpty(entityList)) {
            entityList.forEach(entity -> {
                Integer id = entity.getId();
                entity.setPreviousStatus(entity.getStatus());
                entity.setStatus(SourceState.SOURCE_DEL.getCode());
                entity.setIsDeleted(id);
                entity.setModifier(operator);
                entity.setModifyTime(now);

                sourceMapper.deleteByPrimaryKey(id);
            });
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

        List<StreamSourceEntity> entityList = sourceMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isNotEmpty(entityList)) {
            entityList.forEach(entity -> {
                sourceMapper.deleteByPrimaryKey(entity.getId());
            });
        }

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

    @Override
    public Boolean reportSnapshot(TaskSnapshotRequest request) {
        return heartbeatOperation.putData(request);
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
