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

package org.apache.inlong.manager.service.core.impl;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.sink.SinkBriefResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceDbDetailInfo;
import org.apache.inlong.manager.common.pojo.source.SourceFileDetailInfo;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamRequest;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamExtInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamListResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamPageRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamTopicResponse;
import org.apache.inlong.manager.common.pojo.stream.StreamBriefResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamExtEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamFieldEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamFieldEntityMapper;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.core.SourceDbService;
import org.apache.inlong.manager.service.core.SourceFileService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Inlong stream service layer implementation
 */
@Service
public class InlongStreamServiceImpl implements InlongStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongStreamServiceImpl.class);

    @Autowired
    private InlongStreamEntityMapper streamMapper;
    @Autowired
    private InlongStreamExtEntityMapper streamExtMapper;
    @Autowired
    private InlongStreamFieldEntityMapper streamFieldMapper;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private SourceFileService sourceFileService;
    @Autowired
    private SourceDbService sourceDbService;
    @Autowired
    private StreamSourceService sourceService;
    @Autowired
    private StreamSinkService sinkService;

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Integer save(InlongStreamRequest request, String operator) {
        LOGGER.debug("begin to save inlong stream info={}", request);
        Preconditions.checkNotNull(request, "inlong stream info is empty");
        String groupId = request.getInlongGroupId();
        String streamId = request.getInlongStreamId();
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, Constant.STREAM_ID_IS_EMPTY);

        // Check if it can be added
        checkBizIsTempStatus(groupId);

        // The streamId under the same groupId cannot be repeated
        Integer count = streamMapper.selectExistByIdentifier(groupId, streamId);
        if (count >= 1) {
            LOGGER.error("inlong stream id [{}] has already exists", streamId);
            throw new BusinessException(ErrorCodeEnum.STREAM_ID_DUPLICATE);
        }
        if (StringUtils.isEmpty(request.getMqResourceObj())) {
            request.setMqResourceObj(streamId);
        }
        // Processing inlong stream
        InlongStreamEntity streamEntity = CommonBeanUtils.copyProperties(request, InlongStreamEntity::new);
        streamEntity.setStatus(EntityStatus.STREAM_NEW.getCode());
        streamEntity.setCreator(operator);
        streamEntity.setCreateTime(new Date());

        streamMapper.insertSelective(streamEntity);
        this.saveField(groupId, streamId, request.getFieldList());

        LOGGER.info("success to save inlong stream info for groupId={}", groupId);
        return streamEntity.getId();
    }

    @Override
    public InlongStreamResponse get(String groupId, String streamId) {
        LOGGER.debug("begin to get inlong stream by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, Constant.STREAM_ID_IS_EMPTY);

        InlongStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        if (streamEntity == null) {
            LOGGER.error("inlong stream not found by groupId={}, streamId={}", groupId, streamId);
            throw new BusinessException(ErrorCodeEnum.STREAM_NOT_FOUND);
        }

        InlongStreamResponse response = CommonBeanUtils.copyProperties(streamEntity, InlongStreamResponse::new);
        List<InlongStreamFieldInfo> streamFields = this.getStreamFields(groupId, streamId);
        response.setFieldList(streamFields);

        LOGGER.info("success to get inlong stream for groupId={}", groupId);
        return response;
    }

    @Override
    public Boolean exist(String groupId, String streamId) {
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        InlongStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        return streamEntity != null;
    }

    /**
     * Query and set the extended information and data source fields of the inlong stream
     */
    private List<InlongStreamFieldInfo> getStreamFields(String groupId, String streamId) {
        List<InlongStreamFieldEntity> fieldEntityList = streamFieldMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isEmpty(fieldEntityList)) {
            return Collections.emptyList();
        }
        return CommonBeanUtils.copyListProperties(fieldEntityList, InlongStreamFieldInfo::new);
    }

    @Override
    public PageInfo<InlongStreamListResponse> listByCondition(InlongStreamPageRequest request) {
        LOGGER.debug("begin to list inlong stream page by {}", request);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongStreamEntity> entityPage = (Page<InlongStreamEntity>) streamMapper.selectByCondition(request);
        List<InlongStreamListResponse> streamList = CommonBeanUtils.copyListProperties(entityPage,
                InlongStreamListResponse::new);

        // Filter out inlong streams that do not have this sink type (only one of each inlong stream can be created)
        String groupId = request.getInlongGroupId();
        String sinkType = request.getSinkType();
        if (StringUtils.isNotEmpty(sinkType)) {
            List<String> streamIdList = streamList.stream().map(InlongStreamListResponse::getInlongStreamId)
                    .distinct().collect(Collectors.toList());
            List<String> resultList = sinkService.getExistsStreamIdList(groupId, sinkType, streamIdList);
            streamList.removeIf(entity -> resultList.contains(entity.getInlongStreamId()));
        }

        // Query all stream sink targets corresponding to each inlong stream according to streamId
        if (request.getNeedSinkList() == 1) {
            streamList.forEach(stream -> {
                String streamId = stream.getInlongStreamId();
                List<String> sinkTypeList = sinkService.getSinkTypeList(groupId, streamId);
                stream.setSinkTypeList(sinkTypeList);
            });
        }

        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<InlongStreamListResponse> page = new PageInfo<>(streamList);
        page.setTotal(streamList.size());

        LOGGER.debug("success to list inlong stream info for groupId={}", groupId);
        return page;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Boolean update(InlongStreamRequest request, String operator) {
        LOGGER.debug("begin to update inlong stream info={}", request);
        Preconditions.checkNotNull(request, "inlong stream request is empty");
        String groupId = request.getInlongGroupId();
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        String streamId = request.getInlongStreamId();
        Preconditions.checkNotNull(streamId, Constant.STREAM_ID_IS_EMPTY);

        // Check if it can be modified
        InlongGroupEntity inlongGroupEntity = this.checkBizIsTempStatus(groupId);

        // Make sure the stream was exists
        InlongStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        if (streamEntity == null) {
            LOGGER.error("inlong stream not found by groupId={}, streamId={}", groupId, streamId);
            throw new BusinessException(ErrorCodeEnum.STREAM_NOT_FOUND);
        }

        // Check whether the current inlong group status supports modification
        this.checkCanUpdate(inlongGroupEntity.getStatus(), streamEntity, request);

        CommonBeanUtils.copyProperties(request, streamEntity, true);
        streamEntity.setModifier(operator);
        streamEntity.setStatus(EntityStatus.GROUP_CONFIG_ING.getCode());
        streamMapper.updateByIdentifierSelective(streamEntity);

        // Update field information
        this.updateField(groupId, streamId, request.getFieldList());

        LOGGER.info("success to update inlong stream for groupId={}", groupId);
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Boolean delete(String groupId, String streamId, String operator) {
        LOGGER.debug("begin to delete inlong stream, groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, Constant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(groupId);

        InlongStreamEntity entity = streamMapper.selectByIdentifier(groupId, streamId);
        if (entity == null) {
            LOGGER.error("inlong stream not found by groupId={}, streamId={}", groupId, streamId);
            throw new BusinessException(ErrorCodeEnum.STREAM_NOT_FOUND);
        }

        // If there is undeleted stream source, the deletion fails
        Integer sourceCount = sourceService.getCount(groupId, streamId);
        if (sourceCount > 0) {
            LOGGER.error("inlong stream has undeleted sources, delete failed");
            throw new BusinessException(ErrorCodeEnum.STREAM_DELETE_HAS_SOURCE);
        }

        // If there is undeleted stream sink, the deletion fails
        int sinkCount = sinkService.getCount(groupId, streamId);
        if (sinkCount > 0) {
            LOGGER.error("inlong stream has undeleted sinks, delete failed");
            throw new BusinessException(ErrorCodeEnum.STREAM_DELETE_HAS_SINK);
        }

        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        streamMapper.updateByPrimaryKey(entity);

        // Logically delete the associated field table
        LOGGER.debug("begin to delete inlong stream field, streamId={}", streamId);
        streamFieldMapper.logicDeleteAllByIdentifier(groupId, streamId);

        LOGGER.info("success to delete inlong stream, ext property and fields for groupId={}", groupId);
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Boolean logicDeleteAll(String groupId, String operator) {
        LOGGER.debug("begin to delete all inlong stream by groupId={}", groupId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(groupId);

        List<InlongStreamEntity> entityList = streamMapper.selectByGroupId(groupId);
        if (CollectionUtils.isEmpty(entityList)) {
            LOGGER.info("inlong stream not found by groupId={}", groupId);
            return true;
        }

        for (InlongStreamEntity entity : entityList) {
            entity.setIsDeleted(1);
            entity.setModifier(operator);
            streamMapper.updateByIdentifierSelective(entity);

            String streamId = entity.getInlongStreamId();
            // Logically delete the associated field, source and sink info
            streamFieldMapper.logicDeleteAllByIdentifier(groupId, streamId);
            sourceService.logicDeleteAll(groupId, streamId, operator);
            sinkService.logicDeleteAll(groupId, streamId, operator);
        }

        LOGGER.info("success to delete all inlong stream, ext property and fields by groupId={}", groupId);
        return true;
    }

    /**
     * According to groupId and streamId, query whether there are undeleted data sources
     */
    private boolean hasDataSource(String groupId, String streamId, String dataSourceType) {
        boolean exist;
        if (Constant.DATA_SOURCE_FILE.equalsIgnoreCase(dataSourceType)) {
            List<SourceFileDetailInfo> fileDetailList = sourceFileService.listDetailByIdentifier(groupId, streamId);
            exist = CollectionUtils.isNotEmpty(fileDetailList);
        } else if (Constant.DATA_SOURCE_DB.equalsIgnoreCase(dataSourceType)) {
            List<SourceDbDetailInfo> dbDetailList = sourceDbService.listDetailByIdentifier(groupId, streamId);
            exist = CollectionUtils.isNotEmpty(dbDetailList);
        } else {
            exist = false;
        }
        return exist;
    }

    @Override
    public List<StreamBriefResponse> getBriefList(String groupId) {
        LOGGER.debug("begin to get inlong stream brief list by groupId={}", groupId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);

        List<InlongStreamEntity> entityList = streamMapper.selectByGroupId(groupId);
        List<StreamBriefResponse> briefInfoList = CommonBeanUtils
                .copyListProperties(entityList, StreamBriefResponse::new);

        // Query stream sinks based on groupId and streamId
        for (StreamBriefResponse briefInfo : briefInfoList) {
            String streamId = briefInfo.getInlongStreamId();
            List<SinkBriefResponse> sinkList = sinkService.listBrief(groupId, streamId);
            briefInfo.setSinkList(sinkList);
        }

        LOGGER.info("success to get inlong stream brief list for groupId={}", groupId);
        return briefInfoList;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean saveAll(FullStreamRequest fullStreamRequest, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to save all stream page info: {}", fullStreamRequest);
        }
        Preconditions.checkNotNull(fullStreamRequest, "fullStreamRequest is empty");
        InlongStreamRequest streamRequest = fullStreamRequest.getStreamInfo();
        Preconditions.checkNotNull(streamRequest, "inlong stream info is empty");

        // Check whether it can be added: check by lower-level specific services
        // this.checkBizIsTempStatus(streamInfo.getInlongGroupId());

        // 1. Save inlong stream
        this.save(streamRequest, operator);

        // 2 Save source info
        if (CollectionUtils.isNotEmpty(fullStreamRequest.getSourceInfo())) {
            for (SourceRequest source : fullStreamRequest.getSourceInfo()) {
                sourceService.save(source, operator);
            }
        }

        // 3. Save sink info
        if (CollectionUtils.isNotEmpty(fullStreamRequest.getSinkInfo())) {
            for (SinkRequest sinkInfo : fullStreamRequest.getSinkInfo()) {
                sinkService.save(sinkInfo, operator);
            }
        }

        LOGGER.info("success to save all stream page info");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean batchSaveAll(List<FullStreamRequest> fullStreamRequestList, String operator) {
        if (CollectionUtils.isEmpty(fullStreamRequestList)) {
            return true;
        }
        LOGGER.info("begin to batch save all stream page info, batch size={}", fullStreamRequestList.size());

        // Check if it can be added
        InlongStreamRequest firstStream = fullStreamRequestList.get(0).getStreamInfo();
        Preconditions.checkNotNull(firstStream, "inlong stream info is empty");
        String groupId = firstStream.getInlongGroupId();
        this.checkBizIsTempStatus(groupId);

        // This bulk save is only used when creating or editing inlong group after approval is rejected.
        // To ensure data consistency, you need to physically delete all associated data and then add
        // Note: There may be records with the same groupId and streamId in the historical data,
        // and the ones with is_deleted=0 should be deleted
        streamMapper.deleteAllByGroupId(groupId);

        for (FullStreamRequest pageInfo : fullStreamRequestList) {
            // 1.1 Delete the inlong stream extensions and fields corresponding to groupId and streamId
            InlongStreamRequest streamInfo = pageInfo.getStreamInfo();
            String streamId = streamInfo.getInlongStreamId();
            streamFieldMapper.deleteAllByIdentifier(groupId, streamId);

            // 2. Delete all stream source
            sourceService.deleteAll(groupId, streamId, operator);

            // 3. Delete all stream sink
            sinkService.deleteAll(groupId, streamId, operator);

            // 4. Save the inlong stream of this batch
            this.saveAll(pageInfo, operator);
        }
        LOGGER.info("success to batch save all stream page info");
        return true;
    }

    @Override
    public PageInfo<FullStreamResponse> listAllWithGroupId(InlongStreamPageRequest request) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to list full inlong stream page by {}", request);
        }
        Preconditions.checkNotNull(request, "request is empty");
        Preconditions.checkNotNull(request.getInlongGroupId(), Constant.GROUP_ID_IS_EMPTY);

        // 1. Query all valid data sources under groupId
        String groupId = request.getInlongGroupId();
        // The person in charge of the inlong group has the authority of all inlong streams
        InlongGroupEntity inlongGroupEntity = groupMapper.selectByGroupId(groupId);
        Preconditions.checkNotNull(inlongGroupEntity, "inlong group not found by groupId=" + groupId);

        String inCharges = inlongGroupEntity.getInCharges();
        request.setInCharges(inCharges);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongStreamEntity> page = (Page<InlongStreamEntity>) streamMapper.selectByCondition(request);
        List<InlongStreamResponse> streamInfoList = CommonBeanUtils.copyListProperties(page, InlongStreamResponse::new);

        // Convert and encapsulate the paged results
        List<FullStreamResponse> responseList = new ArrayList<>(streamInfoList.size());
        for (InlongStreamResponse streamInfo : streamInfoList) {
            // 2Set the field information of the inlong stream
            String streamId = streamInfo.getInlongStreamId();
            List<InlongStreamFieldInfo> streamFields = getStreamFields(groupId, streamId);
            streamInfo.setFieldList(streamFields);

            FullStreamResponse pageInfo = new FullStreamResponse();
            pageInfo.setStreamInfo(streamInfo);

            // 3. Query stream sources information
            List<SourceResponse> sourceList = sourceService.listSource(groupId, streamId);
            pageInfo.setSourceInfo(sourceList);

            // 4. Query various stream sinks and its extended information, field information
            List<SinkResponse> sinkList = sinkService.listSink(groupId, streamId);
            pageInfo.setSinkInfo(sinkList);

            // 5. Add a single result to the paginated list
            responseList.add(pageInfo);
        }

        PageInfo<FullStreamResponse> pageInfo = new PageInfo<>(responseList);
        pageInfo.setTotal(pageInfo.getTotal());

        LOGGER.debug("success to list full inlong stream info");
        return pageInfo;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean updateAll(InlongStreamRequest request, String operator) {
        LOGGER.info("begin to update all stream page info: " + request);
        Preconditions.checkNotNull(request, "request is empty");

        this.update(request, operator);
        LOGGER.info("success to update all stream page info");
        return true;
    }

    @Override
    public int selectCountByGroupId(String groupId) {
        LOGGER.debug("begin to get count by groupId={}", groupId);
        if (StringUtils.isEmpty(groupId)) {
            return 0;
        }
        int count = streamMapper.selectCountByGroupId(groupId);
        LOGGER.info("success to get count");
        return count;
    }

    @Override
    public List<InlongStreamTopicResponse> getTopicList(String groupId) {
        LOGGER.debug("begin bo get topic list by group id={}", groupId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);

        List<InlongStreamTopicResponse> topicList = streamMapper.selectTopicList(groupId);
        LOGGER.debug("success to get topic list by groupId={}", groupId);
        return topicList;
    }

    @Override
    public boolean updateAfterApprove(List<InlongStreamApproveRequest> streamApproveList, String operator) {
        if (CollectionUtils.isEmpty(streamApproveList)) {
            return true;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update stream after approve={}", streamApproveList);
        }

        String groupId = null;
        for (InlongStreamApproveRequest info : streamApproveList) {
            // Modify the inlong stream info after approve
            InlongStreamEntity streamEntity = new InlongStreamEntity();
            groupId = info.getInlongGroupId(); // these groupIds are all the same
            streamEntity.setInlongGroupId(groupId);
            streamEntity.setInlongStreamId(info.getInlongStreamId());
            streamEntity.setStatus(EntityStatus.STREAM_CONFIG_ING.getCode());
            streamMapper.updateByIdentifierSelective(streamEntity);

            // Modify the sink info after approve, such as update cluster info
            sinkService.updateAfterApprove(info.getSinkList(), operator);
        }

        LOGGER.info("success to update stream after approve for groupId={}", groupId);
        return true;
    }

    @Override
    public boolean updateStatus(String groupId, String streamId, Integer status, String operator) {
        LOGGER.debug("begin to update status by groupId={}, streamId={}", groupId, streamId);
        streamMapper.updateStatusByIdentifier(groupId, streamId, status, operator);
        LOGGER.info("success to update stream after approve for groupId=" + groupId + ", streamId=" + streamId);
        return true;
    }

    @Override
    public void insertDlqOrRlq(String groupId, String topicName, String operator) {
        Integer count = streamMapper.selectExistByIdentifier(groupId, topicName);
        if (count >= 1) {
            LOGGER.error("DLQ/RLQ topic already exists with name={}", topicName);
            throw new BusinessException(ErrorCodeEnum.STREAM_ID_DUPLICATE, "DLQ/RLQ topic already exists");
        }

        InlongStreamEntity streamEntity = new InlongStreamEntity();
        streamEntity.setInlongGroupId(groupId);
        streamEntity.setInlongStreamId(topicName);
        streamEntity.setMqResourceObj(topicName);
        streamEntity.setDescription("This is DLQ / RLQ topic created by SYSTEM");
        streamEntity.setDailyRecords(1000);
        streamEntity.setDailyStorage(1000);
        streamEntity.setPeakRecords(1000);
        streamEntity.setMaxLength(1000);

        streamEntity.setStatus(EntityStatus.STREAM_CONFIG_SUCCESSFUL.getCode());
        streamEntity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
        streamEntity.setCreator(operator);
        streamEntity.setModifier(operator);
        Date now = new Date();
        streamEntity.setCreateTime(now);
        streamEntity.setModifyTime(now);

        streamMapper.insert(streamEntity);
    }

    @Override
    public void logicDeleteDlqOrRlq(String groupId, String topicName, String operator) {
        streamMapper.logicDeleteDlqOrRlq(groupId, topicName, operator);
        LOGGER.info("success to logic delete dlq or rlq by groupId={}, topicName={}", groupId, topicName);
    }

    /**
     * Update extended information
     * <p/>First physically delete the existing extended information, and then add this batch of extended information
     */
    @Transactional(rollbackFor = Throwable.class)
    void updateExt(String groupId, String streamId, List<InlongStreamExtInfo> extInfoList) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update inlong stream ext, groupId={}, streamId={}, ext={}", groupId, streamId,
                    extInfoList);
        }

        try {
            streamExtMapper.deleteAllByIdentifier(groupId, streamId);
            saveExt(groupId, streamId, extInfoList, new Date());
            LOGGER.info("success to update inlong stream ext for groupId={}", groupId);
        } catch (Exception e) {
            LOGGER.error("failed to update inlong stream ext: ", e);
            throw new BusinessException(ErrorCodeEnum.STREAM_EXT_SAVE_FAILED);
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    void saveExt(String groupId, String streamId, List<InlongStreamExtInfo> infoList, Date date) {
        if (CollectionUtils.isEmpty(infoList)) {
            return;
        }
        List<InlongStreamExtEntity> entityList = CommonBeanUtils.copyListProperties(infoList,
                InlongStreamExtEntity::new);
        for (InlongStreamExtEntity entity : entityList) {
            entity.setInlongGroupId(groupId);
            entity.setInlongStreamId(streamId);
            entity.setModifyTime(date);
        }
        streamExtMapper.insertAll(entityList);
    }

    /**
     * Update field information
     * <p/>First physically delete the existing field information, and then add the field information of this batch
     */
    @Transactional(rollbackFor = Throwable.class)
    void updateField(String groupId, String streamId, List<InlongStreamFieldInfo> fieldInfoList) {
        LOGGER.debug("begin to update inlong stream field, groupId={}, streamId={}, field={}", groupId, streamId,
                fieldInfoList);
        try {
            streamFieldMapper.deleteAllByIdentifier(groupId, streamId);
            saveField(groupId, streamId, fieldInfoList);
            LOGGER.info("success to update inlong stream field for groupId={}", groupId);
        } catch (Exception e) {
            LOGGER.error("failed to update inlong stream field: ", e);
            throw new BusinessException(ErrorCodeEnum.STREAM_FIELD_SAVE_FAILED);
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    void saveField(String groupId, String streamId, List<InlongStreamFieldInfo> infoList) {
        if (CollectionUtils.isEmpty(infoList)) {
            return;
        }
        List<InlongStreamFieldEntity> entities = CommonBeanUtils.copyListProperties(infoList,
                InlongStreamFieldEntity::new);
        for (InlongStreamFieldEntity entity : entities) {
            entity.setInlongGroupId(groupId);
            entity.setInlongStreamId(streamId);
        }
        streamFieldMapper.insertAll(entities);
    }

    /**
     * Check whether the inlong group status is temporary
     *
     * @param groupId Inlong group id
     * @return usiness entity for caller reuse
     */
    private InlongGroupEntity checkBizIsTempStatus(String groupId) {
        InlongGroupEntity inlongGroupEntity = groupMapper.selectByGroupId(groupId);
        Preconditions.checkNotNull(inlongGroupEntity, "groupId is invalid");
        // Add/modify/delete is not allowed under certain inlong group status
        if (EntityStatus.GROUP_TEMP_STATUS.contains(inlongGroupEntity.getStatus())) {
            LOGGER.error("inlong group status was not allowed to add/update/delete inlong stream");
            throw new BusinessException(ErrorCodeEnum.STREAM_OPT_NOT_ALLOWED);
        }

        return inlongGroupEntity;
    }

    /**
     * Verify the fields that cannot be modified in the current inlong group status
     *
     * @param groupStatus Inlong group status
     * @param streamEntity Original inlong stream entity
     * @param request New inlong stream information
     */
    private void checkCanUpdate(Integer groupStatus, InlongStreamEntity streamEntity, InlongStreamRequest request) {
        if (streamEntity == null || request == null) {
            return;
        }

        // Fields that are not allowed to be modified when the inlong group [configuration is successful]
        if (EntityStatus.GROUP_CONFIG_SUCCESSFUL.getCode().equals(groupStatus)) {
            checkUpdatedFields(streamEntity, request);
        }

        // Inlong group [Waiting to submit] [Approval rejected] [Configuration failed], if there is a
        // stream source/stream sink, the fields that are not allowed to be modified
        List<Integer> statusList = Arrays.asList(
                EntityStatus.GROUP_WAIT_SUBMIT.getCode(),
                EntityStatus.GROUP_APPROVE_REJECTED.getCode(),
                EntityStatus.GROUP_CONFIG_FAILED.getCode());
        if (statusList.contains(groupStatus)) {
            String groupId = request.getInlongGroupId();
            String streamId = request.getInlongStreamId();
            // Whether there is undeleted stream source and sink
            int sourceCount = sourceService.getCount(groupId, streamId);
            int sinkCount = sinkService.getCount(groupId, streamId);
            if (sourceCount > 0 || sinkCount > 0) {
                checkUpdatedFields(streamEntity, request);
            }
        }
    }

    /**
     * Check that groupId, streamId, and dataSourceType are not allowed to be modified
     */
    private void checkUpdatedFields(InlongStreamEntity streamEntity, InlongStreamRequest request) {
        String newGroupId = request.getInlongGroupId();
        if (newGroupId != null && !newGroupId.equals(streamEntity.getInlongGroupId())) {
            LOGGER.error("current status was not allowed to update inlong group id");
            throw new BusinessException(ErrorCodeEnum.STREAM_ID_UPDATE_NOT_ALLOWED);
        }

        String newStreamId = request.getInlongStreamId();
        if (newStreamId != null && !newStreamId.equals(streamEntity.getInlongStreamId())) {
            LOGGER.error("current status was not allowed to update inlong stream id");
            throw new BusinessException(ErrorCodeEnum.STREAM_ID_UPDATE_NOT_ALLOWED);
        }
    }

}
