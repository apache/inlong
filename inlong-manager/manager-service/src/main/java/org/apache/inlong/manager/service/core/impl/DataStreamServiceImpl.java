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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbBasicInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileBasicInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileDetailInfo;
import org.apache.inlong.manager.common.pojo.datastorage.BaseStorageInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageSummaryInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamApproveInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamExtInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamListVO;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamPageRequest;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamSummaryInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamTopicVO;
import org.apache.inlong.manager.common.pojo.datastream.FullPageInfo;
import org.apache.inlong.manager.common.pojo.datastream.FullPageUpdateInfo;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.apache.inlong.manager.dao.entity.DataStreamEntity;
import org.apache.inlong.manager.dao.entity.DataStreamExtEntity;
import org.apache.inlong.manager.dao.entity.DataStreamFieldEntity;
import org.apache.inlong.manager.dao.mapper.BusinessEntityMapper;
import org.apache.inlong.manager.dao.mapper.DataStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.DataStreamExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.DataStreamFieldEntityMapper;
import org.apache.inlong.manager.service.core.DataStreamService;
import org.apache.inlong.manager.service.core.SourceDbService;
import org.apache.inlong.manager.service.core.SourceFileService;
import org.apache.inlong.manager.service.core.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Data stream service layer implementation
 */
@Service
public class DataStreamServiceImpl implements DataStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamServiceImpl.class);

    @Autowired
    private DataStreamEntityMapper streamMapper;
    @Autowired
    private DataStreamExtEntityMapper streamExtMapper;
    @Autowired
    private DataStreamFieldEntityMapper streamFieldMapper;
    @Autowired
    private BusinessEntityMapper businessMapper;
    @Autowired
    private SourceFileService sourceFileService;
    @Autowired
    private SourceDbService sourceDbService;
    @Autowired
    private StorageService storageService;

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Integer save(DataStreamInfo streamInfo, String operator) {
        LOGGER.debug("begin to save data stream info={}", streamInfo);
        Preconditions.checkNotNull(streamInfo, "data stream info is empty");
        String groupId = streamInfo.getInlongGroupId();
        String streamId = streamInfo.getInlongStreamId();
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be added
        BusinessEntity businessEntity = this.checkBizIsTempStatus(groupId);

        // The streamId under the same groupId cannot be repeated
        Integer count = streamMapper.selectExistByIdentifier(groupId, streamId);
        if (count >= 1) {
            LOGGER.error("data stream id [{}] has already exists", streamId);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_ID_DUPLICATE);
        }

        streamInfo.setMqResourceObj(streamId);
        // Processing dataStream
        DataStreamEntity streamEntity = CommonBeanUtils.copyProperties(streamInfo, DataStreamEntity::new);
        Date date = new Date();
        streamEntity.setStatus(EntityStatus.DATA_STREAM_NEW.getCode());
        streamEntity.setModifier(operator);
        streamEntity.setCreateTime(date);

        streamMapper.insertSelective(streamEntity);

        // Processing extended information
        this.saveExt(groupId, streamId, streamInfo.getExtList(), date);
        // Process data source fields
        this.saveField(groupId, streamId, streamInfo.getFieldList());

        LOGGER.info("success to save data stream info for groupId={}", groupId);
        return streamEntity.getId();
    }

    @Override
    public DataStreamInfo get(String groupId, String streamId) {
        LOGGER.debug("begin to get data stream by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        DataStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        if (streamEntity == null) {
            LOGGER.error("data stream not found by groupId={}, streamId={}", groupId, streamId);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_NOT_FOUND);
        }

        DataStreamInfo streamInfo = CommonBeanUtils.copyProperties(streamEntity, DataStreamInfo::new);
        this.setStreamExtAndField(groupId, streamId, streamInfo);

        LOGGER.info("success to get data stream for groupId={}", groupId);
        return streamInfo;
    }

    @Override
    public Boolean exist(String groupId, String streamId) {
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        DataStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        return streamEntity != null;
    }

    /**
     * Query and set the extended information and data source fields of the data stream
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @param streamInfo Data stream that needs to be filled
     */
    private void setStreamExtAndField(String groupId, String streamId, DataStreamInfo streamInfo) {
        List<DataStreamExtEntity> extEntityList = streamExtMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isNotEmpty(extEntityList)) {
            streamInfo.setExtList(CommonBeanUtils.copyListProperties(extEntityList, DataStreamExtInfo::new));
        }
        List<DataStreamFieldEntity> fieldEntityList = streamFieldMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isNotEmpty(fieldEntityList)) {
            streamInfo.setFieldList(CommonBeanUtils.copyListProperties(fieldEntityList, DataStreamFieldInfo::new));
        }
    }

    @Override
    public PageInfo<DataStreamListVO> listByCondition(DataStreamPageRequest request) {
        LOGGER.debug("begin to list data stream page by {}", request);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<DataStreamEntity> entityPage = (Page<DataStreamEntity>) streamMapper.selectByCondition(request);
        List<DataStreamListVO> dataStreamList = CommonBeanUtils.copyListProperties(entityPage, DataStreamListVO::new);

        // Filter out data streams that do not have this storage type (only one of each data stream can be created)
        String groupId = request.getInlongGroupId();
        String storageType = request.getStorageType();
        if (StringUtils.isNotEmpty(storageType)) {
            List<String> streamIdList = dataStreamList.stream().map(DataStreamListVO::getInlongStreamId)
                    .distinct().collect(Collectors.toList());
            List<String> resultList = storageService.filterStreamIdByStorageType(groupId, storageType, streamIdList);
            dataStreamList.removeIf(entity -> resultList.contains(entity.getInlongStreamId()));
        }

        // Query all data storage targets corresponding to each data stream according to streamId
        if (request.getNeedStorageList() == 1) {
            dataStreamList.forEach(stream -> {
                String streamId = stream.getInlongStreamId();
                List<String> storageTypeList = storageService.getStorageTypeList(groupId, streamId);
                stream.setStorageTypeList(storageTypeList);
            });
        }

        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<DataStreamListVO> page = new PageInfo<>(dataStreamList);
        page.setTotal(dataStreamList.size());

        LOGGER.debug("success to list data stream info for groupId={}", groupId);
        return page;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean update(DataStreamInfo streamInfo, String operator) {
        LOGGER.debug("begin to update data stream info={}", streamInfo);
        Preconditions.checkNotNull(streamInfo, "data stream info is empty");
        String groupId = streamInfo.getInlongGroupId();
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        String streamId = streamInfo.getInlongStreamId();
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be modified
        BusinessEntity businessEntity = this.checkBizIsTempStatus(groupId);

        // Add if it doesn't exist, modify if it exists
        DataStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        if (streamEntity == null) {
            this.save(streamInfo, operator);
        } else {
            // Check whether the current business status supports modification
            this.checkCanUpdate(businessEntity.getStatus(), streamEntity, streamInfo);

            CommonBeanUtils.copyProperties(streamInfo, streamEntity, true);
            streamEntity.setModifier(operator);
            streamEntity.setStatus(EntityStatus.BIZ_CONFIG_ING.getCode());
            streamMapper.updateByIdentifierSelective(streamEntity);

            // Update extended information, field information
            this.updateExt(groupId, streamId, streamInfo.getExtList());
            this.updateField(groupId, streamId, streamInfo.getFieldList());
        }

        LOGGER.info("success to update business info for groupId={}", groupId);
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean delete(String groupId, String streamId, String operator) {
        LOGGER.debug("begin to delete data stream, groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        BusinessEntity businessEntity = this.checkBizIsTempStatus(groupId);

        DataStreamEntity entity = streamMapper.selectByIdentifier(groupId, streamId);
        if (entity == null) {
            LOGGER.error("data stream not found by groupId={}, streamId={}", groupId, streamId);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_NOT_FOUND);
        }

        // If there is an undeleted data source, the deletion fails
        boolean dataSourceExist = hasDataSource(groupId, streamId, entity.getDataSourceType());
        if (dataSourceExist) {
            LOGGER.error("data stream has undeleted data sources, delete failed");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_DELETE_HAS_SOURCE);
        }

        // If there is undeleted data storage information, the deletion fails
        boolean dataStorageExist = hasDataStorage(groupId, streamId);
        if (dataStorageExist) {
            LOGGER.error("data stream has undeleted data storages, delete failed");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_DELETE_HAS_STORAGE);
        }

        entity.setIsDeleted(1);
        entity.setModifier(operator);
        streamMapper.updateByPrimaryKey(entity);

        // To logically delete the associated extension table
        LOGGER.debug("begin to delete data stream ext property, groupId={}, streamId={}", groupId, streamId);
        streamExtMapper.logicDeleteAllByIdentifier(groupId, streamId);

        // Logically delete the associated field table
        LOGGER.debug("begin to delete data stream field, streamId={}", streamId);
        streamFieldMapper.logicDeleteAllByIdentifier(groupId, streamId);

        LOGGER.info("success to delete data stream, ext property and fields for groupId={}", groupId);
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean logicDeleteAll(String groupId, String operator) {
        LOGGER.debug("begin to delete all data stream by groupId={}", groupId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(groupId);

        List<DataStreamEntity> entityList = streamMapper.selectByGroupId(groupId);
        if (CollectionUtils.isEmpty(entityList)) {
            LOGGER.info("data stream not found by groupId={}", groupId);
            return true;
        }

        for (DataStreamEntity entity : entityList) {
            entity.setIsDeleted(1);
            entity.setModifier(operator);
            streamMapper.updateByIdentifierSelective(entity);

            String streamId = entity.getInlongStreamId();
            // To logically delete the associated extension table
            streamExtMapper.logicDeleteAllByIdentifier(groupId, streamId);
            // Logically delete the associated field table
            streamFieldMapper.logicDeleteAllByIdentifier(groupId, streamId);
            // Tombstone the associated data source
            sourceFileService.logicDeleteAllByIdentifier(groupId, streamId, operator);
            sourceDbService.logicDeleteAllByIdentifier(groupId, streamId, operator);
            // Logical deletion of associated data storage information
            storageService.logicDeleteAllByIdentifier(groupId, streamId, operator);
        }

        LOGGER.info("success to delete all data stream, ext property and fields by groupId={}", groupId);
        return true;
    }

    /**
     * According to groupId and streamId, query the number of associated undeleted data storage
     */
    private boolean hasDataStorage(String groupId, String streamId) {
        int count = storageService.getCountByIdentifier(groupId, streamId);
        return count > 0;
    }

    /**
     * According to groupId and streamId, query whether there are undeleted data sources
     */
    private boolean hasDataSource(String groupId, String streamId, String dataSourceType) {
        boolean exist;
        if (BizConstant.DATA_SOURCE_FILE.equalsIgnoreCase(dataSourceType)) {
            List<SourceFileDetailInfo> fileDetailList = sourceFileService.listDetailByIdentifier(groupId, streamId);
            exist = CollectionUtils.isNotEmpty(fileDetailList);
        } else if (BizConstant.DATA_SOURCE_DB.equalsIgnoreCase(dataSourceType)) {
            List<SourceDbDetailInfo> dbDetailList = sourceDbService.listDetailByIdentifier(groupId, streamId);
            exist = CollectionUtils.isNotEmpty(dbDetailList);
        } else {
            exist = false;
        }
        return exist;
    }

    @Override
    public List<DataStreamSummaryInfo> getSummaryList(String groupId) {
        LOGGER.debug("begin to get data stream summary list by groupId={}", groupId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        List<DataStreamEntity> entityList = streamMapper.selectByGroupId(groupId);
        List<DataStreamSummaryInfo> summaryInfoList = CommonBeanUtils
                .copyListProperties(entityList, DataStreamSummaryInfo::new);

        // Query data storage based on groupId and streamId
        for (DataStreamSummaryInfo summaryInfo : summaryInfoList) {
            String streamId = summaryInfo.getInlongStreamId();
            List<StorageSummaryInfo> storageList = storageService.listSummaryByIdentifier(groupId, streamId);
            summaryInfo.setStorageList(storageList);
        }

        LOGGER.info("success to get data stream summary list for groupId={}", groupId);
        return summaryInfoList;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean saveAll(FullPageInfo fullPageInfo, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to save all stream page info: {}", fullPageInfo);
        }
        Preconditions.checkNotNull(fullPageInfo, "fullPageInfo is empty");
        DataStreamInfo streamInfo = fullPageInfo.getStreamInfo();
        Preconditions.checkNotNull(streamInfo, "data stream info is empty");

        // Check whether it can be added: check by lower-level specific services
        // this.checkBizIsTempStatus(streamInfo.getInlongGroupId());

        // 1. Save data stream
        this.save(streamInfo, operator);

        // 2.1 Save file data source information
        if (fullPageInfo.getFileBasicInfo() != null) {
            sourceFileService.saveBasic(fullPageInfo.getFileBasicInfo(), operator);
        }
        if (CollectionUtils.isNotEmpty(fullPageInfo.getFileDetailInfoList())) {
            for (SourceFileDetailInfo detailInfo : fullPageInfo.getFileDetailInfoList()) {
                sourceFileService.saveDetail(detailInfo, operator);
            }
        }

        // 2.2 Save DB data source information
        if (fullPageInfo.getDbBasicInfo() != null) {
            sourceDbService.saveBasic(fullPageInfo.getDbBasicInfo(), operator);
        }
        if (CollectionUtils.isNotEmpty(fullPageInfo.getDbDetailInfoList())) {
            for (SourceDbDetailInfo detailInfo : fullPageInfo.getDbDetailInfoList()) {
                sourceDbService.saveDetail(detailInfo, operator);
            }
        }

        // 3. Save data storage information
        if (CollectionUtils.isNotEmpty(fullPageInfo.getStorageInfo())) {
            for (BaseStorageInfo storageInfo : fullPageInfo.getStorageInfo()) {
                storageService.save(storageInfo, operator);
            }
        }

        LOGGER.info("success to save all stream page info");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean batchSaveAll(List<FullPageInfo> fullPageInfoList, String operator) {
        if (CollectionUtils.isEmpty(fullPageInfoList)) {
            return true;
        }
        LOGGER.info("begin to batch save all stream page info, batch size={}", fullPageInfoList.size());

        // Check if it can be added
        DataStreamInfo firstStream = fullPageInfoList.get(0).getStreamInfo();
        Preconditions.checkNotNull(firstStream, "data stream info is empty");
        String groupId = firstStream.getInlongGroupId();
        this.checkBizIsTempStatus(groupId);

        // This bulk save is only used when creating new business or editing business after approval is rejected.
        // To ensure data consistency, you need to physically delete all associated data and then add
        // Note: There may be records with the same groupId and streamId in the historical data,
        // and the ones with is_deleted=0 should be deleted
        streamMapper.deleteAllByGroupId(groupId);

        for (FullPageInfo pageInfo : fullPageInfoList) {
            // 1.1 Delete the data stream extensions and fields corresponding to groupId and streamId
            DataStreamInfo streamInfo = pageInfo.getStreamInfo();
            String streamId = streamInfo.getInlongStreamId();

            streamExtMapper.deleteAllByIdentifier(groupId, streamId);
            streamFieldMapper.deleteAllByIdentifier(groupId, streamId);

            // 2. Delete file data source, DB data source information
            sourceFileService.deleteAllByIdentifier(groupId, streamId);
            sourceDbService.deleteAllByIdentifier(groupId, streamId);

            // 3. Delete data storage information
            storageService.deleteAllByIdentifier(groupId, streamId);

            // 4. Save the data stream of this batch
            this.saveAll(pageInfo, operator);
        }
        LOGGER.info("success to batch save all stream page info");
        return true;
    }

    @Override
    public PageInfo<FullPageInfo> listAllWithGroupId(DataStreamPageRequest request) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to list full data stream page by {}", request);
        }
        LOGGER.debug("begin to list full data stream page by {}", request);
        Preconditions.checkNotNull(request, "request is empty");
        Preconditions.checkNotNull(request.getInlongGroupId(), BizConstant.GROUP_ID_IS_EMPTY);

        // 1. Query all valid data sources under groupId
        String groupId = request.getInlongGroupId();
        // The person in charge of the business has the authority of all data streams
        BusinessEntity businessEntity = businessMapper.selectByIdentifier(groupId);
        Preconditions.checkNotNull(businessEntity, "business not found by groupId=" + groupId);

        String inCharges = businessEntity.getInCharges();
        request.setInCharges(inCharges);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<DataStreamEntity> page = (Page<DataStreamEntity>) streamMapper.selectByCondition(request);
        List<DataStreamInfo> streamInfoList = CommonBeanUtils.copyListProperties(page, DataStreamInfo::new);

        // Convert and encapsulate the paged results
        List<FullPageInfo> fullPageInfoList = new ArrayList<>(streamInfoList.size());
        for (DataStreamInfo streamInfo : streamInfoList) {
            // 2.1 Set the extended information and field information of the data stream
            String streamId = streamInfo.getInlongStreamId();
            setStreamExtAndField(groupId, streamId, streamInfo);

            // 2.3 Set the data stream to the result sub-object
            FullPageInfo pageInfo = new FullPageInfo();
            pageInfo.setStreamInfo(streamInfo);

            // 3. Query the basic and detailed information of the data source
            String dataSourceType = streamInfo.getDataSourceType();
            if (StringUtils.isEmpty(dataSourceType)) {
                continue;
            }
            switch (dataSourceType.toUpperCase(Locale.ROOT)) {
                case BizConstant.DATA_SOURCE_FILE:
                    SourceFileBasicInfo fileBasicInfo = sourceFileService.getBasicByIdentifier(groupId, streamId);
                    pageInfo.setFileBasicInfo(fileBasicInfo);
                    List<SourceFileDetailInfo> fileDetailInfoList = sourceFileService.listDetailByIdentifier(groupId,
                            streamId);
                    pageInfo.setFileDetailInfoList(fileDetailInfoList);
                    break;
                case BizConstant.DATA_SOURCE_DB:
                    SourceDbBasicInfo dbBasicInfo = sourceDbService.getBasicByIdentifier(groupId, streamId);
                    pageInfo.setDbBasicInfo(dbBasicInfo);
                    List<SourceDbDetailInfo> dbDetailInfoList = sourceDbService.listDetailByIdentifier(groupId,
                            streamId);
                    pageInfo.setDbDetailInfoList(dbDetailInfoList);
                    break;
                case BizConstant.DATA_SOURCE_AUTO_PUSH:
                    break;
                default:
                    throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_TYPE_NOT_SUPPORTED);
            }

            // 4. Query various data storage and its extended information, field information
            List<BaseStorageInfo> storageInfoList = storageService.listByIdentifier(groupId, streamId);
            pageInfo.setStorageInfo(storageInfoList);

            // 5. Add a single result to the paginated list
            fullPageInfoList.add(pageInfo);
        }

        PageInfo<FullPageInfo> pageInfo = new PageInfo<>(fullPageInfoList);
        pageInfo.setTotal(pageInfo.getTotal());

        LOGGER.info("success to list full data stream info");
        return pageInfo;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean updateAll(FullPageUpdateInfo updateInfo, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update all stream page info: {}", updateInfo);
        }
        Preconditions.checkNotNull(updateInfo, "updateInfo is empty");
        Preconditions.checkNotNull(updateInfo.getStreamInfo(), "data stream info is empty");

        // Check whether it can be added: check by sub-layer specific services
        // this.checkBizIsTempStatus(streamInfo.getInlongGroupId());

        // 1. Modify the data stream (data stream information cannot be empty)
        this.update(updateInfo.getStreamInfo(), operator);

        // 2. Modify the basic information of the file data source
        if (updateInfo.getFileBasicInfo() != null) {
            sourceFileService.updateBasic(updateInfo.getFileBasicInfo(), operator);
        }

        // 3. Save the basic information of the DB data source
        if (updateInfo.getDbBasicInfo() != null) {
            sourceDbService.updateBasic(updateInfo.getDbBasicInfo(), operator);
        }

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
    public List<DataStreamTopicVO> getTopicList(String groupId) {
        LOGGER.debug("begin bo get topic list by group id={}", groupId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        List<DataStreamTopicVO> topicList = streamMapper.selectTopicList(groupId);
        LOGGER.debug("success to get topic list by groupId={}", groupId);
        return topicList;
    }

    @Override
    public boolean updateAfterApprove(List<DataStreamApproveInfo> streamApproveList, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update stream after approve={}", streamApproveList);
        }
        if (CollectionUtils.isEmpty(streamApproveList)) {
            return true;
        }

        String groupId = null;
        for (DataStreamApproveInfo info : streamApproveList) {
            // Modify mqResourceObj
            DataStreamEntity streamEntity = new DataStreamEntity();
            groupId = info.getInlongGroupId(); // these groupIds are all the same
            streamEntity.setInlongGroupId(groupId);
            streamEntity.setInlongStreamId(info.getInlongStreamId());
            // Update status to [DATA_STREAM_CONFIG_ING]
            streamEntity.setStatus(EntityStatus.DATA_STREAM_CONFIG_ING.getCode());
            streamMapper.updateByIdentifierSelective(streamEntity);
            // If you need to change data stream info after approve, just do in here

            // Modify the storage information
            storageService.updateAfterApprove(info.getStorageList(), operator);
        }

        LOGGER.info("success to update stream after approve for groupId={}", groupId);
        return true;
    }

    @Override
    public boolean updateStatus(String groupId, String streamId, Integer status, String operator) {
        LOGGER.debug("begin to update status by groupId={}, streamId={}", groupId, streamId);

        // businessMapper.updateStatusByIdentifier(groupId, status, operator);
        streamMapper.updateStatusByIdentifier(groupId, streamId, status, operator);

        LOGGER.info("success to update stream after approve for groupId={}", groupId);
        return true;
    }

    @Override
    public void insertDlqOrRlq(String groupId, String topicName, String operator) {
        Integer count = streamMapper.selectExistByIdentifier(groupId, topicName);
        if (count >= 1) {
            LOGGER.error("DLQ/RLQ topic already exists with name={}", topicName);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_ID_DUPLICATE, "DLQ/RLQ topic already exists");
        }

        DataStreamEntity streamEntity = new DataStreamEntity();
        streamEntity.setInlongGroupId(groupId);
        streamEntity.setInlongStreamId(topicName);
        streamEntity.setMqResourceObj(topicName);
        streamEntity.setDescription("This is DLQ / RLQ topic created by SYSTEM");
        streamEntity.setDailyRecords(1000);
        streamEntity.setDailyStorage(1000);
        streamEntity.setPeakRecords(1000);
        streamEntity.setMaxLength(1000);

        streamEntity.setStatus(EntityStatus.DATA_STREAM_CONFIG_SUCCESSFUL.getCode());
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
    void updateExt(String groupId, String streamId, List<DataStreamExtInfo> extInfoList) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update data stream ext, groupId={}, streamId={}, ext={}", groupId, streamId,
                    extInfoList);
        }

        try {
            streamExtMapper.deleteAllByIdentifier(groupId, streamId);
            saveExt(groupId, streamId, extInfoList, new Date());
            LOGGER.info("success to update data stream ext for groupId={}", groupId);
        } catch (Exception e) {
            LOGGER.error("failed to update data stream ext: ", e);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_EXT_SAVE_FAILED);
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    void saveExt(String groupId, String streamId, List<DataStreamExtInfo> infoList, Date date) {
        if (CollectionUtils.isEmpty(infoList)) {
            return;
        }
        List<DataStreamExtEntity> entityList = CommonBeanUtils.copyListProperties(infoList, DataStreamExtEntity::new);
        for (DataStreamExtEntity entity : entityList) {
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
    void updateField(String groupId, String streamId, List<DataStreamFieldInfo> fieldInfoList) {
        LOGGER.debug("begin to update data stream field, groupId={}, streamId={}, field={}", groupId, streamId,
                fieldInfoList);
        try {
            streamFieldMapper.deleteAllByIdentifier(groupId, streamId);
            saveField(groupId, streamId, fieldInfoList);
            LOGGER.info("success to update data stream field for groupId={}", groupId);
        } catch (Exception e) {
            LOGGER.error("failed to update data stream field: ", e);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_FIELD_SAVE_FAILED);
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    void saveField(String groupId, String streamId, List<DataStreamFieldInfo> infoList) {
        if (CollectionUtils.isEmpty(infoList)) {
            return;
        }
        List<DataStreamFieldEntity> entities = CommonBeanUtils.copyListProperties(infoList, DataStreamFieldEntity::new);
        for (DataStreamFieldEntity entity : entities) {
            entity.setInlongGroupId(groupId);
            entity.setInlongStreamId(streamId);
        }
        streamFieldMapper.insertAll(entities);
    }

    /**
     * Check whether the business status is temporary
     *
     * @param groupId Business group id
     * @return usiness entity for caller reuse
     */
    private BusinessEntity checkBizIsTempStatus(String groupId) {
        BusinessEntity businessEntity = businessMapper.selectByIdentifier(groupId);
        Preconditions.checkNotNull(businessEntity, "groupId is invalid");
        // Add/modify/delete is not allowed under certain business status
        if (EntityStatus.BIZ_TEMP_STATUS.contains(businessEntity.getStatus())) {
            LOGGER.error("business status was not allowed to add/update/delete data stream");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_OPT_NOT_ALLOWED);
        }

        return businessEntity;
    }

    /**
     * Verify the fields that cannot be modified in the current business state
     *
     * @param bizStatus Business status
     * @param streamEntity Original data stream entity
     * @param streamInfo New data stream information
     */
    private void checkCanUpdate(Integer bizStatus, DataStreamEntity streamEntity, DataStreamInfo streamInfo) {
        if (streamEntity == null || streamInfo == null) {
            return;
        }

        // Fields that are not allowed to be modified when the business [configuration is successful]
        if (EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode().equals(bizStatus)) {
            checkUpdatedFields(streamEntity, streamInfo);
        }

        // Business [Waiting to submit] [Approval rejected] [Configuration failed], if there is a
        // data source/data storage, the fields that are not allowed to be modified
        List<Integer> statusList = Arrays.asList(
                EntityStatus.BIZ_WAIT_SUBMIT.getCode(),
                EntityStatus.BIZ_APPROVE_REJECTED.getCode(),
                EntityStatus.BIZ_CONFIG_FAILED.getCode());
        if (statusList.contains(bizStatus)) {
            String groupId = streamInfo.getInlongGroupId();
            String streamId = streamInfo.getInlongStreamId();
            // Whether there is an undeleted data source
            boolean dataSourceExist = hasDataSource(groupId, streamId, streamInfo.getDataSourceType());
            // Whether there is undeleted data storage
            boolean dataStorageExist = hasDataStorage(groupId, streamId);
            if (dataSourceExist || dataStorageExist) {
                checkUpdatedFields(streamEntity, streamInfo);
            }
        }
    }

    /**
     * Check that groupId, streamId, and dataSourceType are not allowed to be modified
     */
    private void checkUpdatedFields(DataStreamEntity streamEntity, DataStreamInfo streamInfo) {
        String newGroupId = streamInfo.getInlongGroupId();
        if (newGroupId != null && !newGroupId.equals(streamEntity.getInlongGroupId())) {
            LOGGER.error("current status was not allowed to update business group id");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_ID_UPDATE_NOT_ALLOWED);
        }

        String newDsid = streamInfo.getInlongStreamId();
        if (newDsid != null && !newDsid.equals(streamEntity.getInlongStreamId())) {
            LOGGER.error("current status was not allowed to update data stream id");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_ID_UPDATE_NOT_ALLOWED);
        }

        String newSourceType = streamInfo.getDataSourceType();
        if (newSourceType != null && !newSourceType.equals(streamEntity.getDataSourceType())) {
            LOGGER.error("current status was not allowed to update data source type");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_SOURCE_UPDATE_NOT_ALLOWED);
        }
    }

}
