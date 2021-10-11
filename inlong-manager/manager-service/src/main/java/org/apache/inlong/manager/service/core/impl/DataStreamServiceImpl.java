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
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfoToHiveConfig;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamListVO;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamPageRequest;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamSummaryInfo;
import org.apache.inlong.manager.common.pojo.datastream.FullDataStreamPageRequest;
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
    public Integer save(DataStreamInfo dataStreamInfo, String operator) {
        LOGGER.debug("begin to save data stream info={}", dataStreamInfo);
        Preconditions.checkNotNull(dataStreamInfo, "data stream info is empty");
        String bid = dataStreamInfo.getBusinessIdentifier();
        String dsid = dataStreamInfo.getDataStreamIdentifier();
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Check if it can be added
        BusinessEntity businessEntity = this.checkBizIsTempStatus(bid);

        // The dataStreamIdentifier under the same bid cannot be repeated
        Integer count = streamMapper.selectExistByIdentifier(bid, dsid);
        if (count >= 1) {
            LOGGER.error("data stream id [{}] has already exists", dsid);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_ID_DUPLICATE);
        }

        // Processing dataStream
        DataStreamEntity streamEntity = CommonBeanUtils.copyProperties(dataStreamInfo, DataStreamEntity::new);
        Date date = new Date();
        streamEntity.setStatus(EntityStatus.DATA_STREAM_NEW.getCode());
        streamEntity.setModifier(operator);
        streamEntity.setCreateTime(date);

        streamMapper.insertSelective(streamEntity);

        // Processing extended information
        this.saveExt(bid, dsid, dataStreamInfo.getExtList(), date);
        // Process data source fields
        this.saveField(bid, dsid, dataStreamInfo.getFieldList());

        LOGGER.info("success to save data stream info");
        return streamEntity.getId();
    }

    @Override
    public DataStreamInfo get(String bid, String dsid) {
        LOGGER.debug("begin to get data stream by bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        DataStreamEntity streamEntity = streamMapper.selectByIdentifier(bid, dsid);
        if (streamEntity == null) {
            LOGGER.error("data stream not found by bid={}, dsid={}", bid, dsid);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_NOT_FOUND);
        }

        DataStreamInfo streamInfo = CommonBeanUtils.copyProperties(streamEntity, DataStreamInfo::new);
        this.setStreamExtAndField(bid, dsid, streamInfo);

        LOGGER.info("success to get data stream");
        return streamInfo;
    }

    /**
     * Query and set the extended information and data source fields of the data stream
     *
     * @param bid Business identifier
     * @param dsid Data stream identifier
     * @param streamInfo Data stream that needs to be filled
     */
    private void setStreamExtAndField(String bid, String dsid, DataStreamInfo streamInfo) {
        List<DataStreamExtEntity> extEntityList = streamExtMapper.selectByIdentifier(bid, dsid);
        if (CollectionUtils.isNotEmpty(extEntityList)) {
            streamInfo.setExtList(CommonBeanUtils.copyListProperties(extEntityList, DataStreamExtInfo::new));
        }
        List<DataStreamFieldEntity> fieldEntityList = streamFieldMapper.selectByIdentifier(bid, dsid);
        if (CollectionUtils.isNotEmpty(fieldEntityList)) {
            streamInfo.setFieldList(CommonBeanUtils.copyListProperties(fieldEntityList, DataStreamFieldInfo::new));
        }
    }

    @Override
    public List<DataStreamInfoToHiveConfig> queryHiveConfigForAllDataStream(String businessId) {
        return streamMapper.queryStreamToHiveBaseInfoByBid(businessId);
    }

    @Override
    public DataStreamInfoToHiveConfig queryHiveConfigForOneDataStream(String bid, String dsid) {
        return streamMapper.queryStreamToHiveBaseInfoByIdentifier(bid, dsid);
    }

    @Override
    public PageInfo<DataStreamListVO> listByCondition(DataStreamPageRequest request) {
        LOGGER.debug("begin to list data stream page by {}", request);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<DataStreamEntity> entityPage = (Page<DataStreamEntity>) streamMapper.selectByCondition(request);
        List<DataStreamListVO> dataStreamList = CommonBeanUtils.copyListProperties(entityPage, DataStreamListVO::new);

        // Filter out data streams that do not have this storage type (only one of each data stream can be created)
        String bid = request.getBid();
        String storageType = request.getStorageType();
        if (StringUtils.isNotEmpty(storageType)) {
            List<String> dsidList = dataStreamList.stream().map(DataStreamListVO::getDataStreamIdentifier)
                    .distinct().collect(Collectors.toList());
            List<String> resultList = storageService.filterStreamIdByStorageType(bid, storageType, dsidList);
            dataStreamList.removeIf(entity -> resultList.contains(entity.getDataStreamIdentifier()));
        }

        // Query all data storage targets corresponding to each data stream according to dsid
        if (request.getNeedStorageList() == 1) {
            dataStreamList.forEach(stream -> {
                String dsid = stream.getDataStreamIdentifier();
                List<String> storageTypeList = storageService.getStorageTypeList(bid, dsid);
                stream.setStorageTypeList(storageTypeList);
            });
        }

        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<DataStreamListVO> page = new PageInfo<>(dataStreamList);
        page.setTotal(dataStreamList.size());

        LOGGER.debug("success to list data stream info");
        return page;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean update(DataStreamInfo streamInfo, String operator) {
        LOGGER.debug("begin to update data stream info={}", streamInfo);
        Preconditions.checkNotNull(streamInfo, "data stream info is empty");
        String bid = streamInfo.getBusinessIdentifier();
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        String dsid = streamInfo.getDataStreamIdentifier();
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Check if it can be modified
        BusinessEntity businessEntity = this.checkBizIsTempStatus(bid);

        // Add if it doesn't exist, modify if it exists
        DataStreamEntity streamEntity = streamMapper.selectByIdentifier(bid, dsid);
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
            this.updateExt(bid, dsid, streamInfo.getExtList());
            this.updateField(bid, dsid, streamInfo.getFieldList());
        }

        LOGGER.info("success to update business info");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean delete(String bid, String dsid, String operator) {
        LOGGER.debug("begin to delete data stream, bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Check if it can be deleted
        BusinessEntity businessEntity = this.checkBizIsTempStatus(bid);

        DataStreamEntity entity = streamMapper.selectByIdentifier(bid, dsid);
        if (entity == null) {
            LOGGER.error("data stream not found by bid={}, dsid={}", bid, dsid);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_NOT_FOUND);
        }

        // If there is an undeleted data source, the deletion fails
        boolean dataSourceExist = hasDataSource(bid, dsid, entity.getDataSourceType());
        if (dataSourceExist) {
            LOGGER.error("data stream has undeleted data sources, delete failed");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_DELETE_HAS_SOURCE);
        }

        // If there is undeleted data storage information, the deletion fails
        boolean dataStorageExist = hasDataStorage(bid, dsid);
        if (dataStorageExist) {
            LOGGER.error("data stream has undeleted data storages, delete failed");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_DELETE_HAS_STORAGE);
        }

        entity.setIsDeleted(1);
        entity.setModifier(operator);
        streamMapper.updateByPrimaryKey(entity);

        // To logically delete the associated extension table
        LOGGER.debug("begin to delete data stream ext property, bid={}, dsid={}", bid, dsid);
        streamExtMapper.logicDeleteAllByIdentifier(bid, dsid);

        // Logically delete the associated field table
        LOGGER.debug("begin to delete data stream field, dsid={}", dsid);
        streamFieldMapper.logicDeleteAllByIdentifier(bid, dsid);

        LOGGER.info("success to delete data stream, ext property and fields");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean logicDeleteAllByBid(String bid, String operator) {
        LOGGER.debug("begin to delete all data stream by bid={}", bid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(bid);

        List<DataStreamEntity> entityList = streamMapper.selectByBid(bid);
        if (CollectionUtils.isEmpty(entityList)) {
            LOGGER.info("data stream not found by bid={}", bid);
            return true;
        }

        for (DataStreamEntity entity : entityList) {
            entity.setIsDeleted(1);
            entity.setModifier(operator);
            streamMapper.updateByIdentifierSelective(entity);

            String dsid = entity.getDataStreamIdentifier();
            // To logically delete the associated extension table
            streamExtMapper.logicDeleteAllByIdentifier(bid, dsid);
            // Logically delete the associated field table
            streamFieldMapper.logicDeleteAllByIdentifier(bid, dsid);
            // Tombstone the associated data source
            sourceFileService.logicDeleteAllByIdentifier(bid, dsid, operator);
            sourceDbService.logicDeleteAllByIdentifier(bid, dsid, operator);
            // Logical deletion of associated data storage information
            storageService.logicDeleteAllByIdentifier(bid, dsid, operator);
        }

        LOGGER.info("success to delete all data stream, ext property and fields by bid={}", bid);
        return true;
    }

    /**
     * According to bid and dsid, query the number of associated undeleted data storage
     */
    private boolean hasDataStorage(String bid, String dsid) {
        int count = storageService.getCountByIdentifier(bid, dsid);
        return count > 0;
    }

    /**
     * According to bid and dsid, query whether there are undeleted data sources
     */
    private boolean hasDataSource(String bid, String dsid, String dataSourceType) {
        boolean exist;
        if (BizConstant.DATA_SOURCE_TYPE_FILE.equalsIgnoreCase(dataSourceType)) {
            List<SourceFileDetailInfo> fileDetailList = sourceFileService.listDetailByIdentifier(bid, dsid);
            exist = CollectionUtils.isNotEmpty(fileDetailList);
        } else if (BizConstant.DATA_SOURCE_TYPE_DB.equalsIgnoreCase(dataSourceType)) {
            List<SourceDbDetailInfo> dbDetailList = sourceDbService.listDetailByIdentifier(bid, dsid);
            exist = CollectionUtils.isNotEmpty(dbDetailList);
        } else {
            exist = false;
        }
        return exist;
    }

    @Override
    public List<DataStreamSummaryInfo> getSummaryList(String bid) {
        LOGGER.debug("begin to get data stream summary list by bid={}", bid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);

        List<DataStreamEntity> entityList = streamMapper.selectByBid(bid);
        List<DataStreamSummaryInfo> summaryInfoList = CommonBeanUtils
                .copyListProperties(entityList, DataStreamSummaryInfo::new);

        // Query data storage based on bid and dsid
        for (DataStreamSummaryInfo summaryInfo : summaryInfoList) {
            String dsid = summaryInfo.getDataStreamIdentifier();
            List<StorageSummaryInfo> storageList = storageService.listSummaryByIdentifier(bid, dsid);
            summaryInfo.setStorageList(storageList);
        }

        LOGGER.info("success to get data stream summary list");
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
        // this.checkBizIsTempStatus(streamInfo.getBusinessIdentifier());

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
        String bid = firstStream.getBusinessIdentifier();
        this.checkBizIsTempStatus(bid);

        // This bulk save is only used when creating new business or editing business after approval is rejected.
        // To ensure data consistency, you need to physically delete all associated data and then add
        // Note: There may be records with the same bid and dsid in the historical data,
        // and the ones with is_deleted=0 should be deleted
        streamMapper.deleteAllByBid(bid);

        for (FullPageInfo pageInfo : fullPageInfoList) {
            // 1.1 Delete the data stream extensions and fields corresponding to bid and dsid
            DataStreamInfo streamInfo = pageInfo.getStreamInfo();
            String dsid = streamInfo.getDataStreamIdentifier();

            streamExtMapper.deleteAllByIdentifier(bid, dsid);
            streamFieldMapper.deleteAllByIdentifier(bid, dsid);

            // 2. Delete file data source, DB data source information
            sourceFileService.deleteAllByIdentifier(bid, dsid);
            sourceDbService.deleteAllByIdentifier(bid, dsid);

            // 3. Delete data storage information
            storageService.deleteAllByIdentifier(bid, dsid);

            // 4. Save the data stream of this batch
            this.saveAll(pageInfo, operator);
        }
        LOGGER.info("success to batch save all stream page info");
        return true;
    }

    @Override
    public PageInfo<FullPageInfo> listAllWithBid(FullDataStreamPageRequest request) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to list full data stream page by {}", request);
        }
        LOGGER.debug("begin to list full data stream page by {}", request);
        Preconditions.checkNotNull(request, "request is empty");
        Preconditions.checkNotNull(request.getBid(), BizConstant.BID_IS_EMPTY);

        // 1. Query all valid data sources under bid
        String bid = request.getBid();
        // The person in charge of the business has the authority of all data streams
        BusinessEntity businessEntity = businessMapper.selectByIdentifier(bid);
        Preconditions.checkNotNull(businessEntity, "business not found by bid=" + bid);
        String inCharges = businessEntity.getInCharges();

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<DataStreamEntity> entityPage = (Page<DataStreamEntity>) streamMapper
                .selectByBidAndCondition(request, inCharges);
        List<DataStreamInfo> streamInfoList = CommonBeanUtils.copyListProperties(entityPage, DataStreamInfo::new);

        // Convert and encapsulate the paged results
        List<FullPageInfo> fullPageInfoList = new ArrayList<>(streamInfoList.size());
        for (DataStreamInfo streamInfo : streamInfoList) {
            // 2.1 Set the extended information and field information of the data stream
            String dsid = streamInfo.getDataStreamIdentifier();
            setStreamExtAndField(bid, dsid, streamInfo);

            // 2.3 Set the data stream to the result sub-object
            FullPageInfo pageInfo = new FullPageInfo();
            pageInfo.setStreamInfo(streamInfo);

            // 3. Query the basic and detailed information of the data source
            String dataSourceType = streamInfo.getDataSourceType();
            if (StringUtils.isEmpty(dataSourceType)) {
                continue;
            }
            switch (dataSourceType.toUpperCase(Locale.ROOT)) {
                case BizConstant.DATA_SOURCE_TYPE_FILE:
                    SourceFileBasicInfo fileBasicInfo = sourceFileService.getBasicByIdentifier(bid, dsid);
                    pageInfo.setFileBasicInfo(fileBasicInfo);
                    List<SourceFileDetailInfo> fileDetailInfoList = sourceFileService.listDetailByIdentifier(bid, dsid);
                    pageInfo.setFileDetailInfoList(fileDetailInfoList);
                    break;
                case BizConstant.DATA_SOURCE_TYPE_DB:
                    SourceDbBasicInfo dbBasicInfo = sourceDbService.getBasicByIdentifier(bid, dsid);
                    pageInfo.setDbBasicInfo(dbBasicInfo);
                    List<SourceDbDetailInfo> dbDetailInfoList = sourceDbService.listDetailByIdentifier(bid, dsid);
                    pageInfo.setDbDetailInfoList(dbDetailInfoList);
                    break;
                case BizConstant.DATA_SOURCE_TYPE_AUTO_PUSH:
                    break;
                default:
                    throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_TYPE_NOT_SUPPORTED);
            }

            // 4. Query various data storage and its extended information, field information
            List<BaseStorageInfo> storageInfoList = storageService.listByIdentifier(bid, dsid);
            pageInfo.setStorageInfo(storageInfoList);

            // 5. Add a single result to the paginated list
            fullPageInfoList.add(pageInfo);
        }

        PageInfo<FullPageInfo> page = new PageInfo<>(fullPageInfoList);
        page.setTotal(entityPage.getTotal());

        LOGGER.info("success to list full data stream info");
        return page;
    }

    @Override
    public List<DataStreamInfo> listAllByBid(String bid) {
        LOGGER.debug("begin to list all data stream page by bid={}", bid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);

        // Query all valid data sources under bid
        List<DataStreamEntity> entityList = streamMapper.selectByBid(bid);
        List<DataStreamInfo> streamInfoList = CommonBeanUtils.copyListProperties(entityList, DataStreamInfo::new);

        // Set the extended information and field information of the data stream
        for (DataStreamInfo streamInfo : streamInfoList) {
            String dsid = streamInfo.getDataStreamIdentifier();
            setStreamExtAndField(bid, dsid, streamInfo);
        }

        LOGGER.info("success to list all data stream page by bid={}", bid);

        return streamInfoList;
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
        // this.checkBizIsTempStatus(streamInfo.getBusinessIdentifier());

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
    public int selectCountByBid(String bid) {
        LOGGER.debug("begin bo get count by bid={}", bid);
        if (StringUtils.isEmpty(bid)) {
            return 0;
        }
        int count = streamMapper.selectCountByBid(bid);
        LOGGER.info("success to get count");
        return count;
    }

    @Override
    public boolean updateAfterApprove(List<DataStreamApproveInfo> streamApproveList, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update stream after approve={}", streamApproveList);
        }
        if (CollectionUtils.isEmpty(streamApproveList)) {
            return true;
        }

        for (DataStreamApproveInfo info : streamApproveList) {
            // Modify mqResourceObj
            DataStreamEntity streamEntity = new DataStreamEntity();
            streamEntity.setBusinessIdentifier(info.getBusinessIdentifier());
            streamEntity.setDataStreamIdentifier(info.getDataStreamIdentifier());
            streamEntity.setStatus(EntityStatus.DATA_STREAM_CONFIG_ING.getCode());
            streamMapper.updateByIdentifierSelective(streamEntity);
            // Update status to [DATA_STREAM_CONFIG_ING]
            // If you need to change data stream info after approve, just do in here

            // Modify the storage information
            storageService.updateAfterApprove(info.getStorageList(), operator);
        }

        LOGGER.info("success to update stream after approve");
        return true;
    }

    @Override
    public boolean updateStatus(String bid, String dsid, Integer status, String operator) {
        LOGGER.debug("begin to update status by bid={}, dsid={}", bid, dsid);

        // businessMapper.updateStatusByIdentifier(bid, status, operator);
        streamMapper.updateStatusByIdentifier(bid, dsid, status, operator);

        LOGGER.info("success to update stream after approve");
        return true;
    }

    /**
     * Update extended information
     * <p/>First physically delete the existing extended information, and then add this batch of extended information
     */
    @Transactional(rollbackFor = Throwable.class)
    void updateExt(String bid, String dsid, List<DataStreamExtInfo> extInfoList) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update data stream ext, bid={}, dsid={}, ext={}", bid, dsid, extInfoList);
        }

        try {
            streamExtMapper.deleteAllByIdentifier(bid, dsid);
            saveExt(bid, dsid, extInfoList, new Date());
            LOGGER.info("success to update data stream ext");
        } catch (Exception e) {
            LOGGER.error("failed to update data stream ext: ", e);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_EXT_SAVE_FAILED);
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    void saveExt(String bid, String dsid, List<DataStreamExtInfo> infoList, Date date) {
        if (CollectionUtils.isEmpty(infoList)) {
            return;
        }
        List<DataStreamExtEntity> entityList = CommonBeanUtils.copyListProperties(infoList, DataStreamExtEntity::new);
        for (DataStreamExtEntity entity : entityList) {
            entity.setBusinessIdentifier(bid);
            entity.setDataStreamIdentifier(dsid);
            entity.setModifyTime(date);
        }
        streamExtMapper.insertAll(entityList);
    }

    /**
     * Update field information
     * <p/>First physically delete the existing field information, and then add the field information of this batch
     */
    @Transactional(rollbackFor = Throwable.class)
    void updateField(String bid, String dsid, List<DataStreamFieldInfo> fieldInfoList) {
        LOGGER.debug("begin to update data stream field, bid={}, dsid={}, field={}", bid, dsid, fieldInfoList);
        try {
            streamFieldMapper.deleteAllByIdentifier(bid, dsid);
            saveField(bid, dsid, fieldInfoList);
            LOGGER.info("success to update data stream field");
        } catch (Exception e) {
            LOGGER.error("failed to update data stream field: ", e);
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_FIELD_SAVE_FAILED);
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    void saveField(String bid, String dsid, List<DataStreamFieldInfo> infoList) {
        if (CollectionUtils.isEmpty(infoList)) {
            return;
        }
        List<DataStreamFieldEntity> entities = CommonBeanUtils.copyListProperties(infoList, DataStreamFieldEntity::new);
        for (DataStreamFieldEntity entity : entities) {
            entity.setBusinessIdentifier(bid);
            entity.setDataStreamIdentifier(dsid);
        }
        streamFieldMapper.insertAll(entities);
    }

    /**
     * Check whether the business status is temporary
     *
     * @param bid Business identifier
     * @return usiness entity for caller reuse
     */
    private BusinessEntity checkBizIsTempStatus(String bid) {
        BusinessEntity businessEntity = businessMapper.selectByIdentifier(bid);
        Preconditions.checkNotNull(businessEntity, "businessIdentifier is invalid");
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
            String bid = streamInfo.getBusinessIdentifier();
            String dsid = streamInfo.getDataStreamIdentifier();
            // Whether there is an undeleted data source
            boolean dataSourceExist = hasDataSource(bid, dsid, streamInfo.getDataSourceType());
            // Whether there is undeleted data storage
            boolean dataStorageExist = hasDataStorage(bid, dsid);
            if (dataSourceExist || dataStorageExist) {
                checkUpdatedFields(streamEntity, streamInfo);
            }
        }
    }

    /**
     * Check that bid, dsid, and dataSourceType are not allowed to be modified
     */
    private void checkUpdatedFields(DataStreamEntity streamEntity, DataStreamInfo streamInfo) {
        String newBid = streamInfo.getBusinessIdentifier();
        if (newBid != null && !newBid.equals(streamEntity.getBusinessIdentifier())) {
            LOGGER.error("current status was not allowed to update business identifier");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_BID_UPDATE_NOT_ALLOWED);
        }

        String newDsid = streamInfo.getDataStreamIdentifier();
        if (newDsid != null && !newDsid.equals(streamEntity.getDataStreamIdentifier())) {
            LOGGER.error("current status was not allowed to update data stream identifier");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_ID_UPDATE_NOT_ALLOWED);
        }

        String newSourceType = streamInfo.getDataSourceType();
        if (newSourceType != null && !newSourceType.equals(streamEntity.getDataSourceType())) {
            LOGGER.error("current status was not allowed to update data source type");
            throw new BusinessException(BizErrorCodeEnum.DATA_STREAM_SOURCE_UPDATE_NOT_ALLOWED);
        }
    }

}
