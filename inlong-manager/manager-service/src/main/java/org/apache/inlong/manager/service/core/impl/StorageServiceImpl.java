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

import com.github.pagehelper.PageInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.datastorage.BaseStorageListResponse;
import org.apache.inlong.manager.common.pojo.datastorage.BaseStorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.BaseStorageResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StorageApproveInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StoragePageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageSummaryInfo;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.apache.inlong.manager.dao.entity.StorageHiveEntity;
import org.apache.inlong.manager.dao.mapper.StorageHiveEntityMapper;
import org.apache.inlong.manager.service.core.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Implementation of service layer interface for data storage
 */
@Service
public class StorageServiceImpl extends StorageBaseOperation implements StorageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageServiceImpl.class);

    @Autowired
    private StorageHiveOperation hiveOperation;

    @Autowired
    private StorageHiveEntityMapper hiveStorageMapper;

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Integer save(BaseStorageRequest storageInfo, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to save storage info={}", storageInfo);
        }

        Preconditions.checkNotNull(storageInfo, "storage info is empty");
        String groupId = storageInfo.getInlongGroupId();
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        String streamId = storageInfo.getInlongStreamId();
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be added
        BusinessEntity businessEntity = super.checkBizIsTempStatus(groupId);

        // According to the storage type, save storage information
        String storageType = storageInfo.getStorageType();
        Preconditions.checkNotNull(storageType, "storageType is empty");

        int id;
        if (BizConstant.STORAGE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
            id = hiveOperation.saveHiveStorage(storageInfo, operator);
        } else {
            LOGGER.error("the storageType={} not support", storageType);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
        }

        // If the business status is [Configuration Successful], then asynchronously initiate
        // the [Single data stream Resource Creation] workflow
        if (EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode().equals(businessEntity.getStatus())) {
            super.executorService.execute(new WorkflowStartRunnable(operator, businessEntity, streamId));
        }

        LOGGER.info("success to save storage info");
        return id;
    }

    @Override
    public BaseStorageResponse getById(String storageType, Integer id) {
        LOGGER.debug("begin to get storage by storageType={}, id={}", storageType, id);
        Preconditions.checkNotNull(id, "storage id is null");
        Preconditions.checkNotNull(storageType, "storageType is empty");

        BaseStorageResponse storageInfo;
        if (BizConstant.STORAGE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
            storageInfo = hiveOperation.getHiveStorage(id);
        } else {
            LOGGER.error("the storageType={} not support", storageType);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
        }

        LOGGER.info("success to get storage info");
        return storageInfo;
    }

    @Override
    public Integer getCountByIdentifier(String groupId, String streamId) {
        LOGGER.debug("begin to get storage count by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        Integer count = hiveStorageMapper.selectCountByIdentifier(groupId, streamId);

        LOGGER.info("the storage count={} by groupId={}, streamId={}", count, groupId, streamId);
        return count;
    }

    @Override
    public List<BaseStorageResponse> listByIdentifier(String groupId, String streamId) {
        LOGGER.debug("begin to list storage by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        // Query HDFS, HIVE, ES storage information and encapsulate it in the result set
        List<BaseStorageResponse> responseList = new ArrayList<>();
        hiveOperation.setHiveStorageResponse(groupId, streamId, responseList);

        LOGGER.info("success to list storage info");
        return responseList;
    }

    @Override
    public List<StorageSummaryInfo> listSummaryByIdentifier(String groupId, String streamId) {
        LOGGER.debug("begin to list storage summary by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Query HDFS, HIVE, ES storage information and encapsulate it in the result set
        List<StorageSummaryInfo> totalList = new ArrayList<>();
        List<StorageSummaryInfo> hiveSummaryList = hiveStorageMapper.selectSummary(groupId, streamId);

        totalList.addAll(hiveSummaryList);

        LOGGER.info("success to list storage summary");
        return totalList;
    }

    @Override
    public PageInfo<? extends BaseStorageListResponse> listByCondition(StoragePageRequest request) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to list storage page by {}", request);
        }
        Preconditions.checkNotNull(request.getInlongGroupId(), BizConstant.GROUP_ID_IS_EMPTY);

        String storageType = request.getStorageType();
        Preconditions.checkNotNull(storageType, "storageType is empty");

        PageInfo<? extends BaseStorageListResponse> page;
        if (BizConstant.STORAGE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
            page = hiveOperation.getHiveStorageList(request);
        } else {
            LOGGER.error("the storageType={} not support", storageType);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
        }

        LOGGER.info("success to list storage page");
        return page;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean update(BaseStorageRequest storageRequest, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update storage info={}", storageRequest);
        }

        Preconditions.checkNotNull(storageRequest, "storage info is empty");
        String groupId = storageRequest.getInlongGroupId();
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        String streamId = storageRequest.getInlongStreamId();
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be modified
        BusinessEntity businessEntity = super.checkBizIsTempStatus(groupId);

        String storageType = storageRequest.getStorageType();
        Preconditions.checkNotNull(storageType, "storageType is empty");

        if (BizConstant.STORAGE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
            hiveOperation.updateHiveStorage(businessEntity.getStatus(), storageRequest, operator);
        } else {
            LOGGER.error("the storageType={} not support", storageType);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
        }

        // The business status is [Configuration successful], then asynchronously initiate
        // the [Single data stream resource creation] workflow
        if (EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode().equals(businessEntity.getStatus())) {
            super.executorService.execute(new WorkflowStartRunnable(operator, businessEntity, streamId));
        }
        LOGGER.info("success to update storage info");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean delete(String storageType, Integer id, String operator) {
        LOGGER.debug("begin to delete storage by storageType={}, id={}", storageType, id);
        Preconditions.checkNotNull(id, "storage id is null");
        Preconditions.checkNotNull(storageType, "storageType is empty");

        boolean result;
        if (BizConstant.STORAGE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
            result = hiveOperation.logicDeleteHiveStorage(id, operator);
        } else {
            LOGGER.error("the storageType={} not support", storageType);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
        }

        LOGGER.info("success to delete storage info");
        return result;
    }

    @Override
    public void updateHiveStatusById(int id, int status, String log) {
        StorageHiveEntity entity = new StorageHiveEntity();
        entity.setId(id);
        entity.setStatus(status);
        entity.setOptLog(log);
        hiveStorageMapper.updateStorageStatusById(entity);
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean deleteAllByIdentifier(String groupId, String streamId) {
        LOGGER.debug("begin to delete all storage info by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(groupId);

        hiveOperation.deleteHiveByIdentifier(groupId, streamId);

        LOGGER.info("success to delete all storage info");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean logicDeleteAllByIdentifier(String groupId, String streamId, String operator) {
        LOGGER.debug("begin to logic delete all storage info by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(groupId);

        hiveOperation.logicDeleteHiveByIdentifier(groupId, streamId, operator);

        LOGGER.info("success to logic delete all storage info");
        return true;
    }

    @Override
    public List<String> filterStreamIdByStorageType(String groupId, String storageType, List<String> streamIdList) {
        LOGGER.debug("begin to filter stream by groupId={}, type={}, streamId={}", groupId, storageType, streamIdList);

        List<String> resultList = new ArrayList<>();
        if (StringUtils.isEmpty(storageType) || CollectionUtils.isEmpty(streamIdList)) {
            return resultList;
        }

        if (BizConstant.STORAGE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
            resultList = hiveStorageMapper.selectDataStreamExists(groupId, streamIdList);
        } else {
            LOGGER.error("the storageType={} not support", storageType);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
        }

        LOGGER.info("success to filter stream");
        return resultList;
    }

    @Override
    public List<String> getStorageTypeList(String groupId, String streamId) {
        LOGGER.debug("begin to get storage type list by groupId={}, streamId={}", groupId, streamId);

        List<String> resultList = new ArrayList<>();
        if (StringUtils.isEmpty(streamId)) {
            return resultList;
        }

        if (hiveStorageMapper.selectCountByIdentifier(groupId, streamId) > 0) {
            resultList.add(BizConstant.STORAGE_HIVE);
        }

        LOGGER.info("success to get storage type list");
        return resultList;
    }

    @Override
    public boolean updateAfterApprove(List<StorageApproveInfo> storageApproveList, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update storage after approve={}", storageApproveList);
        }
        if (CollectionUtils.isEmpty(storageApproveList)) {
            return true;
        }

        for (StorageApproveInfo info : storageApproveList) {
            // According to the storage type, save storage information
            String storageType = info.getStorageType();
            Preconditions.checkNotNull(storageType, "storageType is empty");

            if (BizConstant.STORAGE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
                StorageHiveEntity hiveEntity = new StorageHiveEntity();
                hiveEntity.setId(info.getId());
                hiveEntity.setModifier(operator);
                hiveEntity.setStatus(EntityStatus.DATA_STORAGE_CONFIG_ING.getCode());
                hiveStorageMapper.updateByPrimaryKeySelective(hiveEntity);
            } else {
                LOGGER.error("the storageType={} not support", storageType);
                throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
            }
        }

        LOGGER.info("success to update storage after approve");
        return true;
    }

}
