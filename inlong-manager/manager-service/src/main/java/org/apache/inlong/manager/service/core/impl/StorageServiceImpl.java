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
import org.apache.inlong.manager.common.pojo.datastorage.BaseStorageInfo;
import org.apache.inlong.manager.common.pojo.datastorage.BaseStorageListVO;
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
    public Integer save(BaseStorageInfo storageInfo, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to save storage info={}", storageInfo);
        }

        Preconditions.checkNotNull(storageInfo, "storage info is empty");
        String bid = storageInfo.getBusinessIdentifier();
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        String dsid = storageInfo.getDataStreamIdentifier();
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Check if it can be added
        BusinessEntity businessEntity = super.checkBizIsTempStatus(bid);

        // According to the storage type, save storage information
        String storageType = storageInfo.getStorageType();
        Preconditions.checkNotNull(storageType, "storageType is empty");

        int id;
        if (BizConstant.STORAGE_TYPE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
            id = hiveOperation.saveHiveStorage(storageInfo, operator);
        } else {
            LOGGER.error("the storageType={} not support", storageType);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
        }

        // If the business status is [Configuration Successful], then asynchronously initiate
        // the [Single data stream Resource Creation] workflow
        if (EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode().equals(businessEntity.getStatus())) {
            super.executorService.execute(new WorkflowStartRunnable(operator, businessEntity, dsid));
        }

        LOGGER.info("success to save storage info");
        return id;
    }

    @Override
    public BaseStorageInfo getById(String storageType, Integer id) {
        LOGGER.debug("begin to get storage by storageType={}, id={}", storageType, id);
        Preconditions.checkNotNull(id, "storage id is null");
        Preconditions.checkNotNull(storageType, "storageType is empty");

        BaseStorageInfo storageInfo;
        if (BizConstant.STORAGE_TYPE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
            storageInfo = hiveOperation.getHiveStorage(id);
        } else {
            LOGGER.error("the storageType={} not support", storageType);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
        }

        LOGGER.info("success to get storage info");
        return storageInfo;
    }

    @Override
    public int getCountByIdentifier(String bid, String dsid) {
        LOGGER.debug("begin to get storage count by bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        int count = hiveStorageMapper.selectCountByIdentifier(bid, dsid);

        LOGGER.info("the storage count={} by bid={}, dsid={}", count, bid, dsid);
        return count;
    }

    @Override
    public List<BaseStorageInfo> listByIdentifier(String bid, String dsid) {
        LOGGER.debug("begin to list storage by bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);

        // Query HDFS, HIVE, ES storage information and encapsulate it in the result set
        List<BaseStorageInfo> storageInfoList = new ArrayList<>();
        hiveOperation.setHiveStorageInfo(bid, dsid, storageInfoList);

        LOGGER.info("success to list storage info");
        return storageInfoList;
    }

    @Override
    public List<StorageSummaryInfo> listSummaryByIdentifier(String bid, String dsid) {
        LOGGER.debug("begin to list storage summary by bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Query HDFS, HIVE, ES storage information and encapsulate it in the result set
        List<StorageSummaryInfo> totalList = new ArrayList<>();
        List<StorageSummaryInfo> hiveSummaryList = hiveStorageMapper.selectSummaryByIdentifier(bid, dsid);

        totalList.addAll(hiveSummaryList);

        LOGGER.info("success to list storage summary");
        return totalList;
    }

    @Override
    public PageInfo<? extends BaseStorageListVO> listByCondition(StoragePageRequest request) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to list storage page by {}", request);
        }
        Preconditions.checkNotNull(request.getBid(), BizConstant.BID_IS_EMPTY);

        String storageType = request.getStorageType();
        Preconditions.checkNotNull(storageType, "storageType is empty");

        PageInfo<? extends BaseStorageListVO> page;
        if (BizConstant.STORAGE_TYPE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
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
    public boolean update(BaseStorageInfo storageInfo, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update storage info={}", storageInfo);
        }

        Preconditions.checkNotNull(storageInfo, "storage info is empty");
        String bid = storageInfo.getBusinessIdentifier();
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        String dsid = storageInfo.getDataStreamIdentifier();
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Check if it can be modified
        BusinessEntity businessEntity = super.checkBizIsTempStatus(bid);

        String storageType = storageInfo.getStorageType();
        Preconditions.checkNotNull(storageType, "storageType is empty");

        if (BizConstant.STORAGE_TYPE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
            hiveOperation.updateHiveStorage(businessEntity.getStatus(), storageInfo, operator);
        } else {
            LOGGER.error("the storageType={} not support", storageType);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
        }

        // The business status is [Configuration successful], then asynchronously initiate
        // the [Single data stream resource creation] workflow
        if (EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode().equals(businessEntity.getStatus())) {
            super.executorService.execute(new WorkflowStartRunnable(operator, businessEntity, dsid));
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
        if (BizConstant.STORAGE_TYPE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
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
    public boolean deleteAllByIdentifier(String bid, String dsid) {
        LOGGER.debug("begin to delete all storage info by bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(bid);

        hiveOperation.deleteHiveByIdentifier(bid, dsid);

        LOGGER.info("success to delete all storage info");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean logicDeleteAllByIdentifier(String bid, String dsid, String operator) {
        LOGGER.debug("begin to logic delete all storage info by bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(bid);

        hiveOperation.logicDeleteHiveByIdentifier(bid, dsid, operator);

        LOGGER.info("success to logic delete all storage info");
        return true;
    }

    @Override
    public List<String> filterStreamIdByStorageType(String bid, String storageType, List<String> dsidList) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to filter stream by bid={}, storageType={}, dsidList={}", bid, storageType, dsidList);
        }

        List<String> resultList = new ArrayList<>();
        if (StringUtils.isEmpty(storageType) || CollectionUtils.isEmpty(dsidList)) {
            return resultList;
        }

        if (BizConstant.STORAGE_TYPE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
            resultList = hiveStorageMapper.selectDataStreamExists(bid, dsidList);
        } else {
            LOGGER.error("the storageType={} not support", storageType);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORTED);
        }

        LOGGER.info("success to filter stream");
        return resultList;
    }

    @Override
    public List<String> getStorageTypeList(String bid, String dsid) {
        LOGGER.debug("begin to get storage type list by bid={}, dsid={}", bid, dsid);

        List<String> resultList = new ArrayList<>();
        if (StringUtils.isEmpty(dsid)) {
            return resultList;
        }

        if (hiveStorageMapper.selectCountByIdentifier(bid, dsid) > 0) {
            resultList.add(BizConstant.STORAGE_TYPE_HIVE);
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

            if (BizConstant.STORAGE_TYPE_HIVE.equals(storageType.toUpperCase(Locale.ROOT))) {
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
