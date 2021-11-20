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
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.datastorage.BaseStorageInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageExtInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageHiveFieldInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageHiveInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageHiveListVO;
import org.apache.inlong.manager.common.pojo.datastorage.StoragePageRequest;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataStreamEntity;
import org.apache.inlong.manager.dao.entity.StorageExtEntity;
import org.apache.inlong.manager.dao.entity.StorageHiveEntity;
import org.apache.inlong.manager.dao.entity.StorageHiveFieldEntity;
import org.apache.inlong.manager.dao.mapper.DataStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StorageExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.StorageHiveEntityMapper;
import org.apache.inlong.manager.dao.mapper.StorageHiveFieldEntityMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Data is stored in the operation class of HIVE
 */
@Service
public class StorageHiveOperation extends StorageBaseOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageHiveOperation.class);

    @Autowired
    private StorageHiveEntityMapper hiveStorageMapper;
    @Autowired
    private StorageExtEntityMapper storageExtMapper;
    @Autowired
    private StorageHiveFieldEntityMapper hiveFieldMapper;
    @Autowired
    private DataStreamEntityMapper dataStreamMapper;

    /**
     * Save HIVE storage information
     *
     * @param storageInfo Storage i formation
     * @return Id after saving
     */
    public int saveHiveStorage(BaseStorageInfo storageInfo, String operator) {
        String groupId = storageInfo.getInlongGroupId();
        // Make sure that there is no HIVE storage information under the current groupId and streamId
        // (the two are mutually exclusive, only one can exist)
        List<StorageHiveEntity> storageExist = hiveStorageMapper
                .selectByIdentifier(groupId, storageInfo.getInlongStreamId());
        Preconditions.checkEmpty(storageExist, "HIVE storage already exist under the groupId and streamId");

        StorageHiveInfo hiveInfo = (StorageHiveInfo) storageInfo;
        StorageHiveEntity entity = CommonBeanUtils.copyProperties(hiveInfo, StorageHiveEntity::new);

        // Set the encoding type and field splitter
        DataStreamEntity streamEntity = dataStreamMapper.selectByIdentifier(groupId, entity.getInlongStreamId());
        String encodingType = streamEntity.getDataEncoding() == null
                ? StandardCharsets.UTF_8.displayName() : streamEntity.getDataEncoding();
        entity.setEncodingType(encodingType);
        if (entity.getFieldSplitter() == null) {
            entity.setFieldSplitter(streamEntity.getFileDelimiter());
        }

        entity.setStatus(EntityStatus.DATA_STORAGE_NEW.getCode());
        entity.setCreator(operator);
        entity.setModifier(operator);
        entity.setCreateTime(new Date());
        hiveStorageMapper.insertSelective(entity);

        int id = entity.getId();
        // Save field information
        this.saveHiveFieldOpt(id, hiveInfo.getHiveFieldList());
        // Save extended information
        String storageType = BizConstant.STORAGE_HIVE;
        this.saveExtOpt(storageType, id, hiveInfo.getExtList());

        return id;
    }

    /**
     * According to groupId and streamId, query the HIVE storage information to which it belongs
     */
    public void setHiveStorageInfo(String groupId, String streamId, List<BaseStorageInfo> storageInfoList) {
        List<StorageHiveEntity> hiveEntities = hiveStorageMapper.selectByIdentifier(groupId, streamId);

        if (CollectionUtils.isEmpty(hiveEntities)) {
            return;
        }

        // Get extended information and field information, and encapsulate it in the result list
        for (StorageHiveEntity hiveEntity : hiveEntities) {
            Integer storageId = hiveEntity.getId();

            String storageType;
            List<StorageExtEntity> extEntities;

            storageType = BizConstant.STORAGE_HIVE;
            extEntities = storageExtMapper.selectByStorageTypeAndId(BizConstant.STORAGE_HIVE, storageId);

            List<StorageHiveFieldEntity> fieldEntityList = hiveFieldMapper.selectByStorageId(storageId);
            List<StorageHiveFieldInfo> fieldInfoList = CommonBeanUtils
                    .copyListProperties(fieldEntityList, StorageHiveFieldInfo::new);

            StorageHiveInfo hiveInfo = CommonBeanUtils.copyProperties(hiveEntity, StorageHiveInfo::new);
            hiveInfo.setStorageType(storageType);
            hiveInfo.setExtList(CommonBeanUtils.copyListProperties(extEntities, StorageExtInfo::new));
            hiveInfo.setHiveFieldList(fieldInfoList);
            storageInfoList.add(hiveInfo);
        }
    }

    /**
     * Logically delete HIVE storage information based on business group id and data stream id
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @param operator Operator
     * @return Whether succeed
     */
    public boolean logicDeleteHiveByIdentifier(String groupId, String streamId, String operator) {
        List<StorageHiveEntity> hiveEntityList = hiveStorageMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isNotEmpty(hiveEntityList)) {
            hiveEntityList.forEach(entity -> {
                entity.setIsDeleted(EntityStatus.IS_DELETED.getCode());
                entity.setPreviousStatus(entity.getStatus());
                entity.setStatus(EntityStatus.DELETED.getCode());
                entity.setModifier(operator);
                hiveStorageMapper.updateByPrimaryKey(entity);

                // Logical deletion of extended information, field information
                storageExtMapper.logicDeleteAll(entity.getId());
                hiveFieldMapper.logicDeleteAll(entity.getId());
            });
        }

        return true;
    }

    /**
     * Logically delete Hive storage information based on the primary key
     * <p/>The business status is [Configuration successful], then asynchronously initiate the
     * Single data stream resource creation workflow
     *
     * @param id Storage ID
     * @param operator Operator
     * @return Whether succeed
     */
    public boolean logicDeleteHiveStorage(Integer id, String operator) {
        StorageHiveEntity entity = hiveStorageMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("hive storage not found by id={}, delete failed", id);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_INFO_NOT_FOUND);
        }

        super.checkBizIsTempStatus(entity.getInlongGroupId());

        entity.setIsDeleted(EntityStatus.IS_DELETED.getCode());
        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(EntityStatus.DELETED.getCode());
        entity.setModifier(operator);
        int resultCount = hiveStorageMapper.updateByPrimaryKey(entity);

        // Logical deletion of extended information, field information
        storageExtMapper.logicDeleteAll(id);
        hiveFieldMapper.logicDeleteAll(id);

        return resultCount >= 0;
    }

    /**
     * Physically delete HIVE storage information of the specified id
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @return Whether succeed
     */
    public boolean deleteHiveByIdentifier(String groupId, String streamId) {
        List<StorageHiveEntity> storageHiveEntities = hiveStorageMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isNotEmpty(storageHiveEntities)) {
            storageHiveEntities.forEach(entity -> {
                hiveStorageMapper.deleteByPrimaryKey(entity.getId());
                hiveFieldMapper.deleteAllByStorageId(entity.getId());
            });
        }
        return true;
    }

    /**
     * Query HIVE storage information based on ID
     *
     * @param id Storage ID
     * @return Storage information
     */
    public BaseStorageInfo getHiveStorage(Integer id) {
        StorageHiveEntity entity = hiveStorageMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("hive storage not found by id={}", id);
            return null;
        }

        StorageHiveInfo hiveInfo = CommonBeanUtils.copyProperties(entity, StorageHiveInfo::new);
        String storageType = BizConstant.STORAGE_HIVE;
        List<StorageExtEntity> extEntityList = storageExtMapper.selectByStorageTypeAndId(storageType, id);
        List<StorageExtInfo> extInfoList = CommonBeanUtils.copyListProperties(extEntityList, StorageExtInfo::new);
        hiveInfo.setExtList(extInfoList);

        List<StorageHiveFieldEntity> entities = hiveFieldMapper.selectByStorageId(id);
        List<StorageHiveFieldInfo> infos = CommonBeanUtils.copyListProperties(entities, StorageHiveFieldInfo::new);
        hiveInfo.setHiveFieldList(infos);

        return hiveInfo;
    }

    /**
     * Query the storage list of HIVE according to conditions
     *
     * @param request Query conditions
     * @return Store the paged results of the list
     */
    public PageInfo<StorageHiveListVO> getHiveStorageList(StoragePageRequest request) {
        LOGGER.info("begin to list hive storage page by {}", request);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<StorageHiveEntity> entityPage = (Page<StorageHiveEntity>) hiveStorageMapper.selectByCondition(request);
        List<StorageHiveListVO> detailList = CommonBeanUtils.copyListProperties(entityPage, StorageHiveListVO::new);

        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<StorageHiveListVO> page = new PageInfo<>(detailList);
        page.setTotal(entityPage.getTotal());

        LOGGER.info("success to list hive storage");
        return page;
    }

    /**
     * Update Hive storage information
     *
     * @param bizStatus Business status, used to determine whether the field information can be modified
     * @param storageInfo Storage information
     * @param operator Operator
     * @return Updated id
     */
    public Integer updateHiveStorage(Integer bizStatus, BaseStorageInfo storageInfo, String operator) {
        StorageHiveInfo hiveInfo = (StorageHiveInfo) storageInfo;
        // id exists, update, otherwise add
        Integer id = hiveInfo.getId();
        if (id != null) {
            StorageHiveEntity entity = hiveStorageMapper.selectByPrimaryKey(hiveInfo.getId());
            if (entity == null) {
                LOGGER.error("hive storage not found by id={}, update failed", id);
                throw new BusinessException(BizErrorCodeEnum.STORAGE_INFO_NOT_FOUND);
            }
            BeanUtils.copyProperties(hiveInfo, entity);
            entity.setStatus(EntityStatus.BIZ_CONFIG_ING.getCode());
            entity.setModifier(operator);
            hiveStorageMapper.updateByPrimaryKeySelective(entity);

            super.updateExtOpt(hiveInfo.getStorageType(), id, hiveInfo.getExtList());
            this.updateHiveFieldOpt(bizStatus, id, hiveInfo.getHiveFieldList());
        } else {
            id = this.saveHiveStorage(hiveInfo, operator);
        }

        return id;
    }

    /**
     * Update Hive field
     * <p/>First physically delete the existing field information, and then add the field information of this batch
     */
    private void updateHiveFieldOpt(Integer bizStatus, Integer storageId, List<StorageHiveFieldInfo> fieldInfoList) {
        if (CollectionUtils.isEmpty(fieldInfoList)) {
            return;
        }
        LOGGER.info("begin to update hive field={}", fieldInfoList);

        // When the business status is [Configuration successful], modification and deletion are not allowed,
        // only adding is allowed, and the order of existing fields cannot be changed
        if (EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode().equals(bizStatus)) {
            List<StorageHiveFieldEntity> existsFieldList = hiveFieldMapper.selectByStorageId(storageId);
            if (existsFieldList.size() > fieldInfoList.size()) {
                LOGGER.error("current status was not allowed to update hive field");
                throw new BusinessException(BizErrorCodeEnum.STORAGE_HIVE_FIELD_UPDATE_NOT_ALLOWED);
            }
            for (int i = 0; i < existsFieldList.size(); i++) {
                if (!existsFieldList.get(i).getFieldName().equals(fieldInfoList.get(i).getFieldName())) {
                    LOGGER.error("current status was not allowed to update hive field");
                    throw new BusinessException(BizErrorCodeEnum.STORAGE_HIVE_FIELD_UPDATE_NOT_ALLOWED);
                }
            }
        }

        try {
            hiveFieldMapper.deleteAllByStorageId(storageId);
            saveHiveFieldOpt(storageId, fieldInfoList);
            LOGGER.info("success to update hive field");
        } catch (Exception e) {
            LOGGER.error("failed to update hive field: ", e);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_HIVE_FIELD_SAVE_FAILED);
        }
    }

    /**
     * Save HIVE field
     *
     * @param storageId Primary key for storing information
     * @param hiveFieldList Table field
     */
    private void saveHiveFieldOpt(int storageId, List<StorageHiveFieldInfo> hiveFieldList) {
        if (CollectionUtils.isEmpty(hiveFieldList)) {
            return;
        }
        LOGGER.info("begin to save hive field={}", hiveFieldList);
        for (StorageHiveFieldInfo fieldInfo : hiveFieldList) {
            StorageHiveFieldEntity fieldEntity = CommonBeanUtils.copyProperties(fieldInfo, StorageHiveFieldEntity::new);
            if (StringUtils.isEmpty(fieldEntity.getFieldComment())) {
                fieldEntity.setFieldComment(fieldEntity.getFieldName());
            }
            fieldEntity.setStorageId(storageId);
            hiveFieldMapper.insert(fieldEntity);
        }
        LOGGER.info("success to save hive field");
    }

}
