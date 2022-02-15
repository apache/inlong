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

package org.apache.inlong.manager.service.storage.hive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.datastorage.StorageFieldRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageFieldResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StorageListResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;
import org.apache.inlong.manager.common.pojo.datastorage.hive.HiveStorageDTO;
import org.apache.inlong.manager.common.pojo.datastorage.hive.HiveStorageListResponse;
import org.apache.inlong.manager.common.pojo.datastorage.hive.HiveStorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.hive.HiveStorageResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StorageEntity;
import org.apache.inlong.manager.dao.entity.StorageFieldEntity;
import org.apache.inlong.manager.dao.mapper.StorageEntityMapper;
import org.apache.inlong.manager.dao.mapper.StorageFieldEntityMapper;
import org.apache.inlong.manager.service.storage.StorageOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

/**
 * Hive storage operation
 */
@Service
public class HiveStorageOperation implements StorageOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveStorageOperation.class);

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private StorageEntityMapper storageMapper;
    @Autowired
    private StorageFieldEntityMapper storageFieldMapper;

    @Override
    public Boolean accept(String storageType) {
        return BizConstant.STORAGE_HIVE.equals(storageType);
    }

    @Override
    public Integer saveOpt(StorageRequest request, String operator) {
        String storageType = request.getStorageType();
        Preconditions.checkTrue(BizConstant.STORAGE_HIVE.equals(storageType),
                BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORT.getMessage() + ": " + storageType);

        HiveStorageRequest hiveRequest = (HiveStorageRequest) request;
        StorageEntity entity = CommonBeanUtils.copyProperties(hiveRequest, StorageEntity::new);
        entity.setStatus(EntityStatus.DATA_STORAGE_NEW.getCode());
        entity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
        entity.setCreator(operator);
        entity.setModifier(operator);
        Date now = new Date();
        entity.setCreateTime(now);
        entity.setModifyTime(now);

        // get the ext params
        HiveStorageDTO dto = HiveStorageDTO.getFromRequest(hiveRequest);
        try {
            entity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(BizErrorCodeEnum.STORAGE_SAVE_FAILED);
        }
        storageMapper.insert(entity);

        Integer storageId = entity.getId();
        request.setId(storageId);
        this.saveFieldOpt(request);

        return storageId;
    }

    @Override
    public void saveFieldOpt(StorageRequest request) {
        List<StorageFieldRequest> fieldList = request.getFieldList();
        LOGGER.info("begin to save field={}", fieldList);
        if (CollectionUtils.isEmpty(fieldList)) {
            return;
        }

        int size = fieldList.size();
        List<StorageFieldEntity> entityList = new ArrayList<>(size);
        String groupId = request.getInlongGroupId();
        String streamId = request.getInlongStreamId();
        String storageType = request.getStorageType();
        Integer storageId = request.getId();
        for (StorageFieldRequest fieldInfo : fieldList) {
            StorageFieldEntity fieldEntity = CommonBeanUtils.copyProperties(fieldInfo, StorageFieldEntity::new);
            if (StringUtils.isEmpty(fieldEntity.getFieldComment())) {
                fieldEntity.setFieldComment(fieldEntity.getFieldName());
            }
            fieldEntity.setInlongGroupId(groupId);
            fieldEntity.setInlongStreamId(streamId);
            fieldEntity.setStorageType(storageType);
            fieldEntity.setStorageId(storageId);
            fieldEntity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
            entityList.add(fieldEntity);
        }

        storageFieldMapper.insertAll(entityList);
        LOGGER.info("success to save hive field");
    }

    @Override
    public StorageResponse getById(@NotNull String storageType, @NotNull Integer id) {
        StorageEntity entity = storageMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, BizErrorCodeEnum.STORAGE_INFO_NOT_FOUND.getMessage());
        String existType = entity.getStorageType();
        Preconditions.checkTrue(BizConstant.STORAGE_HIVE.equals(existType),
                String.format(BizConstant.STORAGE_TYPE_NOT_SAME, BizConstant.STORAGE_HIVE, existType));

        StorageResponse response = this.getFromEntity(entity, HiveStorageResponse::new);
        List<StorageFieldEntity> entities = storageFieldMapper.selectByStorageId(id);
        List<StorageFieldResponse> infos = CommonBeanUtils.copyListProperties(entities,
                StorageFieldResponse::new);
        response.setFieldList(infos);

        return response;
    }

    @Override
    public <T> T getFromEntity(StorageEntity entity, Supplier<T> target) {
        T result = target.get();
        if (entity == null) {
            return result;
        }

        String existType = entity.getStorageType();
        Preconditions.checkTrue(BizConstant.STORAGE_HIVE.equals(existType),
                String.format(BizConstant.STORAGE_TYPE_NOT_SAME, BizConstant.STORAGE_HIVE, existType));

        HiveStorageDTO dto = HiveStorageDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, result, true);
        CommonBeanUtils.copyProperties(dto, result, true);

        return result;
    }

    @Override
    public PageInfo<? extends StorageListResponse> getPageInfo(Page<StorageEntity> entityPage) {
        if (CollectionUtils.isEmpty(entityPage)) {
            return new PageInfo<>();
        }
        return entityPage.toPageInfo(entity -> this.getFromEntity(entity, HiveStorageListResponse::new));
    }

    @Override
    public void updateOpt(StorageRequest request, String operator) {
        String storageType = request.getStorageType();
        Preconditions.checkTrue(BizConstant.STORAGE_HIVE.equals(storageType),
                String.format(BizConstant.STORAGE_TYPE_NOT_SAME, BizConstant.STORAGE_HIVE, storageType));

        StorageEntity entity = storageMapper.selectByPrimaryKey(request.getId());
        Preconditions.checkNotNull(entity, BizErrorCodeEnum.STORAGE_INFO_NOT_FOUND.getMessage());
        HiveStorageRequest hiveRequest = (HiveStorageRequest) request;
        CommonBeanUtils.copyProperties(hiveRequest, entity, true);
        try {
            HiveStorageDTO dto = HiveStorageDTO.getFromRequest(hiveRequest);
            entity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(BizErrorCodeEnum.STORAGE_INFO_INCORRECT.getMessage());
        }

        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(EntityStatus.BIZ_CONFIG_ING.getCode());
        entity.setModifier(operator);
        entity.setModifyTime(new Date());
        storageMapper.updateByPrimaryKeySelective(entity);

        boolean onlyAdd = EntityStatus.DATA_STORAGE_CONFIG_SUCCESSFUL.getCode().equals(entity.getPreviousStatus());
        this.updateFieldOpt(onlyAdd, hiveRequest);

        LOGGER.info("success to update storage of type={}", storageType);
    }

    @Override
    public void updateFieldOpt(Boolean onlyAdd, StorageRequest request) {
        Integer storageId = request.getId();
        List<StorageFieldRequest> fieldRequestList = request.getFieldList();
        if (CollectionUtils.isEmpty(fieldRequestList)) {
            return;
        }

        if (onlyAdd) {
            List<StorageFieldEntity> existsFieldList = storageFieldMapper.selectByStorageId(storageId);
            if (existsFieldList.size() > fieldRequestList.size()) {
                throw new BusinessException(BizErrorCodeEnum.STORAGE_FIELD_UPDATE_NOT_ALLOWED);
            }
            for (int i = 0; i < existsFieldList.size(); i++) {
                if (!existsFieldList.get(i).getFieldName().equals(fieldRequestList.get(i).getFieldName())) {
                    throw new BusinessException(BizErrorCodeEnum.STORAGE_FIELD_UPDATE_NOT_ALLOWED);
                }
            }
        }

        // First physically delete the existing fields
        storageFieldMapper.deleteAll(storageId);
        // Then batch save the storage fields
        this.saveFieldOpt(request);

        LOGGER.info("success to update field");
    }

}
