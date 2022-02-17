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

package org.apache.inlong.manager.service.storage.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import java.util.Date;
import java.util.function.Supplier;
import javax.validation.constraints.NotNull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.datastorage.StorageListResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;
import org.apache.inlong.manager.common.pojo.datastorage.kafka.KafkaStorageDTO;
import org.apache.inlong.manager.common.pojo.datastorage.kafka.KafkaStorageListResponse;
import org.apache.inlong.manager.common.pojo.datastorage.kafka.KafkaStorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.kafka.KafkaStorageResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StorageEntity;
import org.apache.inlong.manager.dao.mapper.StorageEntityMapper;
import org.apache.inlong.manager.service.storage.StorageOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Kafka storage operation
 */
@Service
public class KafkaStorageOperation implements StorageOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStorageOperation.class);

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private StorageEntityMapper storageMapper;

    @Override
    public Boolean accept(String storageType) {
        return BizConstant.STORAGE_KAFKA.equals(storageType);
    }

    @Override
    public Integer saveOpt(StorageRequest request, String operator) {
        String storageType = request.getStorageType();
        Preconditions.checkTrue(BizConstant.STORAGE_KAFKA.equals(storageType),
                BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORT.getMessage() + ": " + storageType);

        KafkaStorageRequest kafkaStorageRequest = (KafkaStorageRequest) request;
        StorageEntity entity = CommonBeanUtils.copyProperties(kafkaStorageRequest, StorageEntity::new);
        entity.setStatus(EntityStatus.DATA_STORAGE_NEW.getCode());
        entity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
        entity.setCreator(operator);
        entity.setModifier(operator);
        Date now = new Date();
        entity.setCreateTime(now);
        entity.setModifyTime(now);

        // get the ext params
        KafkaStorageDTO dto = KafkaStorageDTO.getFromRequest(kafkaStorageRequest);
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
        // kafka request without field list,so ignore this method.
    }

    @Override
    public StorageResponse getById(@NotNull String storageType, @NotNull Integer id) {
        StorageEntity entity = storageMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, BizErrorCodeEnum.STORAGE_INFO_NOT_FOUND.getMessage());
        String existType = entity.getStorageType();
        Preconditions.checkTrue(BizConstant.STORAGE_KAFKA.equals(existType),
                String.format(BizConstant.STORAGE_TYPE_NOT_SAME, BizConstant.STORAGE_KAFKA, existType));
        return this.getFromEntity(entity, KafkaStorageResponse::new);
    }

    @Override
    public <T> T getFromEntity(StorageEntity entity, Supplier<T> target) {
        T result = target.get();
        if (entity == null) {
            return result;
        }
        String existType = entity.getStorageType();
        Preconditions.checkTrue(BizConstant.STORAGE_KAFKA.equals(existType),
                String.format(BizConstant.STORAGE_TYPE_NOT_SAME, BizConstant.STORAGE_KAFKA, existType));

        KafkaStorageDTO dto = KafkaStorageDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, result, true);
        CommonBeanUtils.copyProperties(dto, result, true);

        return result;
    }

    @Override
    public PageInfo<? extends StorageListResponse> getPageInfo(Page<StorageEntity> entityPage) {
        if (CollectionUtils.isEmpty(entityPage)) {
            return new PageInfo<>();
        }
        return entityPage.toPageInfo(entity -> this.getFromEntity(entity, KafkaStorageListResponse::new));
    }

    @Override
    public void updateOpt(StorageRequest request, String operator) {
        String storageType = request.getStorageType();
        Preconditions.checkTrue(BizConstant.STORAGE_KAFKA.equals(storageType),
                String.format(BizConstant.STORAGE_TYPE_NOT_SAME, BizConstant.STORAGE_KAFKA, storageType));

        StorageEntity entity = storageMapper.selectByPrimaryKey(request.getId());
        Preconditions.checkNotNull(entity, BizErrorCodeEnum.STORAGE_INFO_NOT_FOUND.getMessage());
        KafkaStorageRequest kafkaStorageRequest = (KafkaStorageRequest) request;
        CommonBeanUtils.copyProperties(kafkaStorageRequest, entity, true);
        try {
            KafkaStorageDTO dto = KafkaStorageDTO.getFromRequest(kafkaStorageRequest);
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
        this.updateFieldOpt(onlyAdd, kafkaStorageRequest);

        LOGGER.info("success to update storage of type={}", storageType);
    }

    @Override
    public void updateFieldOpt(Boolean onlyAdd, StorageRequest request) {
        // kafka request without field list,so ignore this method.
    }

}
