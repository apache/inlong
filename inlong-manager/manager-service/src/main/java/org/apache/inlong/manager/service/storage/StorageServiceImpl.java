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

package org.apache.inlong.manager.service.storage;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.StorageType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageApproveDTO;
import org.apache.inlong.manager.common.pojo.datastorage.StorageBriefResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StorageListResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StoragePageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.apache.inlong.manager.dao.entity.StorageEntity;
import org.apache.inlong.manager.dao.mapper.BusinessEntityMapper;
import org.apache.inlong.manager.dao.mapper.StorageEntityMapper;
import org.apache.inlong.manager.dao.mapper.StorageFieldEntityMapper;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.apache.inlong.manager.service.workflow.business.BusinessResourceProcessForm;
import org.apache.inlong.manager.service.workflow.stream.CreateStreamWorkflowDefinition;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of storage service interface
 */
@Service
public class StorageServiceImpl implements StorageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageServiceImpl.class);
    public final ExecutorService executorService = new ThreadPoolExecutor(
            10,
            20,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(100),
            new ThreadFactoryBuilder().setNameFormat("data-stream-workflow-%s").build(),
            new CallerRunsPolicy());

    @Autowired
    private StorageOperationFactory operationFactory;
    @Autowired
    private BusinessEntityMapper businessMapper;
    @Autowired
    private StorageEntityMapper storageMapper;
    @Autowired
    private StorageFieldEntityMapper storageFieldMapper;
    @Autowired
    private WorkflowService workflowService;

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Integer save(StorageRequest request, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to save storage info=" + request);
        }
        this.checkParams(request);

        // Check if it can be added
        String groupId = request.getInlongGroupId();
        BusinessEntity businessEntity = checkBizIsTempStatus(groupId, operator);

        // Make sure that there is no storage info with the current groupId and streamId
        String streamId = request.getInlongStreamId();
        String storageType = request.getStorageType();
        List<StorageEntity> storageExist = storageMapper.selectByIdAndType(groupId, streamId, storageType);
        Preconditions.checkEmpty(storageExist, BizErrorCodeEnum.STORAGE_ALREADY_EXISTS.getMessage());

        // According to the storage type, save storage information
        StorageOperation operation = operationFactory.getInstance(StorageType.getStorageType(storageType));
        int id = operation.saveOpt(request, operator);

        // If the business status is [Configuration Successful], then asynchronously initiate
        // the [Single data stream Resource Creation] workflow
        if (EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode().equals(businessEntity.getStatus())) {
            executorService.execute(new WorkflowStartRunnable(operator, businessEntity, streamId));
        }

        LOGGER.info("success to save storage info");
        return id;
    }

    @Override
    public StorageResponse get(Integer id, String storageType) {
        LOGGER.debug("begin to get storage by id={}, storageType={}", id, storageType);
        StorageOperation operation = operationFactory.getInstance(StorageType.getStorageType(storageType));
        StorageResponse storageResponse = operation.getById(storageType, id);
        LOGGER.info("success to get storage info");
        return storageResponse;
    }

    @Override
    public Integer getCount(String groupId, String streamId) {
        Integer count = storageMapper.selectCount(groupId, streamId);
        LOGGER.debug("storage count={} with groupId={}, streamId={}", count, groupId, streamId);
        return count;
    }

    @Override
    public List<StorageResponse> listStorage(String groupId, String streamId) {
        LOGGER.debug("begin to list storage by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        List<StorageEntity> entityList = storageMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isEmpty(entityList)) {
            return Collections.emptyList();
        }
        List<StorageResponse> responseList = new ArrayList<>();
        entityList.forEach(entity -> responseList.add(this.get(entity.getId(), entity.getStorageType())));

        LOGGER.info("success to list storage");
        return responseList;
    }

    @Override
    public List<StorageBriefResponse> listBrief(String groupId, String streamId) {
        LOGGER.debug("begin to list storage summary by groupId=" + groupId + ", streamId=" + streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Query all storage information and encapsulate it in the result set
        List<StorageBriefResponse> summaryList = storageMapper.selectSummary(groupId, streamId);

        LOGGER.info("success to list storage summary");
        return summaryList;
    }

    @Override
    public PageInfo<? extends StorageListResponse> listByCondition(StoragePageRequest request) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to list storage page by " + request);
        }
        Preconditions.checkNotNull(request.getInlongGroupId(), BizConstant.GROUP_ID_IS_EMPTY);
        String storageType = request.getStorageType();
        Preconditions.checkNotNull(storageType, BizConstant.STORAGE_TYPE_IS_EMPTY);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<StorageEntity> entityPage = (Page<StorageEntity>) storageMapper.selectByCondition(request);

        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        StorageOperation operation = operationFactory.getInstance(StorageType.getStorageType(storageType));
        PageInfo<? extends StorageListResponse> pageInfo = operation.getPageInfo(entityPage);
        pageInfo.setTotal(entityPage.getTotal());

        LOGGER.info("success to list storage page");
        return pageInfo;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public boolean update(StorageRequest request, String operator) {
        LOGGER.info("begin to update storage info={}", request);
        this.checkParams(request);
        Preconditions.checkNotNull(request.getId(), BizConstant.ID_IS_EMPTY);

        // Check if it can be modified
        String groupId = request.getInlongGroupId();
        BusinessEntity businessEntity = checkBizIsTempStatus(groupId, operator);

        String streamId = request.getInlongStreamId();
        String storageType = request.getStorageType();

        StorageOperation operation = operationFactory.getInstance(StorageType.getStorageType(storageType));
        operation.updateOpt(request, operator);

        // The business status is [Configuration successful], then asynchronously initiate
        // the [Single data stream resource creation] workflow
        if (EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode().equals(businessEntity.getStatus())) {
            executorService.execute(new WorkflowStartRunnable(operator, businessEntity, streamId));
        }
        LOGGER.info("success to update storage info");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean delete(Integer id, String storageType, String operator) {
        LOGGER.info("begin to delete storage by id={}, storageType={}", id, storageType);
        Preconditions.checkNotNull(id, BizConstant.ID_IS_EMPTY);
        // Preconditions.checkNotNull(storageType, BizConstant.STORAGE_TYPE_IS_EMPTY);

        StorageEntity entity = storageMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, BizErrorCodeEnum.STORAGE_INFO_NOT_FOUND.getMessage());
        checkBizIsTempStatus(entity.getInlongGroupId(), operator);

        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(EntityStatus.DELETED.getCode());
        entity.setIsDeleted(id);
        entity.setModifier(operator);
        entity.setModifyTime(new Date());
        storageMapper.updateByPrimaryKeySelective(entity);

        storageFieldMapper.logicDeleteAll(id);

        LOGGER.info("success to delete storage info");
        return true;
    }

    @Override
    public void updateStatus(int id, int status, String log) {
        StorageEntity entity = new StorageEntity();
        entity.setId(id);
        entity.setStatus(status);
        entity.setOperateLog(log);
        storageMapper.updateStorageStatus(entity);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public boolean logicDeleteAll(String groupId, String streamId, String operator) {
        LOGGER.info("begin to logic delete all storage info by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(groupId, operator);

        Date now = new Date();
        List<StorageEntity> entityList = storageMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isNotEmpty(entityList)) {
            entityList.forEach(entity -> {
                Integer id = entity.getId();
                entity.setPreviousStatus(entity.getStatus());
                entity.setStatus(EntityStatus.DELETED.getCode());
                entity.setIsDeleted(id);
                entity.setModifier(operator);
                entity.setModifyTime(now);

                storageMapper.deleteByPrimaryKey(id);
                storageFieldMapper.logicDeleteAll(id);
            });
        }

        LOGGER.info("success to logic delete all storage by groupId={}, streamId={}", groupId, streamId);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public boolean deleteAll(String groupId, String streamId, String operator) {
        LOGGER.info("begin to delete all storage by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(groupId, operator);

        List<StorageEntity> entityList = storageMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isNotEmpty(entityList)) {
            entityList.forEach(entity -> {
                storageMapper.deleteByPrimaryKey(entity.getId());
                storageFieldMapper.deleteAll(entity.getId());
            });
        }

        LOGGER.info("success to delete all storage by groupId={}, streamId={}", groupId, streamId);
        return true;
    }

    @Override
    public List<String> getExistsStreamIdList(String groupId, String storageType, List<String> streamIdList) {
        LOGGER.debug("begin to filter stream by groupId={}, type={}, streamId={}", groupId, storageType, streamIdList);
        if (StringUtils.isEmpty(storageType) || CollectionUtils.isEmpty(streamIdList)) {
            return Collections.emptyList();
        }

        List<String> resultList = storageMapper.selectExistsStreamId(groupId, storageType, streamIdList);
        LOGGER.debug("success to filter stream id list, result streamId={}", resultList);
        return resultList;
    }

    @Override
    public List<String> getStorageTypeList(String groupId, String streamId) {
        LOGGER.debug("begin to get storage type list by groupId={}, streamId={}", groupId, streamId);
        if (StringUtils.isEmpty(streamId)) {
            return Collections.emptyList();
        }

        List<String> resultList = storageMapper.selectStorageType(groupId, streamId);
        LOGGER.debug("success to get storage type list, result storageType={}", resultList);
        return resultList;
    }

    @Override
    public boolean updateAfterApprove(List<StorageApproveDTO> approveList, String operator) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to update storage after approve={}", approveList);
        }
        if (CollectionUtils.isEmpty(approveList)) {
            return true;
        }

        Date now = new Date();
        for (StorageApproveDTO dto : approveList) {
            // According to the storage type, save storage information
            String storageType = dto.getStorageType();
            Preconditions.checkNotNull(storageType, BizConstant.STORAGE_TYPE_IS_EMPTY);

            StorageEntity entity = new StorageEntity();
            entity.setId(dto.getId());

            int status = (dto.getStatus() == null) ? EntityStatus.DATA_STORAGE_CONFIG_ING.getCode() : dto.getStatus();
            entity.setPreviousStatus(entity.getStatus());
            entity.setStatus(status);
            entity.setModifier(operator);
            entity.setModifyTime(now);
            storageMapper.updateByPrimaryKeySelective(entity);
        }

        LOGGER.info("success to update storage after approve");
        return true;
    }

    /**
     * heck whether the business status is temporary
     *
     * @param groupId Business group id
     * @return Business entity, For caller reuse
     */
    public BusinessEntity checkBizIsTempStatus(String groupId, String operator) {
        BusinessEntity businessEntity = businessMapper.selectByIdentifier(groupId);
        Preconditions.checkNotNull(businessEntity, "groupId is invalid");

        List<String> managers = Arrays.asList(businessEntity.getInCharges().split(","));
        Preconditions.checkTrue(managers.contains(operator),
                String.format(BizErrorCodeEnum.USER_IS_NOT_MANAGER.getMessage(), operator, managers));

        // Add/modify/delete is not allowed under certain group status
        if (EntityStatus.BIZ_TEMP_STATUS.contains(businessEntity.getStatus())) {
            LOGGER.error("business status was not allowed to add/update/delete data storage");
            throw new BusinessException(BizErrorCodeEnum.STORAGE_OPT_NOT_ALLOWED);
        }

        return businessEntity;
    }

    private void checkParams(StorageRequest request) {
        Preconditions.checkNotNull(request, BizConstant.REQUEST_IS_EMPTY);
        String groupId = request.getInlongGroupId();
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        String streamId = request.getInlongStreamId();
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);
        String storageType = request.getStorageType();
        Preconditions.checkNotNull(storageType, BizConstant.STORAGE_TYPE_IS_EMPTY);
    }

    /**
     * Asynchronously initiate a single data stream related workflow
     *
     * @see CreateStreamWorkflowDefinition
     */
    class WorkflowStartRunnable implements Runnable {

        private final String operator;
        private final BusinessEntity businessEntity;
        private final String streamId;

        public WorkflowStartRunnable(String operator, BusinessEntity businessEntity, String streamId) {
            this.operator = operator;
            this.businessEntity = businessEntity;
            this.streamId = streamId;
        }

        @Override
        public void run() {
            String groupId = businessEntity.getInlongGroupId();
            LOGGER.info("begin start data stream workflow, groupId={}, streamId={}", groupId, streamId);

            BusinessInfo businessInfo = CommonBeanUtils.copyProperties(businessEntity, BusinessInfo::new);
            BusinessResourceProcessForm form = genBizResourceWorkflowForm(businessInfo, streamId);

            workflowService.start(ProcessName.CREATE_DATASTREAM_RESOURCE, operator, form);
            LOGGER.info("success start data stream workflow, groupId={}, streamId={}", groupId, streamId);
        }

        /**
         * Generate [Create Business Resource] form
         */
        private BusinessResourceProcessForm genBizResourceWorkflowForm(BusinessInfo businessInfo, String streamId) {
            BusinessResourceProcessForm form = new BusinessResourceProcessForm();
            form.setBusinessInfo(businessInfo);
            form.setInlongStreamId(streamId);
            return form;
        }
    }

}
