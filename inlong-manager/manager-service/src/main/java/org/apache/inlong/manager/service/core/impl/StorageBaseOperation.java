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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageExtInfo;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.apache.inlong.manager.dao.entity.StorageExtEntity;
import org.apache.inlong.manager.dao.mapper.BusinessEntityMapper;
import org.apache.inlong.manager.dao.mapper.StorageExtEntityMapper;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.apache.inlong.manager.service.workflow.newbusiness.CreateResourceWorkflowForm;
import org.apache.inlong.manager.service.workflow.newbusiness.NewBusinessWorkflowForm;
import org.apache.inlong.manager.service.workflow.newstream.SingleStreamWorkflowDefinition;
import org.apache.inlong.manager.common.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

/**
 * Data is stored in the operation class of HIVE/THIV
 */
public class StorageBaseOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageBaseOperation.class);

    public final ExecutorService executorService = new ThreadPoolExecutor(
            10,
            20,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(100),
            new ThreadFactoryBuilder().setNameFormat("data-stream-workflow-%s").build(),
            new CallerRunsPolicy());

    @Autowired
    private BusinessEntityMapper businessMapper;
    @Autowired
    private StorageExtEntityMapper storageExtMapper;
    @Autowired
    private WorkflowService workflowService;
    @Autowired
    private BusinessProcessOperation businessProcessOperation;

    /**
     * Initiate business approval process
     *
     * @param operator Operator
     * @param businessEntity Business entity
     */
    public void startBusinessProcess(String operator, BusinessEntity businessEntity) {
        BusinessInfo businessInfo = CommonBeanUtils.copyProperties(businessEntity, BusinessInfo::new);

        NewBusinessWorkflowForm form = businessProcessOperation.genNewBusinessWorkflowForm(businessInfo);

        workflowService.start(ProcessName.NEW_BUSINESS_WORKFLOW, operator, form);
    }

    /**
     * heck whether the business status is temporary
     *
     * @param bid Business identifier
     * @return Business entity，For caller reuse
     */
    public BusinessEntity checkBizIsTempStatus(String bid) {
        BusinessEntity businessEntity = businessMapper.selectByIdentifier(bid);
        Preconditions.checkNotNull(businessEntity, "businessIdentifier is invalid");
        // Add/modify/delete is not allowed under certain business status
        if (EntityStatus.BIZ_TEMP_STATUS.contains(businessEntity.getStatus())) {
            LOGGER.error("business status was not allowed to add/update/delete data storage");
            throw new BusinessException(BizErrorCodeEnum.STORAGE_OPT_NOT_ALLOWED);
        }

        return businessEntity;
    }

    /**
     * Update extended information
     * <p/>First physically delete the existing extended information, and then add this batch of extended information
     *
     * @param storageType Storage type
     * @param storageId Storage ID
     * @param extList Extended information list
     */
    @Transactional(rollbackFor = Throwable.class)
    public void updateExtOpt(String storageType, Integer storageId, List<StorageExtInfo> extList) {
        LOGGER.info("begin to update data storage ext={}", extList);
        try {
            storageExtMapper.deleteByStorageTypeAndId(storageType, storageId);
            saveExtOpt(storageType, storageId, extList);
        } catch (Exception e) {
            LOGGER.error("failed to update data storage ext: ", e);
            throw new BusinessException(BizErrorCodeEnum.STORAGE_SAVE_FAILED);
        }
    }

    /**
     * Save extended information
     *
     * @param storageType Data storage type
     * @param storageId Data storage ID
     * @param extList Extended information list
     */
    public void saveExtOpt(String storageType, int storageId, List<StorageExtInfo> extList) {
        if (CollectionUtils.isEmpty(extList)) {
            return;
        }
        LOGGER.info("begin to save storage ext={}", extList);
        Date date = new Date();
        for (StorageExtInfo extInfo : extList) {
            StorageExtEntity extEntity = CommonBeanUtils.copyProperties(extInfo, StorageExtEntity::new);
            extEntity.setStorageId(storageId);
            extEntity.setStorageType(storageType);
            extEntity.setModifyTime(date);
            storageExtMapper.insert(extEntity);
        }
        LOGGER.info("success to save storage ext");
    }

    /**
     * Asynchronously initiate a single data stream related workflow
     *
     * @see SingleStreamWorkflowDefinition
     */
    class WorkflowStartRunnable implements Runnable {

        private final String operator;
        private final BusinessEntity businessEntity;
        private final String dataStreamIdentifier;

        public WorkflowStartRunnable(String operator, BusinessEntity businessEntity, String dataStreamIdentifier) {
            this.operator = operator;
            this.businessEntity = businessEntity;
            this.dataStreamIdentifier = dataStreamIdentifier;
        }

        @Override
        public void run() {
            String bid = businessEntity.getBusinessIdentifier();
            LOGGER.info("begin start data stream workflow, bid={}, dsid={}", bid, dataStreamIdentifier);

            BusinessInfo businessInfo = CommonBeanUtils.copyProperties(businessEntity, BusinessInfo::new);
            CreateResourceWorkflowForm form = genBizResourceWorkflowForm(businessInfo, dataStreamIdentifier);

            workflowService.start(ProcessName.CREATE_DATASTREAM_RESOURCE, operator, form);
            LOGGER.info("success start data stream workflow, bid={}, dsid={}", bid, dataStreamIdentifier);
        }

        /**
         * Generate [Create Business Resource] form
         */
        private CreateResourceWorkflowForm genBizResourceWorkflowForm(BusinessInfo businessInfo, String dsid) {
            CreateResourceWorkflowForm form = new CreateResourceWorkflowForm();
            form.setBusinessInfo(businessInfo);
            form.setDataStreamIdentifier(dsid);
            return form;
        }
    }
}
