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
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.business.BusinessApproveInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessCountVO;
import org.apache.inlong.manager.common.pojo.business.BusinessExtInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessListVO;
import org.apache.inlong.manager.common.pojo.business.BusinessPageRequest;
import org.apache.inlong.manager.common.pojo.business.BusinessTopicVO;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.apache.inlong.manager.dao.entity.BusinessExtEntity;
import org.apache.inlong.manager.dao.mapper.BusinessEntityMapper;
import org.apache.inlong.manager.dao.mapper.BusinessExtEntityMapper;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.core.DataStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Business access service layer implementation
 */
@Service
public class BusinessServiceImpl implements BusinessService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessServiceImpl.class);

    @Autowired
    private BusinessEntityMapper businessMapper;
    @Autowired
    private BusinessExtEntityMapper businessExtMapper;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private DataStreamService streamService;

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public String save(BusinessInfo businessInfo, String operator) {
        LOGGER.debug("begin to save business info={}", businessInfo);
        Preconditions.checkNotNull(businessInfo, "business info is empty");
        String bizName = businessInfo.getName();
        Preconditions.checkNotNull(bizName, "business name is empty");

        String topic = bizName.toLowerCase(Locale.ROOT);
        // groupId=b_topic, cannot update
        String groupId = "b_" + topic;
        Integer count = businessMapper.selectIdentifierExist(groupId);
        if (count >= 1) {
            LOGGER.error("groupId [{}] has already exists", groupId);
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_DUPLICATE);
        }

        // Processing business and extended information
        BusinessEntity entity = CommonBeanUtils.copyProperties(businessInfo, BusinessEntity::new);
        entity.setInlongGroupId(groupId);
        entity.setMqResourceObj(topic);

        // Only M0 is currently supported
        entity.setSchemaName(BizConstant.SCHEMA_M0_DAY);

        // After saving, the status is set to [BIZ_WAIT_SUBMIT]
        entity.setStatus(EntityStatus.BIZ_WAIT_SUBMIT.getCode());

        entity.setCreator(operator);
        entity.setModifier(operator);
        entity.setCreateTime(new Date());
        businessMapper.insertSelective(entity);

        this.saveExt(groupId, businessInfo.getExtList());

        LOGGER.info("success to save business info");
        return groupId;
    }

    @Override
    public BusinessInfo get(String groupId) {
        LOGGER.debug("begin to get business info by groupId={}", groupId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        BusinessEntity entity = businessMapper.selectByIdentifier(groupId);
        if (entity == null) {
            LOGGER.error("business not found by groupId={}", groupId);
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_NOT_FOUND);
        }

        BusinessInfo businessInfo = CommonBeanUtils.copyProperties(entity, BusinessInfo::new);
        List<BusinessExtEntity> extEntityList = businessExtMapper.selectByGroupId(groupId);
        List<BusinessExtInfo> extInfoList = CommonBeanUtils
                .copyListProperties(extEntityList, BusinessExtInfo::new);
        businessInfo.setExtList(extInfoList);

        LOGGER.info("success to get business info");
        return businessInfo;
    }

    @Override
    public PageInfo<BusinessListVO> listByCondition(BusinessPageRequest request) {
        LOGGER.debug("begin to list business info by {}", request);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<BusinessEntity> entityPage = (Page<BusinessEntity>) businessMapper.selectByCondition(request);
        List<BusinessListVO> businessList = CommonBeanUtils.copyListProperties(entityPage, BusinessListVO::new);
        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<BusinessListVO> page = new PageInfo<>(businessList);
        page.setTotal(entityPage.getTotal());

        LOGGER.info("success to list business info");
        return page;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public String update(BusinessInfo businessInfo, String operator) {
        LOGGER.debug("begin to update business info={}", businessInfo);
        Preconditions.checkNotNull(businessInfo, "business info is empty");
        String groupId = businessInfo.getInlongGroupId();
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        BusinessEntity entity = businessMapper.selectByIdentifier(groupId);
        if (entity == null) {
            LOGGER.error("business not found by groupId={}", groupId);
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_NOT_FOUND);
        }

        // Check whether the current status can be modified
        this.checkBizCanUpdate(entity, businessInfo);

        CommonBeanUtils.copyProperties(businessInfo, entity, true);
        if (EntityStatus.BIZ_CONFIG_FAILED.getCode().equals(entity.getStatus())) {
            entity.setStatus(EntityStatus.BIZ_WAIT_SUBMIT.getCode());
        }
        entity.setModifier(operator);
        businessMapper.updateByIdentifierSelective(entity);

        // Save extended information
        this.updateExt(groupId, businessInfo.getExtList());

        LOGGER.info("success to update business info");
        return groupId;
    }

    /**
     * Check whether modification is supported under the current business status, and which fields can be modified
     *
     * @param entity Original business entity
     * @param businessInfo New business information
     */
    private void checkBizCanUpdate(BusinessEntity entity, BusinessInfo businessInfo) {
        if (entity == null || businessInfo == null) {
            return;
        }
        // Check whether the current state supports modification
        Integer oldStatus = entity.getStatus();
        if (!EntityStatus.ALLOW_UPDATE_BIZ_STATUS.contains(oldStatus)) {
            LOGGER.error("current status was not allowed to update");
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_UPDATE_NOT_ALLOWED);
        }

        // Non-[DRAFT] status, no groupId modification allowed
        boolean updateGroupId = !EntityStatus.DRAFT.getCode().equals(oldStatus)
                && !Objects.equals(entity.getInlongGroupId(), businessInfo.getInlongGroupId());
        if (updateGroupId) {
            LOGGER.error("current status was not allowed to update business group id");
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_GROUP_ID_UPDATE_NOT_ALLOWED);
        }

        // [Configuration successful] Status, groupId and middleware type are not allowed to be modified
        if (EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode().equals(oldStatus)) {
            if (!Objects.equals(entity.getInlongGroupId(), businessInfo.getInlongGroupId())) {
                LOGGER.error("current status was not allowed to update business group id");
                throw new BusinessException(BizErrorCodeEnum.BUSINESS_GROUP_ID_UPDATE_NOT_ALLOWED);
            }
            if (!Objects.equals(entity.getMiddlewareType(), businessInfo.getMiddlewareType())) {
                LOGGER.error("current status was not allowed to update middleware type");
                throw new BusinessException(BizErrorCodeEnum.BUSINESS_MIDDLEWARE_UPDATE_NOT_ALLOWED);
            }
        }
    }

    @Override
    public boolean updateStatus(String groupId, Integer status, String operator) {
        LOGGER.debug("begin to update business status, groupId={}, status={}, username={}", groupId, status, operator);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        businessMapper.updateStatusByIdentifier(groupId, status, operator);

        LOGGER.info("success to update business status");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean delete(String groupId, String operator) {
        LOGGER.debug("begin to delete business, groupId={}", groupId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        BusinessEntity entity = businessMapper.selectByIdentifier(groupId);
        if (entity == null) {
            LOGGER.error("business not found by groupId={}", groupId);
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_NOT_FOUND);
        }

        // Determine whether the current state can be deleted
        if (!EntityStatus.ALLOW_DELETE_BIZ_STATUS.contains(entity.getStatus())) {
            LOGGER.error("current status was not allowed to delete");
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_DELETE_NOT_ALLOWED);
        }

        // [DRAFT] [BIZ_WAIT_SUBMIT] status, all associated data can be logically deleted directly
        if (EntityStatus.ALLOW_DELETE_BIZ_CASCADE_STATUS.contains(entity.getStatus())) {
            // Logically delete data streams, data sources and data storage information
            streamService.logicDeleteAll(entity.getInlongGroupId(), operator);
        } else {
            // In other states, you need to delete the associated "data stream" first.
            // When deleting a data stream, you also need to check whether there are
            // some associated "data source" and "data storage"
            int count = streamService.selectCountByGroupId(groupId);
            if (count >= 1) {
                LOGGER.error("groupId={} have [{}] data streams, deleted failed", groupId, count);
                throw new BusinessException(BizErrorCodeEnum.BUSINESS_HAS_DATA_STREAM);
            }
        }

        entity.setIsDeleted(EntityStatus.IS_DELETED.getCode());
        entity.setStatus(EntityStatus.DELETED.getCode());
        entity.setModifier(operator);
        businessMapper.updateByIdentifierSelective(entity);

        // To logically delete the associated extension table
        LOGGER.debug("begin to delete business ext property, groupId={}", groupId);
        businessExtMapper.logicDeleteAllByGroupId(groupId);

        LOGGER.info("success to delete business and business ext property");
        return true;
    }

    @Override
    public boolean exist(String groupId) {
        LOGGER.debug("begin to check business, groupId={}", groupId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        Integer count = businessMapper.selectIdentifierExist(groupId);
        LOGGER.info("success to check business");
        return count >= 1;
    }

    @Override
    public BusinessCountVO countBusinessByUser(String operator) {
        LOGGER.debug("begin to count business by user={}", operator);

        BusinessCountVO countVO = new BusinessCountVO();
        List<Map<String, Object>> statusCount = businessMapper.countCurrentUserBusiness(operator);
        for (Map<String, Object> map : statusCount) {
            int status = (Integer) map.get("status");
            long count = (Long) map.get("count");
            countVO.setTotalCount(countVO.getTotalCount() + count);
            if (status == EntityStatus.BIZ_CONFIG_ING.getCode()) {
                countVO.setWaitAssignCount(countVO.getWaitAssignCount() + count);
            } else if (status == EntityStatus.BIZ_WAIT_APPROVAL.getCode()) {
                countVO.setWaitApproveCount(countVO.getWaitApproveCount() + count);
            } else if (status == EntityStatus.BIZ_APPROVE_REJECTED.getCode()) {
                countVO.setRejectCount(countVO.getRejectCount() + count);
            }
        }
        LOGGER.info("success to count business");
        return countVO;
    }

    @Override
    public BusinessTopicVO getTopic(String groupId) {
        LOGGER.debug("begin to get topic by groupId={}", groupId);
        BusinessInfo businessInfo = this.get(groupId);

        String middlewareType = businessInfo.getMiddlewareType();
        BusinessTopicVO topicVO = new BusinessTopicVO();

        if (BizConstant.MIDDLEWARE_TYPE_TUBE.equalsIgnoreCase(middlewareType)) {
            // Tube Topic corresponds to business one-to-one
            topicVO.setTopicName(businessInfo.getMqResourceObj());
            topicVO.setMasterUrl(clusterBean.getTubeMaster());
        } else {
            LOGGER.error("middlewareType={} not supported", middlewareType);
            throw new BusinessException(BizErrorCodeEnum.MIDDLEWARE_TYPE_NOT_SUPPORTED);
        }

        topicVO.setInlongGroupId(groupId);
        topicVO.setMiddlewareType(middlewareType);
        return topicVO;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean updateAfterApprove(BusinessApproveInfo approveInfo, String operator) {
        LOGGER.debug("begin to update business after approve={}", approveInfo);

        // Save the dataSchema, Topic and other information of the business
        Preconditions.checkNotNull(approveInfo, "BusinessApproveInfo is empty");
        String groupId = approveInfo.getInlongGroupId();
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        // Update status to [BIZ_CONFIG_ING]
        // If you need to change business info after approve, just do in here
        this.updateStatus(groupId, EntityStatus.BIZ_CONFIG_ING.getCode(), operator);

        LOGGER.info("success to update business status after approve");
        return true;
    }

    /**
     * Update extended information
     * <p/>First physically delete the existing extended information, and then add this batch of extended information
     */
    @Transactional(rollbackFor = Throwable.class)
    void updateExt(String groupId, List<BusinessExtInfo> extInfoList) {
        LOGGER.debug("begin to update business ext, groupId={}, ext={}", groupId, extInfoList);
        try {
            businessExtMapper.deleteAllByGroupId(groupId);
            saveExt(groupId, extInfoList);
            LOGGER.info("success to update business ext");
        } catch (Exception e) {
            LOGGER.error("failed to update business ext: ", e);
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_SAVE_FAILED);
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    void saveExt(String groupId, List<BusinessExtInfo> infoList) {
        if (CollectionUtils.isEmpty(infoList)) {
            return;
        }
        List<BusinessExtEntity> entityList = CommonBeanUtils.copyListProperties(infoList, BusinessExtEntity::new);
        Date date = new Date();
        for (BusinessExtEntity entity : entityList) {
            entity.setInlongGroupId(groupId);
            entity.setModifyTime(date);
        }
        businessExtMapper.insertAll(entityList);
    }

}
