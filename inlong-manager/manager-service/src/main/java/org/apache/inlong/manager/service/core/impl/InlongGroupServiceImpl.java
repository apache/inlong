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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupCountResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupTopicResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupExtEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupPulsarEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupPulsarEntityMapper;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Inlong group service layer implementation
 */
@Service
public class InlongGroupServiceImpl implements InlongGroupService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongGroupServiceImpl.class);
    @Autowired
    InlongGroupPulsarEntityMapper groupPulsarMapper;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongGroupExtEntityMapper groupExtMapper;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private InlongStreamService streamService;

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public String save(InlongGroupRequest groupInfo, String operator) {
        LOGGER.debug("begin to save inlong group info={}", groupInfo);
        Preconditions.checkNotNull(groupInfo, "inlong group info is empty");
        String bizName = groupInfo.getName();
        Preconditions.checkNotNull(bizName, "inlong group name is empty");

        // groupId=b_name, cannot update
        String groupId = "b_" + bizName.toLowerCase(Locale.ROOT);
        Integer count = groupMapper.selectIdentifierExist(groupId);
        if (count >= 1) {
            LOGGER.error("groupId [{}] has already exists", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_DUPLICATE);
        }

        // Processing inlong group and extended information
        InlongGroupEntity entity = CommonBeanUtils.copyProperties(groupInfo, InlongGroupEntity::new);
        entity.setInlongGroupId(groupId);
        if (StringUtils.isEmpty(entity.getMqResourceObj())) {
            entity.setMqResourceObj(groupId);
        }
        // Only M0 is currently supported
        entity.setSchemaName(Constant.SCHEMA_M0_DAY);

        // After saving, the status is set to [GROUP_WAIT_SUBMIT]
        entity.setStatus(EntityStatus.GROUP_WAIT_SUBMIT.getCode());
        entity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
        if (StringUtils.isEmpty(entity.getCreator())) {
            entity.setCreator(operator);
        }
        if (StringUtils.isEmpty(entity.getModifier())) {
            entity.setModifier(operator);
        }
        entity.setCreateTime(new Date());
        groupMapper.insertSelective(entity);
        this.saveOrUpdateExt(groupId, groupInfo.getExtList());

        if (Constant.MIDDLEWARE_PULSAR.equals(groupInfo.getMiddlewareType())) {
            InlongGroupPulsarInfo pulsarInfo = (InlongGroupPulsarInfo) groupInfo.getMqExtInfo();
            Preconditions.checkNotNull(pulsarInfo, "Pulsar info cannot be empty, as the middleware is Pulsar");

            // Pulsar params must meet: ackQuorum <= writeQuorum <= ensemble
            Integer ackQuorum = pulsarInfo.getAckQuorum();
            Integer writeQuorum = pulsarInfo.getWriteQuorum();

            Preconditions.checkNotNull(ackQuorum, "Pulsar ackQuorum cannot be empty");
            Preconditions.checkNotNull(writeQuorum, "Pulsar writeQuorum cannot be empty");

            if (!(ackQuorum <= writeQuorum)) {
                throw new BusinessException(ErrorCodeEnum.GROUP_SAVE_FAILED,
                        "Pulsar params must meet: ackQuorum <= writeQuorum");
            }
            // The default value of ensemble is writeQuorum
            pulsarInfo.setEnsemble(writeQuorum);

            // Pulsar entity may already exist, such as unsuccessfully deleted, or modify the MQ type to Tube,
            // need to delete and add the Pulsar entity with the same group id
            InlongGroupPulsarEntity pulsarEntity = groupPulsarMapper.selectByGroupId(groupId);
            if (pulsarEntity == null) {
                pulsarEntity = CommonBeanUtils.copyProperties(pulsarInfo, InlongGroupPulsarEntity::new);
                pulsarEntity.setIsDeleted(0);
                pulsarEntity.setInlongGroupId(groupId);
                groupPulsarMapper.insertSelective(pulsarEntity);
            } else {
                Integer id = pulsarEntity.getId();
                pulsarEntity = CommonBeanUtils.copyProperties(pulsarInfo, InlongGroupPulsarEntity::new);
                pulsarEntity.setId(id);
                groupPulsarMapper.updateByPrimaryKeySelective(pulsarEntity);
            }
        }

        LOGGER.debug("success to save inlong group info for groupId={}", groupId);
        return groupId;
    }

    @Override
    public InlongGroupRequest get(String groupId) {
        LOGGER.debug("begin to get inlong group info by groupId={}", groupId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        InlongGroupEntity entity = groupMapper.selectByIdentifier(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        InlongGroupRequest groupInfo = CommonBeanUtils.copyProperties(entity, InlongGroupRequest::new);
        List<InlongGroupExtEntity> extEntityList = groupExtMapper.selectByGroupId(groupId);
        List<InlongGroupExtInfo> extInfoList = CommonBeanUtils
                .copyListProperties(extEntityList, InlongGroupExtInfo::new);
        groupInfo.setExtList(extInfoList);

        // If the middleware is Pulsar, we need to encapsulate Pulsar related data
        String middlewareType = entity.getMiddlewareType();
        if (Constant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middlewareType)) {
            InlongGroupPulsarEntity pulsarEntity = groupPulsarMapper.selectByGroupId(groupId);
            Preconditions.checkNotNull(pulsarEntity, "Pulsar info not found under the inlong group");
            InlongGroupPulsarInfo pulsarInfo = CommonBeanUtils.copyProperties(pulsarEntity, InlongGroupPulsarInfo::new);
            groupInfo.setMqExtInfo(pulsarInfo);
        }

        // For approved inlong group, encapsulate the cluster address of the middleware
        if (EntityStatus.GROUP_CONFIG_SUCCESSFUL.getCode().equals(groupInfo.getStatus())) {
            if (Constant.MIDDLEWARE_TUBE.equalsIgnoreCase(middlewareType)) {
                groupInfo.setTubeMaster(clusterBean.getTubeMaster());
            } else if (Constant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middlewareType)) {
                groupInfo.setPulsarAdminUrl(clusterBean.getPulsarAdminUrl());
                groupInfo.setPulsarServiceUrl(clusterBean.getPulsarServiceUrl());
            }
        }

        LOGGER.debug("success to get inlong group for groupId={}", groupId);
        return groupInfo;
    }

    @Override
    public PageInfo<InlongGroupListResponse> listByCondition(InlongGroupPageRequest request) {
        LOGGER.debug("begin to list inlong group by {}", request);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongGroupEntity> entityPage = (Page<InlongGroupEntity>) groupMapper.selectByCondition(request);
        List<InlongGroupListResponse> groupList = CommonBeanUtils.copyListProperties(entityPage,
                InlongGroupListResponse::new);
        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<InlongGroupListResponse> page = new PageInfo<>(groupList);
        page.setTotal(entityPage.getTotal());

        LOGGER.debug("success to list inlong group");
        return page;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public String update(InlongGroupRequest groupInfo, String operator) {
        LOGGER.debug("begin to update inlong group={}", groupInfo);
        Preconditions.checkNotNull(groupInfo, "inlong group is empty");
        String groupId = groupInfo.getInlongGroupId();
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);

        InlongGroupEntity entity = groupMapper.selectByIdentifier(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        // Check whether the current status can be modified
        this.checkBizCanUpdate(entity, groupInfo);

        CommonBeanUtils.copyProperties(groupInfo, entity, true);
        if (EntityStatus.GROUP_CONFIG_FAILED.getCode().equals(entity.getStatus())) {
            entity.setStatus(EntityStatus.GROUP_WAIT_SUBMIT.getCode());
        }
        entity.setModifier(operator);
        groupMapper.updateByIdentifierSelective(entity);

        // Save extended information
        this.saveOrUpdateExt(groupId, groupInfo.getExtList());

        // Update the Pulsar info
        if (Constant.MIDDLEWARE_PULSAR.equals(groupInfo.getMiddlewareType())) {
            InlongGroupPulsarInfo pulsarInfo = (InlongGroupPulsarInfo) groupInfo.getMqExtInfo();
            Preconditions.checkNotNull(pulsarInfo, "Pulsar info cannot be empty, as the middleware is Pulsar");
            Integer writeQuorum = pulsarInfo.getWriteQuorum();
            Integer ackQuorum = pulsarInfo.getAckQuorum();
            if (!(ackQuorum <= writeQuorum)) {
                throw new BusinessException(ErrorCodeEnum.GROUP_SAVE_FAILED,
                        "Pulsar params must meet: ackQuorum <= writeQuorum");
            }
            InlongGroupPulsarEntity pulsarEntity = CommonBeanUtils.copyProperties(pulsarInfo,
                    InlongGroupPulsarEntity::new);
            pulsarEntity.setInlongGroupId(groupId);
            groupPulsarMapper.updateByIdentifierSelective(pulsarEntity);
        }

        LOGGER.debug("success to update inlong group for groupId={}", groupId);
        return groupId;
    }

    /**
     * Check whether modification is supported under the current inlong group status, and which fields can be modified
     *
     * @param entity Original inlong group entity
     * @param groupInfo New inlong group information
     */
    private void checkBizCanUpdate(InlongGroupEntity entity, InlongGroupRequest groupInfo) {
        if (entity == null || groupInfo == null) {
            return;
        }
        // Check whether the current status supports modification
        Integer oldStatus = entity.getStatus();
        if (!EntityStatus.ALLOW_UPDATE_GROUP_STATUS.contains(oldStatus)) {
            LOGGER.error("current status was not allowed to update");
            throw new BusinessException(ErrorCodeEnum.GROUP_UPDATE_NOT_ALLOWED);
        }

        // Non-[DRAFT] status, no groupId modification allowed
        boolean updateGroupId = !EntityStatus.DRAFT.getCode().equals(oldStatus)
                && !Objects.equals(entity.getInlongGroupId(), groupInfo.getInlongGroupId());
        if (updateGroupId) {
            LOGGER.error("current status was not allowed to update inlong group id");
            throw new BusinessException(ErrorCodeEnum.GROUP_ID_UPDATE_NOT_ALLOWED);
        }

        // [Configuration successful] Status, groupId and middleware type are not allowed to be modified
        if (EntityStatus.GROUP_CONFIG_SUCCESSFUL.getCode().equals(oldStatus)) {
            if (!Objects.equals(entity.getInlongGroupId(), groupInfo.getInlongGroupId())) {
                LOGGER.error("current status was not allowed to update inlong group id");
                throw new BusinessException(ErrorCodeEnum.GROUP_ID_UPDATE_NOT_ALLOWED);
            }
            if (!Objects.equals(entity.getMiddlewareType(), groupInfo.getMiddlewareType())) {
                LOGGER.error("current status was not allowed to update middleware type");
                throw new BusinessException(ErrorCodeEnum.GROUP_MIDDLEWARE_UPDATE_NOT_ALLOWED);
            }
        }
    }

    @Override
    public boolean updateStatus(String groupId, Integer status, String operator) {
        LOGGER.debug("begin to update inlong group status, groupId={}, status={}, username={}", groupId, status,
                operator);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);

        groupMapper.updateStatusByIdentifier(groupId, status, operator);

        LOGGER.info("success to update inlong group status for groupId={}", groupId);
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean delete(String groupId, String operator) {
        LOGGER.debug("begin to delete inlong group, groupId={}", groupId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);

        InlongGroupEntity entity = groupMapper.selectByIdentifier(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        // Determine whether the current status can be deleted
        if (!EntityStatus.ALLOW_DELETE_GROUP_STATUS.contains(entity.getStatus())) {
            LOGGER.error("current status was not allowed to delete");
            throw new BusinessException(ErrorCodeEnum.GROUP_DELETE_NOT_ALLOWED);
        }

        // [DRAFT] [GROUP_WAIT_SUBMIT] status, all associated data can be logically deleted directly
        if (EntityStatus.ALLOW_DELETE_GROUP_CASCADE_STATUS.contains(entity.getStatus())) {
            // Logically delete inlong streams, data sources and data sink information
            streamService.logicDeleteAll(entity.getInlongGroupId(), operator);
        } else {
            // In other status, you need to delete the associated "inlong stream" first.
            // When deleting a inlong stream, you also need to check whether there are
            // some associated "data source" and "stream sink"
            int count = streamService.selectCountByGroupId(groupId);
            if (count >= 1) {
                LOGGER.error("groupId={} have [{}] inlong streams, deleted failed", groupId, count);
                throw new BusinessException(ErrorCodeEnum.GROUP_HAS_STREAM);
            }
        }

        entity.setIsDeleted(EntityStatus.IS_DELETED.getCode());
        entity.setStatus(EntityStatus.DELETED.getCode());
        entity.setModifier(operator);
        groupMapper.updateByIdentifierSelective(entity);

        // To logically delete the associated extension table
        groupExtMapper.logicDeleteAllByGroupId(groupId);

        // To logically delete the associated pulsar table
        groupPulsarMapper.logicDeleteByGroupId(groupId);

        LOGGER.info("success to delete inlong group and inlong group ext property for groupId={}", groupId);
        return true;
    }

    @Override
    public boolean exist(String groupId) {
        LOGGER.debug("begin to check inlong group, groupId={}", groupId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);

        Integer count = groupMapper.selectIdentifierExist(groupId);
        LOGGER.info("success to check inlong group");
        return count >= 1;
    }

    @Override
    public InlongGroupCountResponse countGroupByUser(String operator) {
        LOGGER.debug("begin to count inlong group by user={}", operator);

        InlongGroupCountResponse countVO = new InlongGroupCountResponse();
        List<Map<String, Object>> statusCount = groupMapper.countCurrentUserGroup(operator);
        for (Map<String, Object> map : statusCount) {
            int status = (Integer) map.get("status");
            long count = (Long) map.get("count");
            countVO.setTotalCount(countVO.getTotalCount() + count);
            if (status == EntityStatus.GROUP_CONFIG_ING.getCode()) {
                countVO.setWaitAssignCount(countVO.getWaitAssignCount() + count);
            } else if (status == EntityStatus.GROUP_WAIT_APPROVAL.getCode()) {
                countVO.setWaitApproveCount(countVO.getWaitApproveCount() + count);
            } else if (status == EntityStatus.GROUP_APPROVE_REJECTED.getCode()) {
                countVO.setRejectCount(countVO.getRejectCount() + count);
            }
        }
        LOGGER.info("success to count inlong group for operator={}", operator);
        return countVO;
    }

    @Override
    public InlongGroupTopicResponse getTopic(String groupId) {
        LOGGER.debug("begin to get topic by groupId={}", groupId);
        InlongGroupRequest groupInfo = this.get(groupId);

        String middlewareType = groupInfo.getMiddlewareType();
        InlongGroupTopicResponse topicVO = new InlongGroupTopicResponse();

        if (Constant.MIDDLEWARE_TUBE.equalsIgnoreCase(middlewareType)) {
            // Tube Topic corresponds to inlong group one-to-one
            topicVO.setMqResourceObj(groupInfo.getMqResourceObj());
            topicVO.setTubeMasterUrl(clusterBean.getTubeMaster());
        } else if (Constant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middlewareType)) {
            // Pulsar's topic corresponds to the inlong stream one-to-one
            topicVO.setDsTopicList(streamService.getTopicList(groupId));
            topicVO.setPulsarAdminUrl(clusterBean.getPulsarAdminUrl());
            topicVO.setPulsarServiceUrl(clusterBean.getPulsarServiceUrl());
        } else {
            LOGGER.error("middleware type={} not supported", middlewareType);
            throw new BusinessException(ErrorCodeEnum.MIDDLEWARE_TYPE_NOT_SUPPORTED);
        }

        topicVO.setInlongGroupId(groupId);
        topicVO.setMiddlewareType(middlewareType);
        return topicVO;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean updateAfterApprove(InlongGroupApproveRequest approveInfo, String operator) {
        LOGGER.debug("begin to update inlong group after approve={}", approveInfo);

        // Save the dataSchema, Topic and other information of the inlong group
        Preconditions.checkNotNull(approveInfo, "InlongGroupApproveRequest is empty");
        String groupId = approveInfo.getInlongGroupId();
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        String middlewareType = approveInfo.getMiddlewareType();
        Preconditions.checkNotNull(middlewareType, "Middleware type is empty");

        // Update status to [GROUP_CONFIG_ING]
        // If you need to change inlong group info after approve, just do in here
        this.updateStatus(groupId, EntityStatus.GROUP_CONFIG_ING.getCode(), operator);

        LOGGER.info("success to update inlong group status after approve for groupId={}", groupId);
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public void saveOrUpdateExt(String groupId, List<InlongGroupExtInfo> infoList) {
        LOGGER.debug("begin to save or update inlong group ext info, groupId={}, ext={}", groupId, infoList);

        if (CollectionUtils.isEmpty(infoList)) {
            return;
        }

        List<InlongGroupExtEntity> entityList = CommonBeanUtils.copyListProperties(infoList, InlongGroupExtEntity::new);
        Date date = new Date();
        for (InlongGroupExtEntity entity : entityList) {
            entity.setInlongGroupId(groupId);
            entity.setModifyTime(date);
        }
        groupExtMapper.insertOnDuplicateKeyUpdate(entityList);
        LOGGER.info("success to save or update inlong group ext for groupId={}", groupId);
    }

}
