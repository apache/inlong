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
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupCountResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
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
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    private CommonOperateService commonOperateService;
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
        entity.setStatus(GroupState.GROUP_WAIT_SUBMIT.getCode());
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
    public InlongGroupInfo get(String groupId) {
        LOGGER.debug("begin to get inlong group info by groupId={}", groupId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        InlongGroupInfo groupInfo = CommonBeanUtils.copyProperties(entity, InlongGroupInfo::new);
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
            pulsarInfo.setMiddlewareType(Constant.MIDDLEWARE_PULSAR);
            groupInfo.setMqExtInfo(pulsarInfo);
        }

        // For approved inlong group, encapsulate the cluster address of the middleware
        if (GroupState.GROUP_CONFIG_SUCCESSFUL == GroupState.forCode(groupInfo.getStatus())) {
            if (Constant.MIDDLEWARE_TUBE.equalsIgnoreCase(middlewareType)) {
                groupInfo.setTubeMaster(commonOperateService.getSpecifiedParam(Constant.TUBE_MASTER_URL));
            } else if (Constant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middlewareType)) {
                groupInfo.setPulsarAdminUrl(commonOperateService.getSpecifiedParam(Constant.PULSAR_ADMINURL));
                groupInfo.setPulsarServiceUrl(commonOperateService.getSpecifiedParam(Constant.PULSAR_SERVICEURL));
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

        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        // Check whether the current status can be modified
        this.checkGroupCanUpdate(entity, groupInfo, operator);

        CommonBeanUtils.copyProperties(groupInfo, entity, true);
        entity.setStatus(groupInfo.getStatus());

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
     * Check whether modification is supported under the current inlong group status, and which fields can be modified.
     *
     * @param entity Original inlong group entity.
     * @param groupInfo New inlong group info.
     * @param operator Current operator.
     */
    private void checkGroupCanUpdate(InlongGroupEntity entity, InlongGroupRequest groupInfo, String operator) {
        if (entity == null || groupInfo == null) {
            return;
        }

        // Only the person in charges can update
        if (StringUtils.isEmpty(entity.getInCharges())) {
            LOGGER.error("group [{}] has no inCharges", entity.getInlongGroupId());
            throw new BusinessException(ErrorCodeEnum.GROUP_INFO_INCONSISTENT);
        }
        List<String> inCharges = Arrays.asList(entity.getInCharges().split(","));
        if (!inCharges.contains(operator)) {
            LOGGER.error("user [{}] has no privilege for the inlong group", operator);
            throw new BusinessException(ErrorCodeEnum.GROUP_PERMISSION_DENIED);
        }
        // Check whether the current state supports modification
        GroupState curState = GroupState.forCode(entity.getStatus());
        if (groupInfo.getStatus() != null) {
            GroupState nextState = GroupState.forCode(groupInfo.getStatus());
            if (!GroupState.isAllowedTransition(curState, nextState)) {
                String errMsg = String.format("Current state=%s is not allowed to transfer to state=%s",
                        curState, nextState);
                LOGGER.error(errMsg);
                throw new BusinessException(ErrorCodeEnum.GROUP_UPDATE_NOT_ALLOWED, errMsg);
            }
        } else {
            if (!GroupState.isAllowedUpdate(curState)) {
                String errMsg = String.format("Current state=%s is not allowed to update",
                        curState);
                LOGGER.error(errMsg);
                throw new BusinessException(ErrorCodeEnum.GROUP_UPDATE_NOT_ALLOWED, errMsg);
            }
        }
    }

    @Override
    public boolean updateStatus(String groupId, Integer status, String operator) {
        LOGGER.debug("begin to update inlong group status, groupId={}, status={}, username={}", groupId, status,
                operator);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);
        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        GroupState curState = GroupState.forCode(entity.getStatus());
        GroupState nextState = GroupState.forCode(status);
        if (GroupState.isAllowedTransition(curState, nextState)) {
            groupMapper.updateStatus(groupId, status, operator);
            LOGGER.info("success to update inlong group status for groupId={}", groupId);
            return true;
        } else {
            String warnMsg = String.format("Current state=%s is not allowed to transfer to state=%s",
                    curState, nextState);
            LOGGER.warn(warnMsg);
            return false;
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean delete(String groupId, String operator) {
        LOGGER.debug("begin to delete inlong group, groupId={}", groupId);
        Preconditions.checkNotNull(groupId, Constant.GROUP_ID_IS_EMPTY);

        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        // Determine whether the current status can be deleted
        GroupState curState = GroupState.forCode(entity.getStatus());
        if (!GroupState.isAllowedTransition(curState, GroupState.GROUP_DELETE)) {
            String errMsg = String.format("Current state=%s was not allowed to delete", curState);
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.GROUP_DELETE_NOT_ALLOWED, errMsg);
        }

        // [DRAFT] [GROUP_WAIT_SUBMIT] status, all associated data can be logically deleted directly
        if (GroupState.isAllowedLogicDel(curState)) {
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

        entity.setIsDeleted(entity.getId());
        entity.setStatus(GroupState.GROUP_DELETE.getCode());
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
        List<Map<String, Object>> statusCount = groupMapper.countGroupByUser(operator);
        for (Map<String, Object> map : statusCount) {
            int status = (Integer) map.get("status");
            long count = (Long) map.get("count");
            countVO.setTotalCount(countVO.getTotalCount() + count);
            if (status == GroupState.GROUP_CONFIG_ING.getCode()) {
                countVO.setWaitAssignCount(countVO.getWaitAssignCount() + count);
            } else if (status == GroupState.GROUP_WAIT_APPROVAL.getCode()) {
                countVO.setWaitApproveCount(countVO.getWaitApproveCount() + count);
            } else if (status == GroupState.GROUP_APPROVE_REJECTED.getCode()) {
                countVO.setRejectCount(countVO.getRejectCount() + count);
            }
        }
        LOGGER.info("success to count inlong group for operator={}", operator);
        return countVO;
    }

    @Override
    public InlongGroupTopicResponse getTopic(String groupId) {
        LOGGER.debug("begin to get topic by groupId={}", groupId);
        InlongGroupInfo groupInfo = this.get(groupId);

        String middlewareType = groupInfo.getMiddlewareType();
        InlongGroupTopicResponse topicVO = new InlongGroupTopicResponse();

        if (Constant.MIDDLEWARE_TUBE.equalsIgnoreCase(middlewareType)) {
            // Tube Topic corresponds to inlong group one-to-one
            topicVO.setMqResourceObj(groupInfo.getMqResourceObj());
            topicVO.setTubeMasterUrl(commonOperateService.getSpecifiedParam(Constant.TUBE_MASTER_URL));
        } else if (Constant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middlewareType)) {
            // Pulsar's topic corresponds to the inlong stream one-to-one
            topicVO.setDsTopicList(streamService.getTopicList(groupId));
            topicVO.setPulsarAdminUrl(commonOperateService.getSpecifiedParam(Constant.PULSAR_ADMINURL));
            topicVO.setPulsarServiceUrl(commonOperateService.getSpecifiedParam(Constant.PULSAR_SERVICEURL));
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

        // Update status to [GROUP_APPROVE_PASSED]
        // If you need to change inlong group info after approve, just do in here
        this.updateStatus(groupId, GroupState.GROUP_APPROVE_PASSED.getCode(), operator);

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
