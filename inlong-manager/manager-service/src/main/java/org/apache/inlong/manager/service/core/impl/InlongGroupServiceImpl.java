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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GlobalConstants;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupCountResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupMqExtBase;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupTopicResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupExtEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.core.mq.Middleware;
import org.apache.inlong.manager.service.core.mq.MiddlewareFactory;
import org.apache.inlong.manager.service.source.SourceOperationFactory;
import org.apache.inlong.manager.service.source.StreamSourceOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Inlong group service layer implementation
 */
@Service
public class InlongGroupServiceImpl implements InlongGroupService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongGroupServiceImpl.class);

    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongGroupExtEntityMapper groupExtMapper;
    @Autowired
    private StreamSourceEntityMapper streamSourceEntityMapper;
    @Autowired
    private SourceOperationFactory operationFactory;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private MiddlewareFactory groupMqFactory;

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public String save(InlongGroupRequest groupInfo, String operator) {
        LOGGER.debug("begin to save inlong group info={}", groupInfo);
        Preconditions.checkNotNull(groupInfo, "inlong group info is empty");
        String groupName = groupInfo.getName();
        Preconditions.checkNotNull(groupName, "inlong group name is empty");

        // groupId=b_name, cannot update
        String groupId = "b_" + groupName.toLowerCase(Locale.ROOT);
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
        entity.setStatus(GroupStatus.TO_BE_SUBMIT.getCode());
        entity.setIsDeleted(GlobalConstants.UN_DELETED);
        if (StringUtils.isEmpty(entity.getCreator())) {
            entity.setCreator(operator);
        }
        if (StringUtils.isEmpty(entity.getModifier())) {
            entity.setModifier(operator);
        }
        entity.setCreateTime(new Date());
        groupMapper.insertSelective(entity);
        this.saveOrUpdateExt(groupId, groupInfo.getExtList());
        // Saving MQ information.
        Middleware mqMiddleware = groupMqFactory.getMqMiddleware(MQType.forType(groupInfo.getMiddlewareType()));
        if (groupInfo.getMqExtInfo() != null && StringUtils.isBlank(groupInfo.getMqExtInfo().getInlongGroupId())) {
            groupInfo.getMqExtInfo().setInlongGroupId(groupId);
        }
        mqMiddleware.save(groupInfo.getMqExtInfo());
        LOGGER.debug("success to save inlong group info for groupId={}", groupId);
        return groupId;
    }

    @Override
    public InlongGroupInfo get(String groupId) {
        LOGGER.debug("begin to get inlong group info by groupId={}", groupId);
        Preconditions.checkNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
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
        MQType mqType = MQType.forType(entity.getMiddlewareType());
        Middleware mq = groupMqFactory.getMqMiddleware(mqType);
        groupInfo.setMqExtInfo(mq.get(groupId));

        // For approved inlong group, encapsulate the cluster address of the middleware
        if (GroupStatus.CONFIG_SUCCESSFUL == GroupStatus.forCode(groupInfo.getStatus())) {
            groupInfo = mq.packSpecificInfo(groupInfo);
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
        if (request.isListSources() && CollectionUtils.isNotEmpty(groupList)) {
            List<String> groupIds = groupList.stream().map(InlongGroupListResponse::getInlongGroupId)
                    .collect(Collectors.toList());
            List<StreamSourceEntity> sourceEntities = streamSourceEntityMapper.selectByGroupIds(groupIds);
            Map<String, List<SourceListResponse>> sourceMap = Maps.newHashMap();
            sourceEntities.forEach(sourceEntity -> {
                SourceType sourceType = SourceType.forType(sourceEntity.getSourceType());
                StreamSourceOperation operation = operationFactory.getInstance(sourceType);
                SourceListResponse sourceListResponse = operation.getFromEntity(sourceEntity, SourceListResponse::new);
                sourceMap.computeIfAbsent(sourceEntity.getInlongGroupId(), k -> Lists.newArrayList())
                        .add(sourceListResponse);
            });
            groupList.forEach(group -> {
                List<SourceListResponse> sourceListResponses = sourceMap.getOrDefault(group.getInlongGroupId(),
                        Lists.newArrayList());
                group.setSourceListResponses(sourceListResponses);
            });
        }
        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<InlongGroupListResponse> page = new PageInfo<>(groupList);
        page.setTotal(entityPage.getTotal());

        LOGGER.debug("success to list inlong group");
        return page;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ,
            propagation = Propagation.REQUIRES_NEW)
    public String update(InlongGroupRequest groupRequest, String operator) {
        LOGGER.debug("begin to update inlong group={}", groupRequest);
        Preconditions.checkNotNull(groupRequest, "inlong group is empty");
        String groupId = groupRequest.getInlongGroupId();
        Preconditions.checkNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());

        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        // Check whether the current status can be modified
        this.checkGroupCanUpdate(entity, groupRequest, operator);
        CommonBeanUtils.copyProperties(groupRequest, entity, true);

        entity.setModifier(operator);
        groupMapper.updateByIdentifierSelective(entity);

        // Save extended information
        this.saveOrUpdateExt(groupId, groupRequest.getExtList());

        // Update the MQ info
        MQType mqType = MQType.forType(groupRequest.getMiddlewareType());
        Middleware mqMiddleware = groupMqFactory.getMqMiddleware(mqType);
        InlongGroupMqExtBase mqExtInfo = groupRequest.getMqExtInfo();
        if (mqExtInfo != null && StringUtils.isBlank(mqExtInfo.getInlongGroupId())) {
            mqExtInfo.setInlongGroupId(groupId);
        }
        mqMiddleware.update(mqExtInfo);
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
        GroupStatus curState = GroupStatus.forCode(entity.getStatus());
        if (GroupStatus.notAllowedUpdate(curState)) {
            String errMsg = String.format("Current state=%s is not allowed to update", curState);
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.GROUP_UPDATE_NOT_ALLOWED, errMsg);
        }
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ,
            propagation = Propagation.REQUIRES_NEW)
    public boolean updateStatus(String groupId, Integer status, String operator) {
        LOGGER.info("begin to update group status to [{}] by groupId={}, username={}", status, groupId, operator);
        Preconditions.checkNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
        InlongGroupEntity entity = groupMapper.selectByGroupIdForUpdate(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        GroupStatus curState = GroupStatus.forCode(entity.getStatus());
        GroupStatus nextState = GroupStatus.forCode(status);
        if (GroupStatus.notAllowedTransition(curState, nextState)) {
            String errorMsg = String.format("Current state=%s is not allowed to transfer to state=%s",
                    curState, nextState);
            LOGGER.error(errorMsg);
            throw new BusinessException(errorMsg);
        }

        groupMapper.updateStatus(groupId, status, operator);
        LOGGER.info("success to update inlong group status to [{}] for groupId={}", status, groupId);
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean delete(String groupId, String operator) {
        LOGGER.debug("begin to delete inlong group, groupId={}", groupId);
        Preconditions.checkNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());

        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        // Determine whether the current status can be deleted
        GroupStatus curState = GroupStatus.forCode(entity.getStatus());
        if (GroupStatus.notAllowedTransition(curState, GroupStatus.DELETED)) {
            String errMsg = String.format("Current state=%s was not allowed to delete", curState);
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.GROUP_DELETE_NOT_ALLOWED, errMsg);
        }

        // [DRAFT] [GROUP_WAIT_SUBMIT] status, all associated data can be logically deleted directly
        if (GroupStatus.isAllowedLogicDel(curState)) {
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
        entity.setStatus(GroupStatus.DELETED.getCode());
        entity.setModifier(operator);
        groupMapper.updateByIdentifierSelective(entity);

        // To logically delete the associated extension table
        groupExtMapper.logicDeleteAllByGroupId(groupId);
        groupMqFactory.getMqMiddleware(MQType.forType(entity.getMiddlewareType())).delete(groupId);
        LOGGER.info("success to delete inlong group and inlong group ext property for groupId={}", groupId);
        return true;
    }

    @Override
    public boolean exist(String groupId) {
        LOGGER.debug("begin to check inlong group, groupId={}", groupId);
        Preconditions.checkNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());

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
            if (status == GroupStatus.CONFIG_ING.getCode()) {
                countVO.setWaitAssignCount(countVO.getWaitAssignCount() + count);
            } else if (status == GroupStatus.TO_BE_APPROVAL.getCode()) {
                countVO.setWaitApproveCount(countVO.getWaitApproveCount() + count);
            } else if (status == GroupStatus.APPROVE_REJECTED.getCode()) {
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
        MQType mqType = MQType.forType(groupInfo.getMiddlewareType());
        Middleware mqMiddleware = groupMqFactory.getMqMiddleware(mqType);
        return mqMiddleware.getTopic(groupInfo);
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean updateAfterApprove(InlongGroupApproveRequest approveInfo, String operator) {
        LOGGER.debug("begin to update inlong group after approve={}", approveInfo);

        // Save the dataSchema, Topic and other information of the inlong group
        Preconditions.checkNotNull(approveInfo, "InlongGroupApproveRequest is empty");
        String groupId = approveInfo.getInlongGroupId();
        Preconditions.checkNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
        String middlewareType = approveInfo.getMiddlewareType();
        Preconditions.checkNotNull(middlewareType, "Middleware type is empty");

        // Update status to [GROUP_APPROVE_PASSED]
        // If you need to change inlong group info after approve, just do in here
        this.updateStatus(groupId, GroupStatus.APPROVE_PASSED.getCode(), operator);

        LOGGER.info("success to update inlong group status after approve for groupId={}", groupId);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
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
