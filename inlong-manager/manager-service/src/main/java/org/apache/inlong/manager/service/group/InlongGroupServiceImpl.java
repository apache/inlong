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

package org.apache.inlong.manager.service.group;

import org.apache.inlong.common.bounded.Boundaries;
import org.apache.inlong.common.bounded.BoundaryType;
import org.apache.inlong.manager.common.auth.Authentication.AuthType;
import org.apache.inlong.manager.common.auth.SecretTokenAuthentication;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.OperationTarget;
import org.apache.inlong.manager.common.enums.ProcessName;
import org.apache.inlong.manager.common.enums.TenantUserTypeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupExtEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamExtEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.entity.TenantClusterTagEntity;
import org.apache.inlong.manager.dao.entity.TenantUserRoleEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.dao.mapper.TenantClusterTagEntityMapper;
import org.apache.inlong.manager.dao.mapper.TenantUserRoleEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.common.BatchResult;
import org.apache.inlong.manager.pojo.common.OrderFieldEnum;
import org.apache.inlong.manager.pojo.common.OrderTypeEnum;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.group.GroupFullInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.pojo.group.InlongGroupBriefInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupCountResponse;
import org.apache.inlong.manager.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.pojo.group.InlongGroupTopicInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupTopicRequest;
import org.apache.inlong.manager.pojo.schedule.OfflineJobRequest;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfoRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sort.BaseSortConf;
import org.apache.inlong.manager.pojo.sort.BaseSortConf.SortType;
import org.apache.inlong.manager.pojo.sort.FlinkSortConf;
import org.apache.inlong.manager.pojo.sort.UserDefinedSortConf;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.tenant.InlongTenantInfo;
import org.apache.inlong.manager.pojo.user.InlongRoleInfo;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.GroupResourceProcessForm;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.schedule.ScheduleOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.source.SourceOperatorFactory;
import org.apache.inlong.manager.service.source.StreamSourceOperator;
import org.apache.inlong.manager.service.source.bounded.BoundedSourceType;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.apache.inlong.manager.service.tenant.InlongTenantService;
import org.apache.inlong.manager.service.user.InlongRoleService;
import org.apache.inlong.manager.service.user.UserService;
import org.apache.inlong.manager.service.workflow.WorkflowService;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.inlong.common.constant.ClusterSwitch.BACKUP_CLUSTER_TAG;
import static org.apache.inlong.common.constant.ClusterSwitch.BACKUP_MQ_RESOURCE;
import static org.apache.inlong.common.constant.ClusterSwitch.CLUSTER_SWITCH_TIME;
import static org.apache.inlong.common.constant.ClusterSwitch.FINISH_SWITCH_INTERVAL_MIN;
import static org.apache.inlong.manager.common.consts.InlongConstants.DATASYNC_OFFLINE_MODE;
import static org.apache.inlong.manager.pojo.common.PageRequest.MAX_PAGE_SIZE;
import static org.apache.inlong.manager.workflow.event.process.ProcessEventListener.EXECUTOR_SERVICE;

/**
 * Inlong group service layer implementation
 */
@Service
@Validated
public class InlongGroupServiceImpl implements InlongGroupService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongGroupServiceImpl.class);

    @Value("${sort.enable.zookeeper:false}")
    private boolean enableZookeeper;

    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongGroupExtEntityMapper groupExtMapper;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private StreamSinkService streamSinkService;
    @Autowired
    private StreamSourceEntityMapper streamSourceMapper;
    @Autowired
    private TenantClusterTagEntityMapper tenantClusterTagMapper;
    @Autowired
    private InlongStreamExtEntityMapper streamExtMapper;
    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private WorkflowService workflowService;
    @Autowired
    private InlongClusterEntityMapper clusterEntityMapper;

    @Autowired
    private InlongGroupOperatorFactory groupOperatorFactory;
    @Autowired
    private SourceOperatorFactory sourceOperatorFactory;
    @Autowired
    private InlongTenantService tenantService;
    @Autowired
    private InlongRoleService inlongRoleService;
    @Autowired
    private TenantUserRoleEntityMapper tenantUserRoleEntityMapper;
    @Autowired
    private UserService userService;

    @Autowired
    ScheduleOperator scheduleOperator;

    /**
     * Check whether modification is supported under the current group status, and which fields can be modified.
     *
     * @param entity original inlong group entity
     * @param request request of updated
     * @param operator current operator
     */
    private static void doUpdateCheck(InlongGroupEntity entity, InlongGroupRequest request, String operator) {
        if (entity == null || request == null) {
            return;
        }

        // check whether the current status supports modification
        GroupStatus curStatus = GroupStatus.forCode(entity.getStatus());
        if (GroupStatus.notAllowedUpdate(curStatus)) {
            String errMsg = String.format("Current status=%s is not allowed to update", curStatus);
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.GROUP_UPDATE_NOT_ALLOWED, errMsg);
        }

        // mq type cannot be changed
        if (!entity.getMqType().equals(request.getMqType()) && !GroupStatus.allowedUpdateMQ(curStatus)) {
            String errMsg = String.format("Current status=%s is not allowed to update MQ type", curStatus);
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.GROUP_UPDATE_NOT_ALLOWED, errMsg);
        }
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public String save(InlongGroupRequest request, String operator) {
        LOGGER.debug("begin to save inlong group={} by user={}", request, operator);
        Preconditions.expectNotNull(request, "inlong group request cannot be empty");

        String groupId = request.getInlongGroupId();
        InlongGroupEntity entity = groupMapper.selectByGroupIdWithoutTenant(groupId);
        if (entity != null) {
            LOGGER.error("groupId={} has already exists", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_DUPLICATE);
        }

        if (request.getEnableZookeeper() == null) {
            request.setEnableZookeeper(enableZookeeper ? InlongConstants.ENABLE_ZK : InlongConstants.DISABLE_ZK);
        }
        InlongGroupOperator instance = groupOperatorFactory.getInstance(request.getMqType());
        groupId = instance.saveOpt(request, operator);

        // save ext info
        this.saveOrUpdateExt(groupId, request.getExtList());

        // save schedule info for offline group
        if (DATASYNC_OFFLINE_MODE.equals(request.getInlongGroupMode())) {
            constrainStartAndEndTime(request);
            scheduleOperator.saveOpt(CommonBeanUtils.copyProperties(request, ScheduleInfoRequest::new), operator);
        }

        LOGGER.info("success to save inlong group for groupId={} by user={}", groupId, operator);
        return groupId;
    }

    /**
     * Add constraints to the start and end time of the offline synchronization group.
     * 1. startTime must >= current time
     * 2. endTime must >= startTime
     * */
    private void constrainStartAndEndTime(InlongGroupRequest request) {
        Timestamp startTime = request.getStartTime();
        Timestamp endTime = request.getEndTime();
        Preconditions.expectTrue(startTime != null && endTime != null, "start time or end time cannot be empty");
        long currentTime = System.currentTimeMillis();
        if (startTime.getTime() < currentTime) {
            Timestamp newStartTime = new Timestamp(currentTime);
            request.setStartTime(newStartTime);
            LOGGER.warn("start time is less than current time, re-set to current time for groupId={}, "
                    + "startTime={}, newStartTime={}", request.getInlongGroupId(), startTime, newStartTime);
        }
        if (request.getStartTime().getTime() > endTime.getTime()) {
            request.setEndTime(request.getStartTime());
            LOGGER.warn("end time is less than start time, re-set end time to start time for groupId={}, "
                    + "endTime={}, newEndTime={}", request.getInlongGroupId(), endTime, request.getEndTime());
        }
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public List<BatchResult> batchSave(List<InlongGroupRequest> groupRequestList, String operator) {
        List<BatchResult> resultList = new ArrayList<>();
        for (InlongGroupRequest groupRequest : groupRequestList) {
            BatchResult result = BatchResult.builder()
                    .uniqueKey(groupRequest.getInlongGroupId())
                    .operationTarget(OperationTarget.GROUP)
                    .build();
            try {
                this.save(groupRequest, operator);
                result.setSuccess(true);
            } catch (Exception e) {
                LOGGER.error("failed to save inlong group for groupId={}", groupRequest.getInlongGroupId(), e);
                result.setSuccess(false);
                result.setErrMsg(e.getMessage());
            }
            resultList.add(result);
        }
        return resultList;
    }

    @Override
    public Boolean exist(String groupId) {
        Preconditions.expectNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
        InlongGroupEntity entity = groupMapper.selectByGroupIdWithoutTenant(groupId);
        LOGGER.debug("success to check inlong group {}, exist? {}", groupId, entity != null);
        return entity != null;
    }

    private boolean isScheduleInfoExist(InlongGroupEntity entity) {
        return DATASYNC_OFFLINE_MODE.equals(entity.getInlongGroupMode())
                && scheduleOperator.scheduleInfoExist(entity.getInlongGroupId());
    }

    @Override
    public InlongGroupInfo get(String groupId) {
        Preconditions.expectNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        InlongGroupOperator instance = groupOperatorFactory.getInstance(entity.getMqType());
        InlongGroupInfo groupInfo = instance.getFromEntity(entity);

        // get all ext info
        List<InlongGroupExtEntity> extEntityList = groupExtMapper.selectByGroupId(groupId);
        List<InlongGroupExtInfo> extList = CommonBeanUtils.copyListProperties(extEntityList, InlongGroupExtInfo::new);
        groupInfo.setExtList(extList);
        List<InlongStreamExtEntity> streamExtEntities = streamExtMapper.selectByRelatedId(groupId, null);
        BaseSortConf sortConf = buildSortConfig(streamExtEntities);
        groupInfo.setSortConf(sortConf);
        if (DATASYNC_OFFLINE_MODE.equals(entity.getInlongGroupMode())) {
            // get schedule info and set into group info
            fillInScheduleInfo(entity, groupInfo);
        }
        LOGGER.debug("success to get inlong group for groupId={}", groupId);
        return groupInfo;
    }

    private void fillInScheduleInfo(InlongGroupEntity entity, InlongGroupInfo groupInfo) {
        if (isScheduleInfoExist(entity)) {
            ScheduleInfo scheduleInfo = scheduleOperator.getScheduleInfo(entity.getInlongGroupId());
            int groupVersion = groupInfo.getVersion();
            CommonBeanUtils.copyProperties(scheduleInfo, groupInfo);
            groupInfo.setVersion(groupVersion);
        }
    }

    @Override
    public String getTenant(String groupId, String operator) {
        InlongGroupEntity groupEntity = groupMapper.selectByGroupIdWithoutTenant(groupId);
        String tenant = groupEntity.getTenant();
        if (Objects.equals(InlongConstants.DEFAULT_PULSAR_TENANT, tenant)) {
            return tenant;
        }
        InlongTenantInfo tenantInfo = tenantService.getByName(tenant);
        if (tenantInfo == null) {
            String errMsg = String.format("tenant=[%s] not found", tenant);
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongRoleInfo inlongRoleInfo = inlongRoleService.getByUsername(operator);
        TenantUserRoleEntity tenantRoleInfo = tenantUserRoleEntityMapper.selectByUsernameAndTenant(operator, tenant);
        if (inlongRoleInfo == null && tenantRoleInfo == null) {
            String errMsg = String.format("user=[%s] has no privilege for tenant=[%s]", operator, tenant);
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }
        return tenant;
    }

    @Override
    public InlongGroupCountResponse countGroupByUser(String operator, Integer inlongGroupMode) {
        InlongGroupCountResponse countVO = new InlongGroupCountResponse();
        List<Map<String, Object>> statusCount = groupMapper.countGroupByUser(operator, inlongGroupMode);
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

        LOGGER.debug("success to count inlong group for operator={}", operator);
        return countVO;
    }

    @Override
    public InlongGroupTopicInfo getTopic(String groupId) {
        // the group info will not null in get() method
        InlongGroupInfo groupInfo = this.get(groupId);
        InlongGroupOperator groupOperator = groupOperatorFactory.getInstance(groupInfo.getMqType());
        InlongGroupTopicInfo topicInfo = groupOperator.getTopic(groupInfo);

        // set the base params
        topicInfo.setInlongGroupId(groupId);
        String clusterTag = groupInfo.getInlongClusterTag();
        topicInfo.setInlongClusterTag(clusterTag);

        // assert: each MQ type has a corresponding type of cluster
        List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(clusterTag, groupInfo.getMqType());
        topicInfo.setClusterInfos(clusterInfos);

        LOGGER.debug("success to get topic for groupId={}, result={}", groupId, topicInfo);
        return topicInfo;
    }

    @Override
    public InlongGroupTopicInfo getBackupTopic(String groupId) {
        // backup topic info saved in the ext table
        InlongGroupExtEntity extEntity = groupExtMapper.selectByUniqueKey(groupId, BACKUP_CLUSTER_TAG);
        if (extEntity == null || StringUtils.isBlank(extEntity.getKeyValue())) {
            LOGGER.warn("not found any backup topic for groupId={}", groupId);
            return null;
        }

        // the group info will not null in get() method
        InlongGroupInfo groupInfo = this.get(groupId);
        InlongGroupOperator groupOperator = groupOperatorFactory.getInstance(groupInfo.getMqType());
        InlongGroupTopicInfo backupTopicInfo = groupOperator.getBackupTopic(groupInfo);

        // set the base params
        backupTopicInfo.setInlongGroupId(groupId);
        String backupClusterTag = extEntity.getKeyValue();
        backupTopicInfo.setInlongClusterTag(backupClusterTag);

        // set backup cluster info
        // assert: each MQ type has a corresponding type of cluster
        List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(backupClusterTag, groupInfo.getMqType());
        backupTopicInfo.setClusterInfos(clusterInfos);

        LOGGER.debug("success to get backup topic for groupId={}, result={}", groupId, backupTopicInfo);
        return backupTopicInfo;
    }

    @Override
    public PageResult<InlongGroupBriefInfo> listBrief(InlongGroupPageRequest request) {
        if (request.getPageSize() > MAX_PAGE_SIZE) {
            LOGGER.warn("list inlong groups, change page size from {} to {}", request.getPageSize(), MAX_PAGE_SIZE);
            request.setPageSize(MAX_PAGE_SIZE);
        }
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        OrderFieldEnum.checkOrderField(request);
        OrderTypeEnum.checkOrderType(request);
        Page<InlongGroupEntity> entityPage = (Page<InlongGroupEntity>) groupMapper.selectByCondition(request);
        PageResult<InlongGroupBriefInfo> pageResult = PageResult.fromPage(entityPage)
                .map(entity -> CommonBeanUtils.copyProperties(entity, InlongGroupBriefInfo::new));

        List<InlongGroupBriefInfo> briefInfos = pageResult.getList();

        // list all related sources
        if (request.isListSources() && CollectionUtils.isNotEmpty(briefInfos)) {
            Set<String> groupIds = briefInfos.stream().map(InlongGroupBriefInfo::getInlongGroupId)
                    .collect(Collectors.toSet());
            List<StreamSourceEntity> sourceEntities = streamSourceMapper.selectByGroupIds(new ArrayList<>(groupIds));
            Map<String, List<StreamSource>> sourceMap = Maps.newHashMap();
            sourceEntities.forEach(sourceEntity -> {
                StreamSourceOperator operation = sourceOperatorFactory.getInstance(sourceEntity.getSourceType());
                StreamSource source = operation.getFromEntity(sourceEntity);
                sourceMap.computeIfAbsent(sourceEntity.getInlongGroupId(), k -> Lists.newArrayList()).add(source);
            });
            briefInfos.forEach(group -> {
                List<StreamSource> sources = sourceMap.getOrDefault(group.getInlongGroupId(), Lists.newArrayList());
                group.setStreamSources(sources);
            });
        }

        LOGGER.debug("success to list inlong group for {}", request);
        return pageResult;
    }

    @Override
    public List<InlongGroupBriefInfo> listBrief(InlongGroupPageRequest request, UserInfo opInfo) {
        // filter records;
        List<InlongGroupEntity> filterGroupEntities = new ArrayList<>();
        OrderFieldEnum.checkOrderField(request);
        OrderTypeEnum.checkOrderType(request);
        for (InlongGroupEntity groupEntity : groupMapper.selectByCondition(request)) {
            // only the person in charges can query
            if (!opInfo.getAccountType().equals(TenantUserTypeEnum.TENANT_ADMIN.getCode())) {
                List<String> inCharges = Arrays.asList(groupEntity.getInCharges().split(InlongConstants.COMMA));
                if (!inCharges.contains(opInfo.getName())) {
                    continue;
                }
            }
            filterGroupEntities.add(groupEntity);
        }
        List<InlongGroupBriefInfo> briefInfos =
                CommonBeanUtils.copyListProperties(filterGroupEntities, InlongGroupBriefInfo::new);
        // list all related sources
        if (request.isListSources() && CollectionUtils.isNotEmpty(briefInfos)) {
            Set<String> groupIds = briefInfos.stream().map(InlongGroupBriefInfo::getInlongGroupId)
                    .collect(Collectors.toSet());
            List<StreamSourceEntity> sourceEntities = streamSourceMapper.selectByGroupIds(new ArrayList<>(groupIds));
            Map<String, List<StreamSource>> sourceMap = Maps.newHashMap();
            sourceEntities.forEach(sourceEntity -> {
                StreamSourceOperator operation = sourceOperatorFactory.getInstance(sourceEntity.getSourceType());
                StreamSource source = operation.getFromEntity(sourceEntity);
                sourceMap.computeIfAbsent(sourceEntity.getInlongGroupId(), k -> Lists.newArrayList()).add(source);
            });
            briefInfos.forEach(group -> {
                List<StreamSource> sources = sourceMap.getOrDefault(group.getInlongGroupId(), Lists.newArrayList());
                group.setStreamSources(sources);
            });
        }
        return briefInfos;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
    public String update(InlongGroupRequest request, String operator) {
        LOGGER.debug("begin to update inlong group={} by user={}", request, operator);

        String groupId = request.getInlongGroupId();
        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        userService.checkUser(entity.getInCharges(), operator,
                "Current user does not have permission to update group info");
        chkUnmodifiableParams(entity, request);
        // check whether the current status can be modified
        doUpdateCheck(entity, request, operator);

        if (request.getEnableZookeeper() == null) {
            request.setEnableZookeeper(enableZookeeper ? InlongConstants.ENABLE_ZK : InlongConstants.DISABLE_ZK);
        }

        InlongGroupOperator instance = groupOperatorFactory.getInstance(request.getMqType());
        instance.updateOpt(request, operator);

        // save ext info
        this.saveOrUpdateExt(groupId, request.getExtList());

        // save schedule info for offline group
        if (DATASYNC_OFFLINE_MODE.equals(request.getInlongGroupMode())) {
            constrainStartAndEndTime(request);
            ScheduleInfoRequest scheduleRequest = CommonBeanUtils.copyProperties(request, ScheduleInfoRequest::new);
            if (scheduleOperator.scheduleInfoExist(groupId)) {
                scheduleRequest.setVersion(scheduleOperator.getScheduleInfo(groupId).getVersion());
            }
            scheduleOperator.updateAndRegister(scheduleRequest, operator);
        }

        LOGGER.info("success to update inlong group for groupId={} by user={}", groupId, operator);
        return groupId;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
    public Boolean updateStatus(String groupId, Integer status, String operator) {
        LOGGER.info("begin to update group status to [{}] for groupId={} by user={}", status, groupId, operator);
        Preconditions.expectNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
        InlongGroupEntity entity = groupMapper.selectByGroupIdForUpdate(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        GroupStatus curState = GroupStatus.forCode(entity.getStatus());
        GroupStatus nextState = GroupStatus.forCode(status);
        if (GroupStatus.notAllowedTransition(curState, nextState)) {
            String errorMsg = String.format("Current status=%s is not allowed to transfer to state=%s",
                    curState, nextState);
            LOGGER.error(errorMsg);
            throw new BusinessException(errorMsg);
        }

        groupMapper.updateStatus(groupId, status, operator);
        LOGGER.info("success to update group status to [{}] for groupId={} by user={}", status, groupId, operator);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, propagation = Propagation.REQUIRES_NEW)
    public void updateAfterApprove(InlongGroupApproveRequest approveRequest, String operator) {
        LOGGER.debug("begin to update inlong group after approve={}", approveRequest);
        String groupId = approveRequest.getInlongGroupId();

        // only the [TO_BE_APPROVAL] status allowed the passing operation
        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        if (entity == null) {
            throw new BusinessException("inlong group not found with group id=" + groupId);
        }
        if (!Objects.equals(GroupStatus.TO_BE_APPROVAL.getCode(), entity.getStatus())) {
            throw new BusinessException("inlong group status [wait_approval] not allowed to approve again");
        }

        // bind cluster tag and update status to [GROUP_APPROVE_PASSED]
        if (StringUtils.isNotBlank(approveRequest.getInlongClusterTag())) {
            entity.setInlongGroupId(groupId);
            entity.setInlongClusterTag(approveRequest.getInlongClusterTag());
            entity.setStatus(GroupStatus.APPROVE_PASSED.getCode());
            if (approveRequest.getDataReportType() != null
                    && !Objects.equals(approveRequest.getDataReportType(), entity.getDataReportType())) {
                entity.setDataReportType(approveRequest.getDataReportType());
            }
            entity.setModifier(operator);
            int rowCount = groupMapper.updateByIdentifierSelective(entity);
            if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
                LOGGER.error("inlong group has already updated with group id={}, curVersion={}",
                        groupId, entity.getVersion());
                throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
            }
        } else {
            this.updateStatus(groupId, GroupStatus.APPROVE_PASSED.getCode(), operator);
        }

        LOGGER.info("success to update inlong group status after approve for groupId={}", groupId);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public void saveOrUpdateExt(String groupId, List<InlongGroupExtInfo> exts) {
        if (CollectionUtils.isEmpty(exts)) {
            return;
        }
        List<InlongGroupExtEntity> entityList =
                CommonBeanUtils.copyListProperties(exts, InlongGroupExtEntity::new);
        for (InlongGroupExtEntity entity : entityList) {
            entity.setInlongGroupId(groupId);
        }
        groupExtMapper.insertOnDuplicateKeyUpdate(entityList);
    }

    @Override
    public List<InlongGroupTopicInfo> listTopics(InlongGroupTopicRequest request) {
        LOGGER.info("start to list group topic infos, request={}", request);
        Preconditions.expectNotEmpty(request.getClusterTag(), "cluster tag should not be empty");
        List<InlongGroupEntity> groupEntities = groupMapper.selectByTopicRequest(request);
        List<InlongGroupTopicInfo> topicInfos = new ArrayList<>();
        for (InlongGroupEntity entity : groupEntities) {
            topicInfos.add(this.getTopic(entity.getInlongGroupId()));
        }
        LOGGER.info("success list group topic infos under clusterTag={}, size={}",
                request.getClusterTag(), topicInfos.size());
        return topicInfos;
    }

    @Override
    public Map<String, Object> detail(String groupId) {
        InlongGroupInfo groupInfo = this.get(groupId);
        InlongGroupOperator instance = groupOperatorFactory.getInstance(groupInfo.getMqType());
        return instance.getDetailInfo(groupInfo);
    }

    @Override
    public InlongGroupInfo doDeleteCheck(String groupId, String operator) {
        InlongGroupInfo groupInfo = this.get(groupId);
        // only the person in charges can update
        List<String> inCharges = Arrays.asList(groupInfo.getInCharges().split(InlongConstants.COMMA));
        if (!inCharges.contains(operator)) {
            throw new BusinessException(ErrorCodeEnum.GROUP_PERMISSION_DENIED,
                    String.format("user [%s] has no privilege for the inlong group", operator));
        }
        // determine whether the current status can be deleted
        GroupStatus curState = GroupStatus.forCode(groupInfo.getStatus());
        if (GroupStatus.notAllowedTransition(curState, GroupStatus.CONFIG_DELETING)) {
            throw new BusinessException(ErrorCodeEnum.GROUP_DELETE_NOT_ALLOWED,
                    String.format("current group status=%s was not allowed to delete", curState));
        }

        // If the status not allowed deleting directly, you need to delete the related "inlong_stream" first,
        // otherwise, all associated info will be logically deleted.
        if (GroupStatus.deleteStreamFirst(curState)) {
            int count = streamService.selectCountByGroupId(groupId);
            if (count >= 1) {
                throw new BusinessException(ErrorCodeEnum.GROUP_DELETE_HAS_STREAM,
                        String.format("groupId=%s have [%s] inlong streams, deleted failed", groupId, count));
            }
        }

        return groupInfo;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Boolean delete(String groupId, String operator) {
        LOGGER.info("begin to delete inlong group for groupId={} by user={}", groupId, operator);
        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        Preconditions.expectNotNull(entity, ErrorCodeEnum.GROUP_NOT_FOUND.getMessage());

        // before deleting an inlong group, delete all inlong streams, sources, sinks, and other info under it
        if (GroupStatus.allowedDeleteSubInfos(GroupStatus.forCode(entity.getStatus()))) {
            streamService.logicDeleteAll(groupId, operator);
        }

        entity.setIsDeleted(entity.getId());
        entity.setStatus(GroupStatus.CONFIG_DELETED.getCode());
        entity.setModifier(operator);
        int rowCount = groupMapper.updateByIdentifierSelective(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error("inlong group has already updated for groupId={} curVersion={}", groupId, entity.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }

        // logically delete the associated extension info
        groupExtMapper.logicDeleteAllByGroupId(groupId);

        // remove schedule
        if (DATASYNC_OFFLINE_MODE.equals(entity.getInlongGroupMode())) {
            try {
                scheduleOperator.deleteByGroupIdOpt(entity.getInlongGroupId(), operator);
            } catch (Exception e) {
                LOGGER.warn("failed to delete schedule info for groupId={}, error msg: {}", groupId, e.getMessage());
            }
        }

        LOGGER.info("success to delete group and group ext property for groupId={} by user={}", groupId, operator);
        return true;
    }

    private BaseSortConf buildSortConfig(List<InlongStreamExtEntity> extInfos) {
        Map<String, String> extMap = new HashMap<>();
        extInfos.forEach(extInfo -> extMap.put(extInfo.getKeyName(), extInfo.getKeyValue()));
        String type = extMap.get(InlongConstants.SORT_TYPE);
        if (StringUtils.isBlank(type)) {
            return null;
        }
        SortType sortType = SortType.forType(type);
        switch (sortType) {
            case FLINK:
                return createFlinkSortConfig(extMap);
            case USER_DEFINED:
                return createUserDefinedSortConfig(extMap);
            default:
                LOGGER.warn("unsupported sort config for sortType: {}", sortType);
                return null;
        }
    }

    private FlinkSortConf createFlinkSortConfig(Map<String, String> extMap) {
        FlinkSortConf sortConf = new FlinkSortConf();
        sortConf.setServiceUrl(extMap.get(InlongConstants.SORT_URL));
        String properties = extMap.get(InlongConstants.SORT_PROPERTIES);
        if (StringUtils.isNotBlank(properties)) {
            sortConf.setProperties(JsonUtils.parseObject(properties,
                    new TypeReference<Map<String, String>>() {
                    }));
        } else {
            sortConf.setProperties(Maps.newHashMap());
        }
        String authenticationType = extMap.get(InlongConstants.SORT_AUTHENTICATION_TYPE);
        if (StringUtils.isNotBlank(authenticationType)) {
            AuthType authType = AuthType.forType(authenticationType);
            Preconditions.expectTrue(authType == AuthType.SECRET_AND_TOKEN,
                    "Only support SECRET_AND_TOKEN for flink sort auth");
            String authentication = extMap.get(InlongConstants.SORT_AUTHENTICATION);
            Map<String, String> authProperties = JsonUtils.parseObject(authentication,
                    new TypeReference<Map<String, String>>() {
                    });
            SecretTokenAuthentication secretTokenAuthentication = new SecretTokenAuthentication();
            secretTokenAuthentication.configure(authProperties);
            sortConf.setAuthentication(secretTokenAuthentication);
        }
        return sortConf;
    }

    private UserDefinedSortConf createUserDefinedSortConfig(Map<String, String> extMap) {
        UserDefinedSortConf sortConf = new UserDefinedSortConf();
        String sortName = extMap.get(InlongConstants.SORT_NAME);
        sortConf.setSortName(sortName);
        String properties = extMap.get(InlongConstants.SORT_PROPERTIES);
        if (StringUtils.isNotBlank(properties)) {
            sortConf.setProperties(JsonUtils.parseObject(properties,
                    new TypeReference<Map<String, String>>() {
                    }));
        } else {
            sortConf.setProperties(Maps.newHashMap());
        }
        return sortConf;
    }

    private void chkUnmodifiableParams(InlongGroupEntity entity, InlongGroupRequest request) {
        // check mqType
        Preconditions.expectTrue(
                Objects.equals(entity.getMqType(), request.getMqType())
                        || Objects.equals(entity.getStatus(), GroupStatus.TO_BE_SUBMIT.getCode()),
                "mqType not allowed modify");
        // check record version
        Preconditions.expectEquals(entity.getVersion(), request.getVersion(),
                ErrorCodeEnum.CONFIG_EXPIRED,
                String.format("record has expired with record version=%d, request version=%d",
                        entity.getVersion(), request.getVersion()));
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
    public Boolean startTagSwitch(String groupId, String clusterTag) {
        LOGGER.info("start to switch cluster tag for group={}, target tag={}", groupId, clusterTag);

        InlongGroupInfo groupInfo = this.get(groupId);

        // check if the group mode is data sync mode
        if (InlongConstants.DATASYNC_REALTIME_MODE.equals(groupInfo.getInlongGroupMode())) {
            String errMSg = String.format("no need to switch sync mode group = {}", groupId);
            LOGGER.error(errMSg);
            throw new BusinessException(errMSg);
        }

        // check if the group is under switching
        List<InlongGroupExtInfo> groupExt = groupInfo.getExtList();
        Set<String> keys = groupExt.stream()
                .map(InlongGroupExtInfo::getKeyName)
                .collect(Collectors.toSet());

        if (keys.contains(BACKUP_CLUSTER_TAG) || keys.contains(BACKUP_MQ_RESOURCE)) {
            String errMsg = String.format("switch failed, current group is under switching, group=[%s]", groupId);
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        // check if the cluster tag is under current tenant
        InlongGroupEntity groupEntity = groupMapper.selectByGroupId(groupId);
        if (groupEntity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        TenantClusterTagEntity tenantClusterTag =
                tenantClusterTagMapper.selectByUniqueKey(clusterTag, groupEntity.getTenant());
        if (tenantClusterTag == null) {
            LOGGER.error("tenant cluster not found for tenant={}, clusterTag={}", groupEntity.getTenant(), clusterTag);
            throw new BusinessException(ErrorCodeEnum.TENANT_CLUSTER_TAG_NOT_FOUND);
        }

        // check if all sink related sort cluster has the target cluster tag
        List<StreamSink> sinks = streamSinkService.listSink(groupEntity.getInlongGroupId(), null);
        for (StreamSink sink : sinks) {
            String clusterName = sink.getInlongClusterName();
            List<InlongClusterEntity> clusterEntity =
                    clusterEntityMapper.selectByKey(clusterTag, clusterName,
                            SinkType.relatedSortClusterType(sink.getSinkType()));
            if (CollectionUtils.isEmpty(clusterEntity) || clusterEntity.size() != 1) {
                String errMsg = String.format("find no cluster or multiple cluster with clusterName=[%s]", clusterName);
                LOGGER.error(errMsg);
                throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND, errMsg);
            }

        }

        // config cluster tag and backup_cluster_tag
        UserInfo userInfo = LoginUserUtils.getLoginUser();
        InlongGroupRequest request = groupInfo.genRequest();
        String oldClusterTag = request.getInlongClusterTag();
        request.setInlongClusterTag(clusterTag);
        request.getExtList().add(new InlongGroupExtInfo(null, groupId, BACKUP_CLUSTER_TAG, oldClusterTag));
        request.getExtList().add(new InlongGroupExtInfo(null, groupId, BACKUP_MQ_RESOURCE, request.getMqResource()));
        request.getExtList().add(new InlongGroupExtInfo(null, groupId, CLUSTER_SWITCH_TIME,
                LocalDateTime.now().toString()));
        this.update(request, userInfo.getName());

        // trigger group workflow to rebuild configs
        this.triggerWorkFlow(groupInfo, userInfo);
        LOGGER.info("success to switch cluster tag for group={}, target tag={}", groupId, clusterTag);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
    public Boolean finishTagSwitch(String groupId) {
        LOGGER.info("start to finish switch cluster tag for group={}", groupId);

        InlongGroupInfo groupInfo = this.get(groupId);
        UserInfo userInfo = LoginUserUtils.getLoginUser();

        // check whether the current status supports modification
        GroupStatus curStatus = GroupStatus.forCode(groupInfo.getStatus());
        if (GroupStatus.notAllowedUpdate(curStatus)) {
            String errMsg = String.format("Current status=%s is not allowed to update", curStatus);
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.GROUP_UPDATE_NOT_ALLOWED, errMsg);
        }

        // check if the group is under switching
        List<InlongGroupExtInfo> groupExt = groupInfo.getExtList();
        Map<String, InlongGroupExtInfo> extInfoMap = groupExt.stream()
                .collect(Collectors.toMap(InlongGroupExtInfo::getKeyName, v -> v));

        if (!extInfoMap.containsKey(BACKUP_CLUSTER_TAG) || !extInfoMap.containsKey(BACKUP_MQ_RESOURCE)) {
            String errMsg = String.format("finish switch failed, current group is not under switching, group=[%s]",
                    groupId);
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongGroupExtInfo switchTime = extInfoMap.get(CLUSTER_SWITCH_TIME);
        LocalDateTime switchStartTime =
                switchTime == null ? LocalDateTime.MIN : LocalDateTime.parse(switchTime.getKeyValue());

        // check the switch time
        LocalDateTime allowSwitchTime = switchStartTime.plusMinutes(FINISH_SWITCH_INTERVAL_MIN);
        if (LocalDateTime.now().isBefore(allowSwitchTime)) {
            String errMsg = String.format("finish switch failed, please retry until={}", allowSwitchTime);
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        // remove backup ext info
        removeExt(extInfoMap.get(BACKUP_CLUSTER_TAG));
        removeExt(extInfoMap.get(BACKUP_MQ_RESOURCE));
        removeExt(extInfoMap.get(CLUSTER_SWITCH_TIME));

        // trigger group workflow to rebuild configs
        this.triggerWorkFlow(groupInfo, userInfo);
        return true;
    }

    private void triggerWorkFlow(InlongGroupInfo groupInfo, UserInfo userInfo) {
        GroupResourceProcessForm processForm = new GroupResourceProcessForm();
        processForm.setGroupInfo(groupInfo);
        List<InlongStreamInfo> streamList = streamService.list(groupInfo.getInlongGroupId());
        processForm.setStreamInfos(streamList);
        EXECUTOR_SERVICE.execute(
                () -> workflowService.startAsync(ProcessName.CREATE_GROUP_RESOURCE, userInfo, processForm));
    }

    private void removeExt(InlongGroupExtInfo extInfo) {
        if (extInfo == null || extInfo.getId() == null) {
            return;
        }
        groupExtMapper.deleteByPrimaryKey(extInfo.getId());
    }

    @Override
    public List<GroupFullInfo> getGroupByClusterTag(String clusterTag) {
        List<InlongGroupEntity> groupEntities = groupMapper.selectByClusterTagWithoutTenant(clusterTag);
        if (CollectionUtils.isEmpty(groupEntities)) {
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        List<GroupFullInfo> groupInfoList = new ArrayList<>();
        for (InlongGroupEntity groupEntity : groupEntities) {
            // query mq information
            InlongGroupOperator instance = groupOperatorFactory.getInstance(groupEntity.getMqType());
            InlongGroupInfo groupInfo = instance.getFromEntity(groupEntity);
            List<InlongStreamInfo> streamInfos = streamService.list(groupInfo.getInlongGroupId());
            GroupFullInfo groupFullInfo = new GroupFullInfo();
            groupFullInfo.setGroupInfo(groupInfo);
            groupFullInfo.setStreamInfoList(streamInfos);
            groupInfoList.add(groupFullInfo);
        }

        return groupInfoList;
    }

    @Override
    public List<GroupFullInfo> getGroupByBackUpClusterTag(String clusterTag) {
        List<GroupFullInfo> groupInfoList = new ArrayList<>();
        List<String> groupIdList = groupExtMapper.selectGroupIdByKeyNameAndValue(BACKUP_CLUSTER_TAG, clusterTag);
        if (CollectionUtils.isEmpty(groupIdList)) {
            return groupInfoList;
        }
        List<InlongGroupEntity> groupEntities = groupMapper.selectByInlongGroupIds(groupIdList);
        if (CollectionUtils.isEmpty(groupEntities)) {
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        for (InlongGroupEntity groupEntity : groupEntities) {
            // query mq information
            InlongGroupOperator instance = groupOperatorFactory.getInstance(groupEntity.getMqType());
            InlongGroupInfo groupInfo = instance.getFromEntity(groupEntity);
            InlongGroupExtEntity backUpMqResource =
                    groupExtMapper.selectByUniqueKey(groupEntity.getInlongGroupId(), BACKUP_MQ_RESOURCE);
            if (backUpMqResource != null) {
                groupInfo.setMqResource(backUpMqResource.getKeyValue());
            }
            List<InlongStreamInfo> streamInfoList = streamService.listBackUp(groupEntity.getInlongGroupId());
            GroupFullInfo groupFullInfo = new GroupFullInfo();
            groupFullInfo.setGroupInfo(groupInfo);
            groupFullInfo.setStreamInfoList(streamInfoList);
            groupInfoList.add(groupFullInfo);
        }
        return groupInfoList;
    }

    @Override
    public Boolean submitOfflineJob(OfflineJobRequest request) {
        // 1. get stream info list
        String groupId = request.getGroupId();
        InlongGroupInfo groupInfo = get(groupId);
        if (groupInfo == null) {
            String msg = String.format("InLong group not found for group=%s", groupId);
            LOGGER.error(msg);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        List<InlongStreamInfo> streamInfoList = streamService.list(groupId);
        if (CollectionUtils.isEmpty(streamInfoList)) {
            LOGGER.warn("No stream info found for group {}, skip submit offline job", groupId);
            return false;
        }

        // check if source type is bounded source
        streamInfoList.forEach(this::checkBoundedSource);

        // get the source boundaries
        checkSourceBoundaryType(request.getBoundaryType());
        BoundaryType boundaryType = BoundaryType.getInstance(request.getBoundaryType());
        if (boundaryType == null) {
            throw new BusinessException(ErrorCodeEnum.BOUNDARY_TYPE_NOT_SUPPORTED,
                    String.format(ErrorCodeEnum.BOUNDARY_TYPE_NOT_SUPPORTED.getMessage(), request.getBoundaryType()));
        }
        Boundaries boundaries = new Boundaries(request.getLowerBoundary(), request.getUpperBoundary(), boundaryType);

        LOGGER.info("Check bounded source success, start to submitting offline job for group {}", groupId);

        return scheduleOperator.submitOfflineJob(groupId, streamInfoList, boundaries);
    }

    private void checkBoundedSource(InlongStreamInfo streamInfo) {
        streamInfo.getSourceList().forEach(stream -> {
            if (!BoundedSourceType.isBoundedSource(stream.getSourceType())) {
                throw new BusinessException(ErrorCodeEnum.BOUNDED_SOURCE_TYPE_NOT_SUPPORTED,
                        String.format(ErrorCodeEnum.BOUNDED_SOURCE_TYPE_NOT_SUPPORTED.getMessage(),
                                stream.getSourceType()));
            }
        });
    }

    private void checkSourceBoundaryType(String sourceBoundaryType) {
        if (!BoundaryType.isSupportBoundaryType(sourceBoundaryType)) {
            throw new BusinessException(ErrorCodeEnum.BOUNDARY_TYPE_NOT_SUPPORTED,
                    String.format(ErrorCodeEnum.BOUNDARY_TYPE_NOT_SUPPORTED.getMessage(),
                            sourceBoundaryType));
        }
    }

}
