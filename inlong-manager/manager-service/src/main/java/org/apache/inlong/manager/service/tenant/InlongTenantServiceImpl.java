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

package org.apache.inlong.manager.service.tenant;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.ProcessName;
import org.apache.inlong.manager.common.enums.SourceStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongConsumeEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.InlongTenantEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.entity.TenantClusterTagEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongConsumeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongTenantEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.dao.mapper.TenantClusterTagEntityMapper;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.tenant.InlongTenantInfo;
import org.apache.inlong.manager.pojo.tenant.InlongTenantPageRequest;
import org.apache.inlong.manager.pojo.tenant.InlongTenantRequest;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.pojo.workflow.ApproverRequest;
import org.apache.inlong.manager.service.core.WorkflowApproverService;
import org.apache.inlong.manager.service.user.TenantRoleService;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.pojo.user.UserRoleCode.INLONG_ADMIN;
import static org.apache.inlong.manager.pojo.user.UserRoleCode.INLONG_OPERATOR;
import static org.apache.inlong.manager.service.workflow.WorkflowDefinition.UT_ADMIN_NAME;

@Service
@Slf4j
public class InlongTenantServiceImpl implements InlongTenantService {

    @Autowired
    private InlongTenantEntityMapper inlongTenantEntityMapper;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongStreamEntityMapper streamMapper;
    @Autowired
    private StreamSinkEntityMapper sinkMapper;
    @Autowired
    private StreamSourceEntityMapper sourceMapper;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;
    @Autowired
    private InlongConsumeEntityMapper consumeEntityMapper;
    @Autowired
    private TenantClusterTagEntityMapper tenantClusterTagMapper;
    @Autowired
    private TenantRoleService tenantRoleService;
    @Autowired
    private WorkflowApproverService workflowApproverService;
    private ExecutorService executorService = new ScheduledThreadPoolExecutor(1);

    @Override
    public InlongTenantInfo getByName(String name) {
        InlongTenantEntity entity = inlongTenantEntityMapper.selectByName(name);
        if (entity == null) {
            log.warn("not found valid inlong tenant by name={}", name);
            return null;
        }
        return CommonBeanUtils.copyProperties(entity, InlongTenantInfo::new);
    }

    @Override
    public Integer save(InlongTenantRequest request) {
        String name = request.getName();
        InlongTenantEntity existEntity = inlongTenantEntityMapper.selectByName(name);
        if (existEntity != null) {
            String errMsg = String.format("tenant already exist for name=%s)", name);
            log.error(errMsg);
            throw new BusinessException(errMsg);
        }
        InlongTenantEntity entity = CommonBeanUtils.copyProperties(request, InlongTenantEntity::new);
        String operator = LoginUserUtils.getLoginUser().getName();
        entity.setCreator(operator);
        entity.setModifier(operator);
        inlongTenantEntityMapper.insert(entity);

        UserInfo loginUserInfo = LoginUserUtils.getLoginUser();
        executorService.submit(() -> {
            loginUserInfo.setTenant(request.getName());
            LoginUserUtils.setUserLoginInfo(loginUserInfo);
            saveDefaultWorkflowApprovers(ProcessName.APPLY_GROUP_PROCESS.name(),
                    UT_ADMIN_NAME, operator);
            saveDefaultWorkflowApprovers(ProcessName.APPLY_CONSUME_PROCESS.name(),
                    UT_ADMIN_NAME, operator);
            LoginUserUtils.removeUserLoginInfo();
        });

        return entity.getId();
    }

    private Integer saveDefaultWorkflowApprovers(String processName, String taskName, String approver) {
        ApproverRequest request = new ApproverRequest();
        request.setProcessName(processName);
        request.setApprovers(approver);
        request.setTaskName(taskName);
        return workflowApproverService.save(request, approver);
    }

    @Override
    public PageResult<InlongTenantInfo> listByCondition(InlongTenantPageRequest request, UserInfo userInfo) {
        if (request.getListByLoginUser()) {
            setTargetTenantList(request, userInfo);
        }

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongTenantEntity> entityPage = inlongTenantEntityMapper.selectByCondition(request);
        return PageResult.fromPage(entityPage)
                .map(entity -> CommonBeanUtils.copyProperties(entity, InlongTenantInfo::new));
    }

    @Override
    public Boolean update(InlongTenantRequest request) {
        InlongTenantEntity exist = inlongTenantEntityMapper.selectByName(request.getName());
        if (exist == null) {
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND,
                    String.format("tenant record not found by name=%s", request.getName()));
        }
        if (!exist.getId().equals(request.getId())) {
            throw new BusinessException(ErrorCodeEnum.RECORD_DUPLICATE,
                    String.format("tenant already exist for name=%s, required id=%s, exist id=%s",
                            request.getName(), request.getId(), exist.getId()));
        }
        InlongTenantEntity entity = CommonBeanUtils.copyProperties(request, InlongTenantEntity::new);
        String operator = LoginUserUtils.getLoginUser().getName();
        entity.setModifier(operator);
        int rowCount = inlongTenantEntityMapper.updateByIdSelective(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED,
                    String.format(
                            "failure to update tenant with name=%s, request version=%d, updated row=%d",
                            request.getName(), request.getVersion(), rowCount));
        }
        return true;
    }

    @Override
    public Boolean delete(String name) {
        String operator = LoginUserUtils.getLoginUser().getName();
        log.info("begin to delete inlong tenant name={} by user={}", name, operator);
        InlongTenantEntity inlongTenantEntity = inlongTenantEntityMapper.selectByName(name);
        // before deleting a tenant, check if all Groups of the tenant are in stop status
        List<InlongGroupEntity> groups = groupMapper.selectAllGroupsByTenant(name);
        List<InlongGroupEntity> notStopGroups =
                groups.stream().filter(
                        group -> !GroupStatus.CONFIG_DELETED.getCode().equals(group.getStatus()))
                        .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(notStopGroups)) {
            List<String> notStopGroupNames =
                    notStopGroups.stream().map(InlongGroupEntity::getName).collect(Collectors.toList());
            String errMsg = String.format(
                    "delete inlong tenant name=[%s] failed, the tenant's group=%s are not in stop status",
                    name, notStopGroupNames);
            log.error(errMsg);
            throw new BusinessException(errMsg);
        }
        // before deleting a tenant, check if all streamSource of the tenant‘s groups are in status 99.
        List<String> groupIds =
                groups.stream().map(InlongGroupEntity::getInlongGroupId).collect(Collectors.toList());
        List<StreamSourceEntity> sourceList = sourceMapper.selectByGroupIds(groupIds);
        List<StreamSourceEntity> noDisabledSources = sourceList.stream()
                .filter(source -> !SourceStatus.SOURCE_DISABLE.getCode().equals(source.getStatus()))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(noDisabledSources)) {
            List<String> noDisabledSourceNames =
                    noDisabledSources.stream().map(StreamSourceEntity::getSourceName).collect(Collectors.toList());
            String errMsg = String.format(
                    "delete inlong tenant name=[%s] failed, the streamSource=%s of the tenant's groups are not in status 99",
                    name, noDisabledSourceNames);
            log.error(errMsg);
            throw new BusinessException(errMsg);
        }
        int success = inlongTenantEntityMapper.deleteById(inlongTenantEntity.getId());
        Preconditions.expectTrue(success == InlongConstants.AFFECTED_ONE_ROW, "delete failed");
        log.info("success delete inlong tenant name={} by user={}", name, operator);
        return true;
    }

    @Override
    public Boolean migrate(String groupId, String to) {
        UserInfo userInfo = LoginUserUtils.getLoginUser();
        Preconditions.expectTrue(hasPermission(userInfo, to),
                String.format("user=[%s] has no permission to tenant=[%s]", userInfo.getName(), to));
        return doMigrate(groupId, userInfo.getTenant(), to);
    }

    public Boolean doMigrate(String groupId, String from, String to) {
        InlongGroupEntity group = groupMapper.selectByGroupId(groupId);
        Preconditions.expectNotNull(group, String.format("Not find group=%s in tenant=%s", groupId, from));

        // get related streams, consumes and tag;
        List<InlongStreamEntity> streamList = streamMapper.selectByGroupId(groupId);
        boolean streamMigrateResult = streamList.stream().allMatch(stream -> migrateStream(stream, from, to));
        log.info("migrate stream from source tenant={} to target tenant={} for groupId={}, result={}", from, to,
                groupId, streamMigrateResult);
        List<InlongConsumeEntity> consumeList = consumeEntityMapper.selectByGroupId(groupId);
        boolean consumeMigrateResult = this.migrateConsume(groupId, from, to, consumeList.size());
        boolean tagCopyResult = this.copyTenantTag(group.getInlongClusterTag(), from, to);
        boolean groupMigrateResult = this.migrateGroup(groupId, from, to);
        boolean migrateResult = streamMigrateResult && consumeMigrateResult && tagCopyResult && groupMigrateResult;
        log.info("migrate from source tenant={} to target tenant={} for groupId={}, result={}", from, to, groupId,
                migrateResult);
        return migrateResult;
    }

    public Boolean migrateStream(InlongStreamEntity stream, String from, String to) {
        List<StreamSinkEntity> sinks =
                sinkMapper.selectByRelatedId(stream.getInlongGroupId(), stream.getInlongStreamId());
        List<StreamSourceEntity> sources =
                sourceMapper.selectByRelatedId(stream.getInlongGroupId(), stream.getInlongStreamId(), null);
        boolean sinkMigrateResult = sinks.stream().allMatch(sink -> migrateStreamSink(sink, from, to));
        boolean sourceMigrateResult = sources.stream().allMatch(source -> migrateStreamSource(source, from, to));
        return sinkMigrateResult && sourceMigrateResult;
    }

    public Boolean migrateStreamSink(StreamSinkEntity sink, String from, String to) {
        String dataNodeName = sink.getDataNodeName();
        String type = sink.getSinkType();
        if (StringUtils.isAnyBlank(dataNodeName, type)) {
            return true;
        }
        DataNodeEntity dataNode = dataNodeEntityMapper.selectByUniqueKey(dataNodeName, type);
        if (dataNode == null) {
            return true;
        }

        String newName = copyDataNode(dataNodeName, type, from, to);
        sink.setDataNodeName(newName);
        if (StringUtils.isNotBlank(sink.getSortTaskName())) {
            sink.setSortTaskName(newName);
        }
        return sinkMapper.updateByIdSelective(sink) == InlongConstants.AFFECTED_ONE_ROW;

    }

    public Boolean migrateStreamSource(StreamSourceEntity source, String from, String to) {
        String dataNodeName = source.getDataNodeName();
        String type = source.getSourceType();
        if (StringUtils.isAnyBlank(dataNodeName, type)) {
            return true;
        }
        DataNodeEntity dataNode = dataNodeEntityMapper.selectByUniqueKey(dataNodeName, type);
        if (dataNode == null) {
            return true;
        }

        // use display name as new name
        String newName = copyDataNode(dataNodeName, type, from, to);
        source.setDataNodeName(newName);
        return sourceMapper.updateByPrimaryKeySelective(source) == InlongConstants.AFFECTED_ONE_ROW;
    }

    public String copyDataNode(String name, String type, String from, String to) {
        DataNodeEntity oldDatanode = dataNodeEntityMapper.selectByUniqueKey(name, type);
        oldDatanode.setTenant(to);
        List<DataNodeEntity> newDatanodeList = dataNodeEntityMapper.selectByIdSelective(oldDatanode);
        if (CollectionUtils.isNotEmpty(newDatanodeList)) {
            return newDatanodeList.get(0).getName();
        }
        String newName = UUID.randomUUID().toString();
        if (dataNodeEntityMapper.copy(name, type, from, to, newName) != InlongConstants.AFFECTED_ONE_ROW) {
            return null;
        }
        return newName;
    }

    public Boolean migrateConsume(String groupId, String from, String to, int size) {
        Boolean result = consumeEntityMapper.migrate(groupId, from, to) == size;
        log.info("migrate consume from source tenant={} to target tenant={} for groupId={}, result={}", from, to,
                groupId, result);
        return result;
    }

    public Boolean migrateGroup(String groupId, String from, String to) {
        Boolean result = groupMapper.migrate(groupId, from, to) == InlongConstants.AFFECTED_ONE_ROW;
        log.info("migrate group from source tenant={} to target tenant={} for groupId={}, result={}", from, to, groupId,
                result);
        return result;
    }

    public Boolean copyTenantTag(String clusterTag, String from, String to) {
        try {
            TenantClusterTagEntity existEntity = tenantClusterTagMapper.selectByUniqueKey(clusterTag, to);
            if (existEntity != null) {
                log.debug("tag name={} in tenant={} already exist", clusterTag, to);
                return true;
            }
            // use displayName as the new name
            tenantClusterTagMapper.copy(clusterTag, from, to);
            return true;
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLIntegrityConstraintViolationException) {
                log.debug("tag name={} in tenant={} already exist", clusterTag, to);
                return true;
            } else {
                throw e;
            }
        }
    }

    private boolean hasPermission(UserInfo userInfo, String tenant) {
        return tenantRoleService.getByUsernameAndTenant(userInfo.getName(), tenant) != null
                || isInlongRoles(userInfo);
    }

    public void setTargetTenantList(InlongTenantPageRequest request, UserInfo userInfo) {
        if (isInlongRoles(userInfo)) {
            // for inlong roles, they can get all tenant info.
            request.setTenantList(null);
            return;
        }

        List<String> tenants = tenantRoleService.listTenantByUsername(userInfo.getName());
        if (CollectionUtils.isEmpty(tenants)) {
            String errMsg = String.format("user=[%s] doesn't belong to any tenant, please contact administrator " +
                    "and get one tenant at least", userInfo.getName());
            log.error(errMsg);
            throw new BusinessException(errMsg);
        }
        request.setTenantList(tenants);
    }

    private boolean isInlongRoles(UserInfo userInfo) {
        return userInfo.getRoles().contains(INLONG_ADMIN) || userInfo.getRoles().contains(INLONG_OPERATOR);
    }
}
