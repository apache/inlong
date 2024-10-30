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

package org.apache.inlong.manager.service.cluster;

import org.apache.inlong.common.constant.Constants;
import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.common.heartbeat.ReportResourceType;
import org.apache.inlong.common.pojo.audit.AuditConfig;
import org.apache.inlong.common.pojo.audit.MQInfo;
import org.apache.inlong.common.pojo.dataproxy.DataProxyCluster;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfig;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfigResponse;
import org.apache.inlong.common.pojo.dataproxy.DataProxyNodeInfo;
import org.apache.inlong.common.pojo.dataproxy.DataProxyNodeResponse;
import org.apache.inlong.common.pojo.dataproxy.DataProxyTopicInfo;
import org.apache.inlong.common.pojo.dataproxy.MQClusterInfo;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.NodeStatus;
import org.apache.inlong.manager.common.enums.TenantUserTypeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterTagEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.TenantClusterTagEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterTagEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.TenantClusterTagEntityMapper;
import org.apache.inlong.manager.pojo.cluster.BindTagRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterTagPageRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterTagRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterTagResponse;
import org.apache.inlong.manager.pojo.cluster.TenantClusterTagInfo;
import org.apache.inlong.manager.pojo.cluster.TenantClusterTagPageRequest;
import org.apache.inlong.manager.pojo.cluster.TenantClusterTagRequest;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.dataproxy.DataProxyClusterDTO;
import org.apache.inlong.manager.pojo.cluster.dataproxy.DataProxyClusterNodeDTO;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterDTO;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.UpdateResult;
import org.apache.inlong.manager.pojo.group.InlongGroupBriefInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarDTO;
import org.apache.inlong.manager.pojo.stream.InlongStreamBriefInfo;
import org.apache.inlong.manager.pojo.tenant.InlongTenantInfo;
import org.apache.inlong.manager.pojo.user.InlongRoleInfo;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.service.cluster.node.InlongClusterNodeInstallOperator;
import org.apache.inlong.manager.service.cluster.node.InlongClusterNodeInstallOperatorFactory;
import org.apache.inlong.manager.service.cluster.node.InlongClusterNodeOperator;
import org.apache.inlong.manager.service.cluster.node.InlongClusterNodeOperatorFactory;
import org.apache.inlong.manager.service.cmd.CommandExecutor;
import org.apache.inlong.manager.service.cmd.CommandResult;
import org.apache.inlong.manager.service.repository.DataProxyConfigRepository;
import org.apache.inlong.manager.service.tenant.InlongTenantService;
import org.apache.inlong.manager.service.user.InlongRoleService;
import org.apache.inlong.manager.service.user.TenantRoleService;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.common.consts.InlongConstants.ALIVE_TIME_MS;
import static org.apache.inlong.manager.common.consts.InlongConstants.CORE_POOL_SIZE;
import static org.apache.inlong.manager.common.consts.InlongConstants.MAX_POOL_SIZE;
import static org.apache.inlong.manager.common.consts.InlongConstants.QUEUE_SIZE;
import static org.apache.inlong.manager.pojo.cluster.InlongClusterTagExtParam.packExtParams;
import static org.apache.inlong.manager.pojo.cluster.InlongClusterTagExtParam.unpackExtParams;

/**
 * Inlong cluster service layer implementation
 */
@Service
public class InlongClusterServiceImpl implements InlongClusterService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongClusterServiceImpl.class);
    private static final Gson GSON = new Gson();
    private final ExecutorService executorService = new ThreadPoolExecutor(
            CORE_POOL_SIZE,
            MAX_POOL_SIZE,
            ALIVE_TIME_MS,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(QUEUE_SIZE),
            new ThreadFactoryBuilder().setNameFormat("agent-install-%s").build(),
            new CallerRunsPolicy());
    private final LinkedBlockingQueue<ClusterNodeRequest> pendingInstallRequests = new LinkedBlockingQueue<>();

    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongStreamEntityMapper streamMapper;
    @Autowired
    private InlongClusterOperatorFactory clusterOperatorFactory;
    @Autowired
    private InlongClusterNodeOperatorFactory clusterNodeOperatorFactory;
    @Autowired
    private InlongClusterTagEntityMapper clusterTagMapper;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongClusterNodeEntityMapper clusterNodeMapper;
    @Autowired
    private TenantClusterTagEntityMapper tenantClusterTagMapper;
    @Autowired
    private InlongTenantService tenantService;
    @Autowired
    private InlongRoleService inlongRoleService;
    @Autowired
    private TenantRoleService tenantRoleService;
    @Autowired
    private InlongClusterNodeInstallOperatorFactory clusterNodeInstallOperatorFactory;
    @Autowired
    private CommandExecutor commandExecutor;

    @Lazy
    @Autowired
    private DataProxyConfigRepository proxyRepository;

    @PostConstruct
    private void startInstallTask() {
        processInstall();
        setReloadTimer();
        LOGGER.info("install task started successfully");
    }

    private void setReloadTimer() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        long reloadInterval = 60000L;
        executorService.scheduleWithFixedDelay(this::processInstall, reloadInterval, reloadInterval,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public Integer saveTag(ClusterTagRequest request, String operator) {
        LOGGER.debug("begin to save cluster tag {}", request);
        Preconditions.expectNotNull(request, "inlong cluster request cannot be empty");
        Preconditions.expectNotBlank(request.getClusterTag(), ErrorCodeEnum.INVALID_PARAMETER,
                "cluster tag cannot be empty");

        // check if the cluster tag already exist
        String clusterTag = request.getClusterTag();
        InlongClusterTagEntity exist = clusterTagMapper.selectByTag(clusterTag);
        if (exist != null) {
            String errMsg = String.format("inlong cluster tag [%s] already exist", clusterTag);
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongClusterTagEntity entity = CommonBeanUtils.copyProperties(request, InlongClusterTagEntity::new);
        request.setExtParams(entity.getExtParams());
        String extParam = packExtParams(request);
        entity.setExtParams(extParam);
        entity.setCreator(operator);
        entity.setModifier(operator);
        clusterTagMapper.insert(entity);

        if (CollectionUtils.isNotEmpty(request.getTenantList())) {
            TenantClusterTagRequest tagRequest = new TenantClusterTagRequest();
            tagRequest.setClusterTag(clusterTag);
            request.getTenantList().forEach(tenant -> {
                tagRequest.setTenant(tenant);
                this.saveTenantTag(tagRequest, operator);
            });
        }

        LOGGER.info("success to save cluster tag={} by user={}", request, operator);
        return entity.getId();
    }

    @Override
    public ClusterTagResponse getTag(Integer id, String currentUser) {
        Preconditions.expectNotNull(id, "inlong cluster tag id cannot be empty");
        InlongClusterTagEntity entity = clusterTagMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("inlong cluster tag not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        ClusterTagResponse response = CommonBeanUtils.copyProperties(entity, ClusterTagResponse::new);
        unpackExtParams(response);

        List<String> tenantList = tenantClusterTagMapper
                .selectByTag(entity.getClusterTag()).stream()
                .map(TenantClusterTagEntity::getTenant)
                .collect(Collectors.toList());
        response.setTenantList(tenantList);

        LOGGER.debug("success to get cluster tag info by id={}", id);
        return response;
    }

    @Override
    public PageResult<ClusterTagResponse> listTag(ClusterTagPageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongClusterTagEntity> entityPage = (Page<InlongClusterTagEntity>) clusterTagMapper
                .selectByCondition(request);
        PageResult<ClusterTagResponse> pageResult = PageResult.fromPage(entityPage)
                .map(entity -> {
                    ClusterTagResponse response = CommonBeanUtils.copyProperties(entity, ClusterTagResponse::new);
                    unpackExtParams(response);

                    List<String> tenantList = tenantClusterTagMapper
                            .selectByTag(entity.getClusterTag()).stream()
                            .map(TenantClusterTagEntity::getTenant)
                            .collect(Collectors.toList());
                    response.setTenantList(tenantList);
                    return response;
                });

        LOGGER.debug("success to list cluster tag by {}", request);
        return pageResult;
    }

    @Override
    public List<ClusterTagResponse> listTag(ClusterTagPageRequest request, UserInfo opInfo) {
        List<InlongClusterTagEntity> filterResult = new ArrayList<>();
        List<InlongClusterTagEntity> clusterTagEntities = clusterTagMapper.selectByCondition(request);
        if (CollectionUtils.isNotEmpty(clusterTagEntities)) {
            for (InlongClusterTagEntity tagEntity : clusterTagEntities) {
                // only the person in charges can query
                if (!opInfo.getAccountType().equals(TenantUserTypeEnum.TENANT_ADMIN.getCode())) {
                    List<String> inCharges = Arrays.asList(tagEntity.getInCharges().split(InlongConstants.COMMA));
                    if (!inCharges.contains(opInfo.getName())) {
                        continue;
                    }
                }
                filterResult.add(tagEntity);
            }
        }
        return CommonBeanUtils.copyListProperties(filterResult, ClusterTagResponse::new);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public Boolean updateTag(ClusterTagRequest request, String operator) {
        LOGGER.debug("begin to update cluster tag={}", request);
        Preconditions.expectNotNull(request, "inlong cluster request cannot be empty");
        Integer id = request.getId();
        Preconditions.expectNotNull(id, "cluster tag id cannot be empty");
        InlongClusterTagEntity exist = clusterTagMapper.selectById(id);
        if (exist == null) {
            LOGGER.warn("inlong cluster tag was not exist for id={}", id);
            return true;
        }
        // append cluster tag if value is empty
        if (StringUtils.isEmpty(request.getClusterTag())) {
            request.setClusterTag(exist.getClusterTag());
        }
        String newClusterTag = request.getClusterTag();
        String errMsg = String.format("cluster tag has already updated with name=%s, curVersion=%s",
                exist.getClusterTag(), request.getVersion());
        if (!Objects.equals(exist.getVersion(), request.getVersion())) {
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        // if the cluster tag was changed, need to check whether the new tag already exists
        String oldClusterTag = exist.getClusterTag();
        if (!newClusterTag.equals(oldClusterTag)) {
            InlongClusterTagEntity tagConflict = clusterTagMapper.selectByTag(newClusterTag);
            if (tagConflict != null) {
                String tagErrMsg = String.format("inlong cluster tag [%s] already exist", newClusterTag);
                LOGGER.error(tagErrMsg);
                throw new BusinessException(tagErrMsg);
            }

            // check if there are some InlongGroups that uses this tag
            this.assertNoInlongGroupExists(oldClusterTag);

            // update the associated cluster tag in inlong_cluster
            List<InlongClusterEntity> clusterEntities = clusterMapper.selectByKey(oldClusterTag, null, null);
            if (CollectionUtils.isNotEmpty(clusterEntities)) {
                clusterEntities.forEach(entity -> {
                    Set<String> tagSet = Sets.newHashSet(entity.getClusterTags().split(InlongConstants.COMMA));
                    tagSet.remove(oldClusterTag);
                    tagSet.add(newClusterTag);
                    String updateTags = Joiner.on(",").join(tagSet);
                    entity.setClusterTags(updateTags);
                    entity.setModifier(operator);
                    if (InlongConstants.AFFECTED_ONE_ROW != clusterMapper.updateById(entity)) {
                        LOGGER.error("cluster has already updated with name={}, type={}, curVersion={}",
                                entity.getName(), entity.getType(), entity.getVersion());
                        throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
                    }
                });
            }
        }

        CommonBeanUtils.copyProperties(request, exist, true);
        request.setExtParams(exist.getExtParams());
        String extParams = packExtParams(request);
        exist.setExtParams(extParams);
        exist.setModifier(operator);
        if (InlongConstants.AFFECTED_ONE_ROW != clusterTagMapper.updateByIdSelective(exist)) {
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }

        // for tenant tag
        if (CollectionUtils.isNotEmpty(request.getTenantList())) {
            Set<String> updatedTenants = new HashSet<>(request.getTenantList());
            List<TenantClusterTagEntity> tenantList = tenantClusterTagMapper.selectByTag(oldClusterTag);
            // remove
            tenantList.stream()
                    .filter(entity -> !updatedTenants.contains(entity.getTenant()))
                    .forEach(entity -> {
                        try {
                            this.deleteTenantTag(entity.getId(), operator);
                        } catch (Exception e) {
                            LOGGER.error(e.getMessage());
                        }
                    });
            // add
            Set<String> currentTenants = tenantList.stream()
                    .map(TenantClusterTagEntity::getTenant)
                    .collect(Collectors.toSet());
            updatedTenants.stream()
                    .filter(tenant -> !currentTenants.contains(tenant))
                    .forEach(tenant -> {
                        try {
                            TenantClusterTagRequest tagRequest = new TenantClusterTagRequest();
                            tagRequest.setTenant(tenant);
                            tagRequest.setClusterTag(oldClusterTag);
                            this.saveTenantTag(tagRequest, operator);
                        } catch (Exception e) {
                            LOGGER.error(e.getMessage());
                        }
                    });
        }

        LOGGER.info("success to update cluster tag={}", request);
        return true;
    }

    @Override
    public Boolean deleteTag(Integer id, String operator) {
        Preconditions.expectNotNull(id, "cluster tag id cannot be empty");
        InlongClusterTagEntity exist = clusterTagMapper.selectById(id);
        if (exist == null || exist.getIsDeleted() > InlongConstants.UN_DELETED) {
            LOGGER.error("inlong cluster tag not found by id={}", id);
            return false;
        }

        // check if there are some InlongGroups that uses this tag
        String clusterTag = exist.getClusterTag();
        this.assertNoInlongGroupExists(clusterTag);

        // update the associated cluster tag in inlong_cluster
        List<InlongClusterEntity> clusterEntities = clusterMapper.selectByKey(clusterTag, null, null);
        if (CollectionUtils.isNotEmpty(clusterEntities)) {
            clusterEntities.forEach(entity -> {
                this.removeClusterTag(entity, clusterTag, operator);
            });
        }

        exist.setIsDeleted(exist.getId());
        exist.setModifier(operator);
        if (InlongConstants.AFFECTED_ONE_ROW != clusterTagMapper.updateByIdSelective(exist)) {
            LOGGER.error("cluster tag has already updated with name={}, curVersion={}",
                    exist.getClusterTag(), exist.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }

        List<TenantClusterTagEntity> tenantList = tenantClusterTagMapper.selectByTag(clusterTag);
        tenantList.forEach(entity -> {
            try {
                this.deleteTenantTag(entity.getId(), operator);
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }
        });

        LOGGER.info("success to delete cluster tag by id={}", id);
        return true;
    }

    @Override
    public Integer save(ClusterRequest request, String operator) {
        LOGGER.debug("begin to save inlong cluster={}", request);
        Preconditions.expectNotNull(request, "inlong cluster request cannot be empty");

        // check if the cluster already exist
        String clusterTag = request.getClusterTags();

        // The name cannot be modified and is automatically generated by the system
        String name = request.getName();
        if (StringUtils.isBlank(name)) {
            name = UUID.randomUUID().toString();
            request.setName(name);
        }
        String type = request.getType();
        List<InlongClusterEntity> exist = clusterMapper.selectByKey(clusterTag, name, type);
        if (CollectionUtils.isNotEmpty(exist)) {
            String errMsg = String.format("inlong cluster already exist for cluster tag=%s name=%s type=%s",
                    clusterTag, name, type);
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongClusterOperator instance = clusterOperatorFactory.getInstance(request.getType());
        Integer id = instance.saveOpt(request, operator);
        LOGGER.info("success to save inlong cluster={} by user={}", request, operator);
        return id;
    }

    @Override
    public ClusterInfo get(Integer id, String currentUser) {
        Preconditions.expectNotNull(id, "inlong cluster id cannot be empty");
        InlongClusterEntity entity = clusterMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("inlong cluster not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        InlongClusterOperator instance = clusterOperatorFactory.getInstance(entity.getType());
        ClusterInfo clusterInfo = instance.getFromEntity(entity);
        LOGGER.debug("success to get inlong cluster info by id={}", id);
        return clusterInfo;
    }

    @Override
    public PageResult<ClusterInfo> list(ClusterPageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongClusterEntity> entityPage = (Page<InlongClusterEntity>) clusterMapper.selectByCondition(request);
        PageResult<ClusterInfo> pageResult = PageResult.fromPage(entityPage)
                .map(entity -> {
                    InlongClusterOperator instance = clusterOperatorFactory.getInstance(entity.getType());
                    return instance.getFromEntity(entity);
                });

        LOGGER.debug("success to list inlong cluster by {}", request);
        return pageResult;
    }

    @Override
    public List<ClusterInfo> list(ClusterPageRequest request, UserInfo opInfo) {
        // get and filter records
        List<InlongClusterEntity> clusterEntities = clusterMapper.selectByCondition(request);
        List<InlongClusterEntity> filterResult = new ArrayList<>();
        for (InlongClusterEntity entity : clusterEntities) {
            // only the person in charges can query
            if (!opInfo.getAccountType().equals(TenantUserTypeEnum.TENANT_ADMIN.getCode())) {
                List<String> inCharges = Arrays.asList(entity.getInCharges().split(InlongConstants.COMMA));
                if (!inCharges.contains(opInfo.getName())) {
                    continue;
                }
            }
            filterResult.add(entity);
        }
        // transfer records
        return filterResult.stream().map(entity -> {
            InlongClusterOperator instance = clusterOperatorFactory.getInstance(entity.getType());
            return instance.getFromEntity(entity);
        }).collect(Collectors.toList());
    }

    @Override
    public List<ClusterInfo> listByTagAndType(String clusterTag, String clusterType) {
        List<InlongClusterEntity> clusterEntities = clusterMapper.selectByKey(clusterTag, null, clusterType);
        if (CollectionUtils.isEmpty(clusterEntities)) {
            throw new BusinessException(String.format("cannot find any cluster by tag %s and type %s",
                    clusterTag, clusterType));
        }

        List<ClusterInfo> clusterInfos = clusterEntities.stream()
                .map(entity -> {
                    InlongClusterOperator operator = clusterOperatorFactory.getInstance(entity.getType());
                    return operator.getFromEntity(entity);
                })
                .collect(Collectors.toList());

        LOGGER.debug("success to list inlong cluster by tag={}", clusterTag);
        return clusterInfos;
    }

    @Override
    public ClusterInfo getOne(String clusterTag, String name, String type) {
        List<InlongClusterEntity> entityList = clusterMapper.selectByKey(clusterTag, name, type);
        if (CollectionUtils.isEmpty(entityList)) {
            throw new BusinessException(String.format("cluster not found by tag=%s, name=%s, type=%s",
                    clusterTag, name, type));
        }

        InlongClusterEntity entity = entityList.get(0);
        InlongClusterOperator instance = clusterOperatorFactory.getInstance(entity.getType());
        ClusterInfo result = instance.getFromEntity(entity);
        LOGGER.debug("success to get inlong cluster by tag={}, name={}, type={}", clusterTag, name, type);
        return result;
    }

    @Override
    public Boolean update(ClusterRequest request, String operator) {
        LOGGER.debug("begin to update inlong cluster: {}", request);
        Preconditions.expectNotNull(request, "inlong cluster info cannot be empty");
        Integer id = request.getId();
        InlongClusterEntity entity = clusterMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("inlong cluster not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        chkUnmodifiableParams(entity, request);
        // check whether the cluster already exists
        String clusterTag = request.getClusterTags();
        List<InlongClusterEntity> exist = clusterMapper.selectByKey(
                clusterTag, request.getName(), request.getType());
        if (CollectionUtils.isNotEmpty(exist) && !Objects.equals(id, exist.get(0).getId())) {
            String errMsg = String.format("inlong cluster already exist for cluster tag=%s name=%s type=%s",
                    clusterTag, request.getName(), request.getType());
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongClusterOperator instance = clusterOperatorFactory.getInstance(request.getType());
        instance.updateOpt(request, operator);
        LOGGER.info("success to update inlong cluster: {} by {}", request, operator);
        return true;
    }

    @Override
    public UpdateResult updateByKey(ClusterRequest request, String operator) {
        LOGGER.debug("begin to update inlong cluster: {}", request);
        Preconditions.expectNotNull(request, "inlong cluster info cannot be null");
        String name = request.getName();
        String type = request.getType();
        InlongClusterEntity entity = clusterMapper.selectByNameAndType(name, type);
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND,
                    String.format("inlong cluster not found by name=%s, type=%s", name, type));
        }
        request.setId(entity.getId());
        // check unmodifiable parameters
        chkUnmodifiableParams(entity, request);
        // update record
        InlongClusterOperator instance = clusterOperatorFactory.getInstance(request.getType());
        instance.updateOpt(request, operator);
        LOGGER.info("success to update inlong cluster: {} by {}", request, operator);
        return new UpdateResult(entity.getId(), true, request.getVersion() + 1);
    }

    @Override
    public Boolean bindTag(BindTagRequest request, String operator) {
        LOGGER.info("begin to bind or unbind cluster tag: {}", request);
        Preconditions.expectNotNull(request, "inlong cluster info cannot be empty");
        String clusterTag = request.getClusterTag();
        Preconditions.expectNotBlank(clusterTag, ErrorCodeEnum.INVALID_PARAMETER, "cluster tag cannot be empty");
        InlongClusterTagEntity exist = clusterTagMapper.selectByTag(clusterTag);
        if (CollectionUtils.isNotEmpty(request.getBindClusters())) {
            request.getBindClusters().forEach(id -> {
                InlongClusterEntity entity = clusterMapper.selectById(id);
                Set<String> tagSet = Sets.newHashSet(entity.getClusterTags().split(InlongConstants.COMMA));
                tagSet.add(clusterTag);
                String updateTags = Joiner.on(",").join(tagSet);
                InlongClusterEntity updateEntity = clusterMapper.selectById(id);
                updateEntity.setClusterTags(updateTags);
                updateEntity.setModifier(operator);
                if (InlongConstants.AFFECTED_ONE_ROW != clusterMapper.updateById(updateEntity)) {
                    LOGGER.error("cluster has already updated with name={}, type={}, curVersion={}",
                            updateEntity.getName(), updateEntity.getType(), updateEntity.getVersion());
                    throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
                }
            });
        }

        if (CollectionUtils.isNotEmpty(request.getUnbindClusters())) {
            request.getUnbindClusters().forEach(id -> {
                InlongClusterEntity entity = clusterMapper.selectById(id);
                this.removeClusterTag(entity, clusterTag, operator);
            });
        }
        LOGGER.info("success to bind or unbind cluster tag {} by {}", request, operator);
        return true;
    }

    @Override
    public Boolean deleteByKey(String name, String type, String operator) {
        Preconditions.expectNotBlank(name, ErrorCodeEnum.INVALID_PARAMETER, "cluster name should not be empty or null");
        Preconditions.expectNotBlank(type, ErrorCodeEnum.INVALID_PARAMETER, "cluster type should not be empty or null");
        InlongClusterEntity entity = clusterMapper.selectByNameAndType(name, type);
        if (entity == null || entity.getIsDeleted() > InlongConstants.UN_DELETED) {
            LOGGER.error("inlong cluster not found by clusterName={}, type={} or was already deleted",
                    name, type);
            return false;
        }

        List<InlongClusterNodeEntity> nodeEntities = clusterNodeMapper.selectByParentId(entity.getId(), null);
        if (CollectionUtils.isNotEmpty(nodeEntities)) {
            String errMsg = String.format("there are undeleted nodes under the cluster [%s], "
                    + "please delete the node first", entity.getName());
            throw new BusinessException(errMsg);
        }

        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        if (InlongConstants.AFFECTED_ONE_ROW != clusterMapper.updateById(entity)) {
            LOGGER.error("cluster has already updated with name={}, type={}, curVersion={}", entity.getName(),
                    entity.getType(), entity.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        LOGGER.info("success to delete inlong cluster for clusterName={}, type={} by user={}",
                name, type, operator);
        return true;
    }

    @Override
    public Boolean delete(Integer id, String operator) {
        Preconditions.expectNotNull(id, "cluster id cannot be empty");
        InlongClusterEntity entity = clusterMapper.selectById(id);
        Preconditions.expectNotNull(entity, ErrorCodeEnum.CLUSTER_NOT_FOUND,
                ErrorCodeEnum.CLUSTER_NOT_FOUND.getMessage());

        List<InlongClusterNodeEntity> nodeEntities = clusterNodeMapper.selectByParentId(id, null);
        if (CollectionUtils.isNotEmpty(nodeEntities)) {
            String errMsg = String.format("there are undeleted nodes under the cluster [%s], "
                    + "please delete the node first", entity.getName());
            throw new BusinessException(errMsg);
        }

        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        if (InlongConstants.AFFECTED_ONE_ROW != clusterMapper.updateById(entity)) {
            LOGGER.error("cluster has already updated with name={}, type={}, curVersion={}", entity.getName(),
                    entity.getType(), entity.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        LOGGER.info("success to delete inlong cluster for id={} by user={}", id, operator);
        return true;
    }

    @Override
    public Boolean delete(Integer id, UserInfo opInfo) {
        InlongClusterEntity entity = clusterMapper.selectById(id);
        Preconditions.expectNotNull(entity, ErrorCodeEnum.CLUSTER_NOT_FOUND,
                ErrorCodeEnum.CONSUME_NOT_FOUND.getMessage());

        List<InlongClusterNodeEntity> nodeEntities = clusterNodeMapper.selectByParentId(id, null);
        if (CollectionUtils.isNotEmpty(nodeEntities)) {
            throw new BusinessException(ErrorCodeEnum.RECORD_IN_USED,
                    String.format("there are undeleted nodes under the cluster [%s], "
                            + "please delete the node first", entity.getName()));
        }
        entity.setIsDeleted(entity.getId());
        entity.setModifier(opInfo.getName());
        if (InlongConstants.AFFECTED_ONE_ROW != clusterMapper.updateById(entity)) {
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED,
                    String.format("cluster has already updated with name=%s, type=%s, curVersion=%s",
                            entity.getName(), entity.getType(), entity.getVersion()));
        }
        return true;
    }

    @Override
    public Integer saveNode(ClusterNodeRequest request, String operator) {
        LOGGER.debug("begin to insert inlong cluster node={}", request);
        Preconditions.expectNotNull(request, "cluster node info cannot be empty");

        // check cluster node if exist
        InlongClusterNodeEntity exist = clusterNodeMapper.selectByUniqueKey(request);
        if (exist != null) {
            String errMsg = String.format("inlong cluster node already exist for type=%s ip=%s port=%s",
                    request.getType(), request.getIp(), request.getPort());
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        // check ip if belongs to two different clusters at the same time
        List<InlongClusterNodeEntity> existList =
                clusterNodeMapper.selectByIpAndType(request.getIp(), request.getType());
        if (CollectionUtils.isNotEmpty(existList)) {
            InlongClusterEntity currentCluster = clusterMapper.selectById(existList.get(0).getParentId());
            InlongClusterEntity targetCluster = clusterMapper.selectById(request.getParentId());
            if (!Objects.equals(currentCluster.getId(), targetCluster.getId())) {
                throw new BusinessException(
                        String.format("current ip can not belong to cluster %s and %s at the same time",
                                currentCluster.getName(), targetCluster.getName()));
            }
        }
        InlongClusterNodeOperator instance = clusterNodeOperatorFactory.getInstance(request.getType());
        Integer id = instance.saveOpt(request, operator);
        if (request.getIsInstall()) {
            request.setId(id);
            clusterNodeMapper.updateOperateLogById(id, NodeStatus.INSTALLING.getStatus(), "begin to install");
            pendingInstallRequests.add(request);
        }
        return id;
    }

    @Override
    public ClusterNodeResponse getNode(Integer id, String currentUser) {
        Preconditions.expectNotNull(id, "cluster node id cannot be empty");
        InlongClusterNodeEntity entity = clusterNodeMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("inlong cluster node not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        InlongClusterNodeOperator instance = clusterNodeOperatorFactory.getInstance(entity.getType());
        return instance.getFromEntity(entity);
    }

    @Override
    public PageResult<ClusterNodeResponse> listNode(ClusterPageRequest request, String currentUser) {
        if (StringUtils.isNotBlank(request.getClusterTag())) {
            List<ClusterNodeResponse> nodeList = listNodeByClusterTag(request);

            return new PageResult<>(nodeList, (long) nodeList.size());
        }
        Integer parentId = request.getParentId();
        Preconditions.expectNotNull(parentId, "Cluster id cannot be empty");
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongClusterNodeEntity> entityPage =
                (Page<InlongClusterNodeEntity>) clusterNodeMapper.selectByCondition(request);
        PageResult<ClusterNodeResponse> pageResult = PageResult.fromPage(entityPage)
                .map(entity -> {
                    InlongClusterNodeOperator instance = clusterNodeOperatorFactory.getInstance(entity.getType());
                    return instance.getFromEntity(entity);
                });

        LOGGER.debug("success to list inlong cluster node by {}", request);
        return pageResult;
    }

    @Override
    public List<ClusterNodeResponse> listNode(ClusterPageRequest request, UserInfo opInfo) {
        if (StringUtils.isBlank(request.getClusterTag())) {
            if (request.getParentId() == null) {
                throw new BusinessException(ErrorCodeEnum.ID_IS_EMPTY,
                        "Cluster id cannot be empty");
            }
            return CommonBeanUtils.copyListProperties(
                    clusterNodeMapper.selectByCondition(request), ClusterNodeResponse::new);
        } else {
            List<InlongClusterNodeEntity> allNodeList = new ArrayList<>();
            List<InlongClusterEntity> clusterList =
                    clusterMapper.selectByKey(request.getClusterTag(), request.getName(), request.getType());
            for (InlongClusterEntity cluster : clusterList) {
                // only the person in charges can query
                if (!opInfo.getAccountType().equals(TenantUserTypeEnum.TENANT_ADMIN.getCode())) {
                    List<String> inCharges = Arrays.asList(cluster.getInCharges().split(InlongConstants.COMMA));
                    if (!inCharges.contains(opInfo.getName())) {
                        continue;
                    }
                }
                allNodeList.addAll(clusterNodeMapper.selectByParentId(cluster.getId(), null));
            }
            return CommonBeanUtils.copyListProperties(allNodeList, ClusterNodeResponse::new);
        }
    }

    @Override
    public List<ClusterNodeResponse> listNodeByGroupId(String groupId, String clusterType, String protocolType) {
        LOGGER.debug("begin to get cluster nodes for groupId={}, clusterType={}, protocol={}",
                groupId, clusterType, protocolType);

        List<InlongClusterNodeEntity> nodeEntities = getClusterNodes(groupId, clusterType, protocolType);
        if (CollectionUtils.isEmpty(nodeEntities)) {
            LOGGER.debug("not any cluster node for groupId={}, clusterType={}, protocol={}",
                    groupId, clusterType, protocolType);
            return Collections.emptyList();
        }

        List<ClusterNodeResponse> result = CommonBeanUtils.copyListProperties(nodeEntities, ClusterNodeResponse::new);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("success to get nodes for groupId={}, clusterType={}, protocol={}, result size={}",
                    groupId, clusterType, protocolType, result);
        }
        return result;
    }

    public List<ClusterNodeResponse> listNodeByClusterTag(ClusterPageRequest request) {
        List<InlongClusterEntity> clusterList = clusterMapper.selectByKey(request.getClusterTag(), request.getName(),
                request.getType());
        List<InlongClusterNodeEntity> allNodeList = new ArrayList<>();
        for (InlongClusterEntity cluster : clusterList) {
            List<InlongClusterNodeEntity> nodeList = clusterNodeMapper.selectByParentId(cluster.getId(), null);
            allNodeList.addAll(nodeList);
        }
        return CommonBeanUtils.copyListProperties(allNodeList, ClusterNodeResponse::new);
    }

    @Override
    public List<String> listNodeIpByType(String type) {
        Preconditions.expectNotBlank(type, ErrorCodeEnum.INVALID_PARAMETER, "cluster type cannot be empty");
        ClusterPageRequest request = new ClusterPageRequest();
        request.setType(type);
        List<InlongClusterNodeEntity> nodeList = clusterNodeMapper.selectByCondition(request);
        if (CollectionUtils.isEmpty(nodeList)) {
            LOGGER.debug("not found any node for type={}", type);
            return Collections.emptyList();
        }

        List<String> ipList = nodeList.stream()
                .map(node -> String.format("%s:%d", node.getIp(), node.getPort()))
                .collect(Collectors.toList());
        LOGGER.debug("success to list node by type={}, result={}", type, ipList);
        return ipList;
    }

    @Override
    public Boolean updateNode(ClusterNodeRequest request, String operator) {
        LOGGER.debug("begin to update inlong cluster node={}", request);
        Preconditions.expectNotNull(request, "inlong cluster node cannot be empty");
        Integer id = request.getId();
        InlongClusterNodeEntity entity = clusterNodeMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("cluster node not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        // check type
        Preconditions.expectEquals(entity.getType(), request.getType(),
                ErrorCodeEnum.INVALID_PARAMETER, "type not allowed modify");
        // check record version
        Preconditions.expectEquals(entity.getVersion(), request.getVersion(),
                ErrorCodeEnum.CONFIG_EXPIRED,
                String.format("record has expired with record version=%d, request version=%d",
                        entity.getVersion(), request.getVersion()));
        // check protocol type
        if (StringUtils.isBlank(request.getProtocolType())) {
            request.setProtocolType(entity.getProtocolType());
        }
        // check cluster node if exist
        InlongClusterNodeEntity exist = clusterNodeMapper.selectByUniqueKey(request);
        if (exist != null && !Objects.equals(id, exist.getId())) {
            String errMsg = "inlong cluster node already exist for " + request;
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }
        // update record
        InlongClusterNodeOperator instance = clusterNodeOperatorFactory.getInstance(request.getType());
        instance.updateOpt(request, operator);
        if (request.getIsInstall()) {
            // when reinstall set install to false
            request.setIsInstall(false);
            clusterNodeMapper.updateOperateLogById(request.getId(), NodeStatus.INSTALLING.getStatus(),
                    "begin to re install");
            pendingInstallRequests.add(request);
        }
        return true;
    }

    @Override
    public Boolean deleteNode(Integer id, String operator) {
        Preconditions.expectNotNull(id, "cluster node id cannot be empty");
        InlongClusterNodeEntity entity = clusterNodeMapper.selectById(id);
        Preconditions.expectNotNull(entity, ErrorCodeEnum.CLUSTER_NOT_FOUND);

        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        if (InlongConstants.AFFECTED_ONE_ROW != clusterNodeMapper.updateById(entity)) {
            LOGGER.error("cluster node has already updated with parentId={}, type={}, ip={}, port={}, protocolType={}",
                    entity.getParentId(), entity.getType(), entity.getIp(), entity.getPort(), entity.getProtocolType());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        LOGGER.info("success to delete inlong cluster node by id={}", id);
        return true;
    }

    @Override
    public Boolean unloadNode(Integer id, String operator) {
        LOGGER.info("begin to unload inlong cluster node={}, operator={}", id, operator);
        InlongClusterNodeEntity clusterNodeEntity = clusterNodeMapper.selectById(id);
        InlongClusterNodeInstallOperator clusterNodeInstallOperator = clusterNodeInstallOperatorFactory.getInstance(
                clusterNodeEntity.getType());
        boolean isSuccess = clusterNodeInstallOperator.unload(clusterNodeEntity, operator);
        LOGGER.info("success to unload inlong cluster node={}, operator={}", id, operator);
        return isSuccess;
    }

    @Override
    public String getManagerSSHPublicKey() {
        String homeDirectory = System.getProperty("user.home");
        String publicKeyPath = homeDirectory + "/.ssh/inlong_rsa.pub";
        try {
            Path path = Paths.get(publicKeyPath);
            if (!Files.exists(path)) {
                commandExecutor.execSSHKeyGeneration();
            }
            return StringUtils.strip(new String(Files.readAllBytes(path)), "\n");
        } catch (Exception e) {
            LOGGER.error("get manager ssh public key error", e);
            throw new RuntimeException("get manager ssh public key error", e);
        }
    }

    @Override
    public Boolean testSSHConnection(ClusterNodeRequest request) {
        AgentClusterNodeRequest nodeRequest = (AgentClusterNodeRequest) request;
        try {
            CommandResult commandResult = commandExecutor.execRemote(nodeRequest, "ls");
            return commandResult.getCode() == 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataProxyNodeResponse getDataProxyNodes(String groupId, String protocolType) {
        LOGGER.debug("begin to get data proxy nodes for groupId={}, protocol={}", groupId, protocolType);

        InlongGroupEntity groupEntity = groupMapper.selectByGroupId(groupId);
        if (groupEntity == null) {
            String errMsg = String.format("group not found by groupId=%s", groupId);
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }
        GroupStatus groupStatus = GroupStatus.forCode(groupEntity.getStatus());
        if (!Objects.equals(groupStatus, GroupStatus.CONFIG_SUCCESSFUL)) {
            String errMsg =
                    String.format("current group status=%s was not allowed to get data proxy nodes", groupStatus);
            LOGGER.warn(errMsg);
            throw new BusinessException(errMsg);
        }
        List<InlongClusterNodeEntity> nodeEntities = getClusterNodes(groupId, ClusterType.DATAPROXY, protocolType);
        DataProxyNodeResponse response = new DataProxyNodeResponse();
        if (CollectionUtils.isEmpty(nodeEntities)) {
            LOGGER.debug("not any data proxy node for groupId={}, protocol={}", groupId, protocolType);
            return response;
        }

        // all cluster nodes belong to the same clusterId
        Integer clusterId = nodeEntities.get(0).getParentId();
        Integer maxPacketLength = null;
        response.setClusterId(clusterId);
        InlongClusterEntity dataProxyCluster = clusterMapper.selectById(clusterId);
        if (dataProxyCluster != null && StringUtils.isNotBlank(dataProxyCluster.getExtParams())) {
            DataProxyClusterDTO dataProxyClusterDTO = DataProxyClusterDTO.getFromJson(dataProxyCluster.getExtParams());
            maxPacketLength = dataProxyClusterDTO.getMaxPacketLength();
            response.setMaxPacketLength(maxPacketLength);
        }

        // TODO consider the data proxy load and re-balance
        List<DataProxyNodeInfo> nodeList = new ArrayList<>();
        for (InlongClusterNodeEntity nodeEntity : nodeEntities) {
            if (StringUtils.isNotBlank(nodeEntity.getExtParams())) {
                DataProxyClusterNodeDTO dataProxyClusterNodeDTO = DataProxyClusterNodeDTO.getFromJson(
                        nodeEntity.getExtParams());
                if (Objects.equals(dataProxyClusterNodeDTO.getEnabledOnline(), false)) {
                    continue;
                }
            }
            DataProxyNodeInfo nodeInfo = new DataProxyNodeInfo();
            nodeInfo.setId(nodeEntity.getId());
            nodeInfo.setIp(nodeEntity.getIp());
            nodeInfo.setPort(nodeEntity.getPort());
            nodeInfo.setProtocolType(nodeEntity.getProtocolType());
            nodeInfo.setNodeLoad(nodeEntity.getNodeLoad());
            nodeList.add(nodeInfo);
        }
        response.setNodeList(nodeList);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("success to get dp nodes for groupId={}, protocol={}, result={}",
                    groupId, protocolType, response);
        }
        return response;
    }

    @Override
    public DataProxyNodeResponse getDataProxyNodesByCluster(String clusterName, String protocolType,
            String reportSourceType) {
        LOGGER.debug("begin to get data proxy nodes for clusterName={}, protocol={}", clusterName, protocolType);
        InlongClusterEntity clusterEntity = clusterMapper.selectByNameAndType(clusterName, ClusterType.DATAPROXY);
        DataProxyNodeResponse response = new DataProxyNodeResponse();
        if (clusterEntity == null) {
            LOGGER.debug("not any dataproxy cluster for clusterName={}, protocol={}", clusterName, protocolType);
            return response;
        }
        Integer maxPacketLength = null;
        if (StringUtils.isNotBlank(clusterEntity.getExtParams())) {
            DataProxyClusterDTO dataProxyClusterDTO = DataProxyClusterDTO.getFromJson(clusterEntity.getExtParams());
            maxPacketLength = dataProxyClusterDTO.getMaxPacketLength();
            response.setMaxPacketLength(maxPacketLength);
        }
        List<InlongClusterNodeEntity> nodeEntities =
                clusterNodeMapper.selectByParentId(clusterEntity.getId(), protocolType);
        if (CollectionUtils.isEmpty(nodeEntities)) {
            LOGGER.debug("not any data proxy node for clusterName={}, protocol={}", clusterName, protocolType);
            return response;
        }
        // all cluster nodes belong to the same clusterId
        response.setClusterId(clusterEntity.getId());
        // TODO consider the data proxy load and re-balance
        List<DataProxyNodeInfo> nodeList = new ArrayList<>();
        for (InlongClusterNodeEntity nodeEntity : nodeEntities) {
            if (Objects.equals(nodeEntity.getStatus(), NodeStatus.HEARTBEAT_TIMEOUT.getStatus())) {
                LOGGER.debug("dataproxy node was timeout, parentId={} ip={} port={}", nodeEntity.getParentId(),
                        nodeEntity.getIp(), nodeEntity.getPort());
                continue;
            }
            if (StringUtils.isNotBlank(nodeEntity.getExtParams())) {
                DataProxyClusterNodeDTO dataProxyClusterNodeDTO = DataProxyClusterNodeDTO.getFromJson(
                        nodeEntity.getExtParams());
                if (StringUtils.isBlank(dataProxyClusterNodeDTO.getReportSourceType())) {
                    dataProxyClusterNodeDTO.setReportSourceType(ReportResourceType.INLONG);
                }
                if (StringUtils.isNotBlank(reportSourceType) && !Objects.equals(
                        dataProxyClusterNodeDTO.getReportSourceType(), reportSourceType)) {
                    continue;
                }
                if (Objects.equals(dataProxyClusterNodeDTO.getEnabledOnline(), false)) {
                    continue;
                }
            }
            DataProxyNodeInfo nodeInfo = new DataProxyNodeInfo();
            nodeInfo.setId(nodeEntity.getId());
            nodeInfo.setIp(nodeEntity.getIp());
            nodeInfo.setPort(nodeEntity.getPort());
            nodeInfo.setProtocolType(nodeEntity.getProtocolType());
            nodeInfo.setNodeLoad(nodeEntity.getNodeLoad());
            nodeList.add(nodeInfo);
        }
        response.setNodeList(nodeList);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("success to get dp nodes for clusterName={}, protocol={}, result={}",
                    clusterName, protocolType, response);
        }
        return response;
    }

    private List<InlongClusterNodeEntity> getClusterNodes(String groupId, String clusterType, String protocolType) {
        InlongGroupEntity groupEntity = groupMapper.selectByGroupId(groupId);
        if (groupEntity == null) {
            LOGGER.warn("inlong group not exists for groupId={}", groupId);
            return Lists.newArrayList();
        }

        String clusterTag = groupEntity.getInlongClusterTag();
        if (StringUtils.isBlank(clusterTag)) {
            String msg = "not found any cluster tag for groupId=" + groupId;
            LOGGER.debug(msg);
            throw new BusinessException(msg);
        }

        List<InlongClusterEntity> clusterList = clusterMapper.selectByKey(clusterTag, null, clusterType);
        if (CollectionUtils.isEmpty(clusterList)) {
            String msg = "not found any data proxy cluster for groupId=" + groupId
                    + " and clusterTag=" + clusterTag;
            LOGGER.debug(msg);
            throw new BusinessException(msg);
        }

        // TODO if more than one data proxy cluster, currently takes first
        return clusterNodeMapper.selectByParentId(clusterList.get(0).getId(), protocolType);
    }

    @Override
    public DataProxyConfig getDataProxyConfig(String clusterTag, String clusterName) {
        LOGGER.debug("GetDPConfig: begin to get config by cluster tag={} name={}", clusterTag, clusterName);

        // get all data proxy clusters
        ClusterPageRequest request = ClusterPageRequest.builder()
                .clusterTag(clusterTag)
                .name(clusterName)
                .type(ClusterType.DATAPROXY)
                .build();
        List<InlongClusterEntity> clusterEntityList = clusterMapper.selectByCondition(request);
        DataProxyConfig result = new DataProxyConfig();
        if (CollectionUtils.isEmpty(clusterEntityList)) {
            LOGGER.warn("GetDPConfig: not found data proxy cluster by tag={} name={}", clusterTag, clusterName);
            return result;
        }

        // get all inlong groups which was successful and belongs to this data proxy cluster
        Set<String> tagSet = new HashSet<>(16);
        clusterEntityList.forEach(e -> tagSet.addAll(Arrays.asList(e.getClusterTags().split(InlongConstants.COMMA))));
        List<String> clusterTagList = new ArrayList<>(tagSet);
        InlongGroupPageRequest groupRequest = InlongGroupPageRequest.builder()
                .statusList(Collections.singletonList(GroupStatus.CONFIG_SUCCESSFUL.getCode()))
                .clusterTagList(clusterTagList)
                .build();

        List<InlongGroupBriefInfo> groupList = groupMapper.selectBriefList(groupRequest);
        if (CollectionUtils.isEmpty(groupList)) {
            LOGGER.warn("GetDPConfig: not found inlong group with success status by cluster tags={}", clusterTagList);
            return result;
        }

        LOGGER.debug("GetDPConfig: begin to get config for cluster tags={}, associated InlongGroup num={}",
                clusterTagList, groupList.size());
        List<DataProxyTopicInfo> topicList = new ArrayList<>();
        for (InlongGroupBriefInfo groupInfo : groupList) {
            String groupId = groupInfo.getInlongGroupId();
            String mqResource = groupInfo.getMqResource();
            String realClusterTag = groupInfo.getInlongClusterTag();

            String mqType = groupInfo.getMqType();
            if (MQType.PULSAR.equals(mqType) || MQType.TDMQ_PULSAR.equals(mqType)) {
                InlongPulsarDTO pulsarDTO = InlongPulsarDTO.getFromJson(groupInfo.getExtParams());
                // First get the tenant from the InlongGroup, and then get it from the PulsarCluster.
                String tenant = pulsarDTO.getPulsarTenant();
                if (StringUtils.isBlank(tenant)) {
                    // If there are multiple Pulsar clusters, take the first one.
                    // Note that the tenants in multiple Pulsar clusters must be identical.
                    List<InlongClusterEntity> pulsarClusters = clusterMapper.selectByKey(realClusterTag, null,
                            ClusterType.PULSAR);
                    if (CollectionUtils.isEmpty(pulsarClusters)) {
                        LOGGER.error("GetDPConfig: not found pulsar cluster by cluster tag={}", realClusterTag);
                        continue;
                    }
                    PulsarClusterDTO cluster = PulsarClusterDTO.getFromJson(pulsarClusters.get(0).getExtParams());
                    tenant = cluster.getPulsarTenant();
                }

                List<InlongStreamBriefInfo> streamList = streamMapper.selectBriefList(groupId);
                for (InlongStreamBriefInfo streamInfo : streamList) {
                    String streamId = streamInfo.getInlongStreamId();
                    String topic = String.format(InlongConstants.PULSAR_TOPIC_FORMAT,
                            tenant, mqResource, streamInfo.getMqResource());
                    DataProxyTopicInfo topicConfig = new DataProxyTopicInfo();
                    // must format to groupId/streamId, needed by DataProxy
                    topicConfig.setInlongGroupId(groupId + "/" + streamId);
                    topicConfig.setTopic(topic);
                    topicList.add(topicConfig);
                }
            } else if (MQType.TUBEMQ.equals(mqType)) {
                DataProxyTopicInfo topicConfig = new DataProxyTopicInfo();
                topicConfig.setInlongGroupId(groupId);
                topicConfig.setTopic(mqResource);
                topicList.add(topicConfig);
            } else if (MQType.KAFKA.equals(mqType)) {
                List<InlongStreamBriefInfo> streamList = streamMapper.selectBriefList(groupId);
                for (InlongStreamBriefInfo streamInfo : streamList) {
                    String streamId = streamInfo.getInlongStreamId();
                    String topic = streamInfo.getMqResource();
                    if (topic.equals(streamId)) {
                        // the default mq resource (stream id) is not sufficient to discriminate different kafka topics
                        topic = String.format(Constants.DEFAULT_KAFKA_TOPIC_FORMAT,
                                mqResource, streamInfo.getMqResource());
                    }
                    DataProxyTopicInfo topicConfig = new DataProxyTopicInfo();
                    topicConfig.setInlongGroupId(groupId + "/" + streamId);
                    topicConfig.setTopic(topic);
                    topicList.add(topicConfig);
                }
            }
        }

        // get mq cluster info
        LOGGER.debug("GetDPConfig: begin to get mq clusters by tags={}", clusterTagList);
        List<MQClusterInfo> mqSet = new ArrayList<>();
        List<String> typeList = Arrays.asList(ClusterType.TUBEMQ, ClusterType.PULSAR, ClusterType.KAFKA);
        ClusterPageRequest pageRequest = ClusterPageRequest.builder()
                .typeList(typeList)
                .clusterTagList(clusterTagList)
                .build();
        List<InlongClusterEntity> mqClusterList = clusterMapper.selectByCondition(pageRequest);
        for (InlongClusterEntity cluster : mqClusterList) {
            MQClusterInfo clusterInfo = new MQClusterInfo();
            clusterInfo.setUrl(cluster.getUrl());
            clusterInfo.setToken(cluster.getToken());
            clusterInfo.setMqType(cluster.getType());
            Map<String, String> configParams = GSON.fromJson(cluster.getExtParams(), Map.class);
            clusterInfo.setParams(configParams);
            mqSet.add(clusterInfo);
        }

        result.setMqClusterList(mqSet);
        result.setTopicList(topicList);

        return result;
    }

    @Override
    @Deprecated
    public String getAllConfig(String clusterName, String md5) {
        DataProxyConfigResponse response = new DataProxyConfigResponse();
        String configMd5 = proxyRepository.getProxyMd5(clusterName);
        if (configMd5 == null) {
            response.setResult(false);
            response.setErrCode(DataProxyConfigResponse.REQ_PARAMS_ERROR);
            return GSON.toJson(response);
        }

        // same config
        if (configMd5.equals(md5)) {
            response.setResult(true);
            response.setErrCode(DataProxyConfigResponse.NOUPDATE);
            response.setMd5(configMd5);
            response.setData(new DataProxyCluster());
            return GSON.toJson(response);
        }

        String configJson = proxyRepository.getProxyConfigJson(clusterName);
        if (configJson == null) {
            response.setResult(false);
            response.setErrCode(DataProxyConfigResponse.REQ_PARAMS_ERROR);
            return GSON.toJson(response);
        }

        return configJson;
    }

    @Override
    public AuditConfig getAuditConfig(String clusterTag) {
        AuditConfig auditConfig = new AuditConfig();
        ClusterPageRequest request = ClusterPageRequest.builder()
                .clusterTag(clusterTag)
                .typeList(Arrays.asList(ClusterType.TUBEMQ, ClusterType.PULSAR, ClusterType.KAFKA))
                .build();
        List<InlongClusterEntity> clusterEntityList = clusterMapper.selectByCondition(request);
        List<MQInfo> mqInfoList = new ArrayList<>();
        for (InlongClusterEntity entity : clusterEntityList) {
            MQInfo info = new MQInfo();
            info.setUrl(entity.getUrl());
            info.setMqType(entity.getType());
            info.setParams(GSON.fromJson(entity.getExtParams(), Map.class));
            mqInfoList.add(info);
        }
        auditConfig.setMqInfoList(mqInfoList);
        return auditConfig;
    }

    @Override
    public Boolean testConnection(ClusterRequest request) {
        LOGGER.info("begin test connection for: {}", request);

        // according to the data node type, test connection
        InlongClusterOperator clusterOperator = clusterOperatorFactory.getInstance(request.getType());
        Boolean result = clusterOperator.testConnection(request);
        LOGGER.info("connection [{}] for: {}", result ? "success" : "failed", request);
        return result;
    }

    @Override
    public Integer saveTenantTag(TenantClusterTagRequest request, String operator) {
        LOGGER.debug("begin to save tenant cluster tag {}", request);
        Preconditions.expectNotNull(request, "tenant cluster request cannot be empty");
        Preconditions.expectNotBlank(request.getClusterTag(), ErrorCodeEnum.INVALID_PARAMETER,
                "cluster tag cannot be empty");
        Preconditions.expectNotBlank(request.getTenant(), ErrorCodeEnum.INVALID_PARAMETER,
                "tenant cannot be empty");
        InlongTenantInfo tenantInfo = tenantService.getByName(request.getTenant());
        Preconditions.expectNotNull(tenantInfo, ErrorCodeEnum.INVALID_PARAMETER,
                "target tenant cannot be found");

        TenantClusterTagEntity entity = CommonBeanUtils.copyProperties(request, TenantClusterTagEntity::new);
        entity.setCreator(operator);
        entity.setModifier(operator);
        tenantClusterTagMapper.insert(entity);
        LOGGER.info("success to save tenant tag, tenant={}, tag={}", request.getTenant(), request.getClusterTag());
        return entity.getId();
    }

    @Override
    public PageResult<ClusterTagResponse> listTagByTenantRole(TenantClusterTagPageRequest request) {

        ClusterTagPageRequest tagRequest = CommonBeanUtils.copyProperties(request, ClusterTagPageRequest::new);

        String loginUser = LoginUserUtils.getLoginUser().getName();
        InlongRoleInfo roleInfo = inlongRoleService.getByUsername(loginUser);

        // for non-inlong role, or not blank tenant condition, need to get target tag list first
        if (roleInfo == null || StringUtils.isNotBlank(request.getTenant())) {
            request.setPageNum(1);
            request.setPageSize(Integer.MAX_VALUE);
            List<String> tags = this.listTenantTag(request).getList()
                    .stream()
                    .map(TenantClusterTagInfo::getClusterTag)
                    .distinct()
                    .collect(Collectors.toList());

            // if no tags under this tenant, return empty directly
            if (CollectionUtils.isEmpty(tags) && StringUtils.isNotBlank(request.getTenant())) {
                return new PageResult<>(new ArrayList<>(), 0L, request.getPageNum(), request.getPageSize());
            }

            tagRequest.setClusterTags(tags);
        }

        return listTag(tagRequest);
    }

    @Override
    public PageResult<ClusterInfo> listByTenantRole(ClusterPageRequest request) {
        String loginUser = LoginUserUtils.getLoginUser().getName();
        InlongRoleInfo roleInfo = inlongRoleService.getByUsername(loginUser);
        if (roleInfo == null) {
            TenantClusterTagPageRequest tagRequest = new TenantClusterTagPageRequest();
            tagRequest.setPageNum(1);
            tagRequest.setPageSize(Integer.MAX_VALUE);
            List<String> tags = this.listTenantTag(tagRequest).getList()
                    .stream()
                    .map(TenantClusterTagInfo::getClusterTag)
                    .distinct()
                    .collect(Collectors.toList());
            request.setClusterTagList(tags);
        }
        return this.list(request);
    }

    @Override
    public PageResult<TenantClusterTagInfo> listTenantTag(TenantClusterTagPageRequest request) {
        LOGGER.debug("begin to list tag by tenant {}", request);

        String loginUser = LoginUserUtils.getLoginUser().getName();
        InlongRoleInfo roleInfo = inlongRoleService.getByUsername(loginUser);
        if (roleInfo == null) {
            List<String> tenants = tenantRoleService.listTenantByUsername(loginUser);
            request.setTenantList(tenants);
        }

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<TenantClusterTagEntity> entityPage =
                (Page<TenantClusterTagEntity>) tenantClusterTagMapper.selectByCondition(request);
        PageResult<TenantClusterTagInfo> pageResult = PageResult.fromPage(entityPage)
                .map(entity -> CommonBeanUtils.copyProperties(entity, TenantClusterTagInfo::new));
        LOGGER.debug("success to list tenant tag with request={}", request);
        return pageResult;
    }

    @Override
    public Boolean deleteTenantTag(Integer id, String operator) {
        LOGGER.debug("start to delete tenant tag with id={}", id);
        TenantClusterTagEntity entity = tenantClusterTagMapper.selectByPrimaryKey(id);
        Preconditions.expectNotNull(entity, ErrorCodeEnum.RECORD_NOT_FOUND.getMessage());

        entity.setModifier(operator);
        entity.setIsDeleted(id);

        int rowCount = tenantClusterTagMapper.updateByIdSelective(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error("tenant cluster tag has already updated for tenant={} tag={}",
                    entity.getTenant(), entity.getClusterTag());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        LOGGER.info("success to delete tenant tag of tenant={} tag={}, user={}", entity.getTenant(),
                entity.getClusterTag(), operator);
        return true;
    }

    /**
     * Remove cluster tag from the given cluster entity.
     */
    private void removeClusterTag(InlongClusterEntity entity, String clusterTag, String operator) {
        Set<String> tagSet = Sets.newHashSet(entity.getClusterTags().split(InlongConstants.COMMA));
        tagSet.remove(clusterTag);
        String updateTags = Joiner.on(",").join(tagSet);
        entity.setClusterTags(updateTags);
        entity.setModifier(operator);
        if (InlongConstants.AFFECTED_ONE_ROW != clusterMapper.updateById(entity)) {
            LOGGER.error("cluster has already updated with name={}, type={}, curVersion={}", entity.getName(),
                    entity.getType(), entity.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
    }

    /**
     * Make sure there is no InlongGroup using this tag.
     */
    private void assertNoInlongGroupExists(String clusterTag) {
        List<InlongGroupEntity> groupEntities = groupMapper.selectByClusterTag(clusterTag);
        if (CollectionUtils.isEmpty(groupEntities)) {
            return;
        }
        List<String> groupIds = groupEntities.stream()
                .map(InlongGroupEntity::getInlongGroupId)
                .collect(Collectors.toList());
        String errMsg = String.format("inlong cluster tag [%s] was used by inlong group %s", clusterTag, groupIds);
        LOGGER.error(errMsg);
        throw new BusinessException(errMsg + ", please delete them first");
    }

    private void chkUnmodifiableParams(InlongClusterEntity entity, ClusterRequest request) {
        // check type
        Preconditions.expectEquals(entity.getType(), request.getType(),
                ErrorCodeEnum.INVALID_PARAMETER, "type not allowed modify");
        // check record version
        Preconditions.expectEquals(entity.getVersion(), request.getVersion(),
                ErrorCodeEnum.CONFIG_EXPIRED,
                String.format("record has expired with record version=%d, request version=%d",
                        entity.getVersion(), request.getVersion()));
        // check and append name
        if (StringUtils.isBlank(request.getName())) {
            request.setName(entity.getName());
        } else {
            // check name
            Preconditions.expectEquals(entity.getName(), request.getName(),
                    ErrorCodeEnum.INVALID_PARAMETER, "name not allowed modify");
        }
        // check and append clusterTag
        if (StringUtils.isBlank(request.getClusterTags())) {
            request.setClusterTags(entity.getClusterTags());
        }
    }

    private class InstallTaskRunnable implements Runnable {

        private ClusterNodeRequest request;

        public InstallTaskRunnable(ClusterNodeRequest request) {
            this.request = request;
        }

        @Override
        public void run() {
            if (request == null) {
                return;
            }
            InlongClusterNodeInstallOperator clusterNodeInstallOperator =
                    clusterNodeInstallOperatorFactory.getInstance(request.getType());
            if (request.getIsInstall()) {
                clusterNodeInstallOperator.install(request, request.getCurrentUser());
            } else {
                clusterNodeInstallOperator.reInstall(request, request.getCurrentUser());
            }
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    public void processInstall() {
        LOGGER.info("begin to process install task");
        while (!pendingInstallRequests.isEmpty()) {
            ClusterNodeRequest request = pendingInstallRequests.poll();
            InstallTaskRunnable installTaskRunnable = new InstallTaskRunnable(request);
            executorService.execute(installTaskRunnable);
        }
        LOGGER.info("success to process install task");
    }
}
