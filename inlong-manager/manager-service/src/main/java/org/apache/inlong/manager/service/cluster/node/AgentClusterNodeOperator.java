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

package org.apache.inlong.manager.service.cluster.node;

import org.apache.inlong.common.pojo.agent.AgentResponseCode;
import org.apache.inlong.common.pojo.agent.installer.ConfigRequest;
import org.apache.inlong.common.pojo.agent.installer.ConfigResult;
import org.apache.inlong.common.pojo.agent.installer.ModuleConfig;
import org.apache.inlong.common.pojo.agent.installer.PackageConfig;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.ModuleType;
import org.apache.inlong.manager.common.enums.NodeStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.AgentTaskConfigEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.entity.ModuleConfigEntity;
import org.apache.inlong.manager.dao.entity.PackageConfigEntity;
import org.apache.inlong.manager.dao.mapper.AgentTaskConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.ModuleConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.PackageConfigEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterNodeDTO;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterNodeResponse;
import org.apache.inlong.manager.pojo.module.ModuleDTO;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Agent cluster node operator.
 */
@Slf4j
@Service
public class AgentClusterNodeOperator extends AbstractClusterNodeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentClusterNodeOperator.class);
    private static final Gson GSON = new Gson();
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private AgentTaskConfigEntityMapper agentTaskConfigEntityMapper;
    @Autowired
    private PackageConfigEntityMapper packageConfigEntityMapper;
    @Autowired
    private ModuleConfigEntityMapper moduleConfigEntityMapper;

    @Override
    public Boolean accept(String clusterNodeType) {
        return getClusterNodeType().equals(clusterNodeType);
    }

    @Override
    public String getClusterNodeType() {
        return ClusterType.AGENT;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Integer saveOpt(ClusterNodeRequest request, String operator) {
        InlongClusterNodeEntity entity = CommonBeanUtils.copyProperties(request, InlongClusterNodeEntity::new);
        // set the ext params
        this.setTargetEntity(request, entity);

        entity.setCreator(operator);
        entity.setModifier(operator);
        entity.setStatus(NodeStatus.HEARTBEAT_TIMEOUT.getStatus());
        clusterNodeMapper.insert(entity);
        InlongClusterEntity clusterEntity = clusterMapper.selectById(request.getParentId());
        String clusterName = clusterEntity.getName();
        String ip = request.getIp();
        updateModuleConfig(ip, clusterName);
        return entity.getId();
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public void updateOpt(ClusterNodeRequest request, String operator) {
        InlongClusterNodeEntity entity = clusterNodeMapper.selectById(request.getId());
        // set the ext params
        this.setTargetEntity(request, entity);
        entity.setModifier(operator);
        if (InlongConstants.AFFECTED_ONE_ROW != clusterNodeMapper.updateByIdSelective(entity)) {
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED,
                    String.format(
                            "cluster node has already updated with ip=%s, port=%s, protocolType=%s, type=%s, curVersion=%s",
                            entity.getIp(), entity.getPort(), entity.getProtocolType(), entity.getType(),
                            entity.getVersion()));
        }
        InlongClusterEntity clusterEntity = clusterMapper.selectById(request.getParentId());
        String clusterName = clusterEntity.getName();
        String ip = request.getIp();
        updateModuleConfig(ip, clusterName);
        LOGGER.debug("success to update inlong cluster node={}", request);
    }

    @Override
    public ClusterNodeResponse getFromEntity(InlongClusterNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        AgentClusterNodeResponse agentClusterNodeResponse = new AgentClusterNodeResponse();
        CommonBeanUtils.copyProperties(entity, agentClusterNodeResponse);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            AgentClusterNodeDTO dto = AgentClusterNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, agentClusterNodeResponse);
        }

        LOGGER.debug("success to get agent cluster node info from entity");
        return agentClusterNodeResponse;
    }

    @Override
    protected void setTargetEntity(ClusterNodeRequest request, InlongClusterNodeEntity targetEntity) {
        AgentClusterNodeRequest agentNodeRequest = (AgentClusterNodeRequest) request;
        CommonBeanUtils.copyProperties(agentNodeRequest, targetEntity, true);
        try {
            AgentClusterNodeDTO dto = AgentClusterNodeDTO.getFromRequest(agentNodeRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
            LOGGER.debug("success to set entity for agent cluster node");
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT,
                    String.format("serialize extParams of Agent ClusterNode failure: %s", e.getMessage()));
        }
    }

    public void updateModuleConfig(String ip, String clusterName) {
        try {
            if (StringUtils.isBlank(clusterName) || StringUtils.isBlank(ip)) {
                LOGGER.info("no need to update module config when ip or cluster name is null");
                return;
            }
            ConfigRequest configRequest = new ConfigRequest();
            LOGGER.info("begin to update module config for cluster name={}, ip={}", clusterName, ip);
            configRequest.setLocalIp(ip);
            configRequest.setClusterName(clusterName);
            ConfigResult configResult = loadModuleConfigs(configRequest);
            AgentTaskConfigEntity existEntity = agentTaskConfigEntityMapper.selectByIdentifier(ip, clusterName);
            AgentTaskConfigEntity agentTaskConfigEntity = existEntity == null ? new AgentTaskConfigEntity()
                    : CommonBeanUtils.copyProperties(existEntity, AgentTaskConfigEntity::new, true);
            agentTaskConfigEntity.setAgentIp(ip);
            agentTaskConfigEntity.setClusterName(clusterName);
            agentTaskConfigEntity.setModuleParams(objectMapper.writeValueAsString(configResult));
            if (existEntity == null) {
                agentTaskConfigEntityMapper.insert(agentTaskConfigEntity);
            } else {
                agentTaskConfigEntityMapper.updateByIdSelective(agentTaskConfigEntity);
            }
            LOGGER.info("success to update module config for cluster name={}, ip={}", clusterName, ip);
        } catch (Exception e) {
            LOGGER.error("load module config failed", e);
            throw new BusinessException("load module config faield");
        }
    }

    private ConfigResult loadModuleConfigs(ConfigRequest request) {
        final String clusterName = request.getClusterName();
        final String ip = request.getLocalIp();
        LOGGER.debug("begin to load config for installer = {}", request);
        Preconditions.expectTrue(StringUtils.isNotBlank(clusterName), "cluster name is blank");
        InlongClusterEntity clusterEntity = clusterMapper.selectByNameAndType(clusterName, ClusterType.AGENT);
        List<ModuleConfig> configs = new ArrayList<>();
        if (clusterEntity != null) {
            List<InlongClusterNodeEntity> clusterNodeEntityList =
                    clusterNodeMapper.selectByParentIdAndIp(clusterEntity.getId(), ip);
            if (CollectionUtils.isNotEmpty(clusterNodeEntityList)) {
                AgentClusterNodeDTO dto = AgentClusterNodeDTO.getFromJson(clusterNodeEntityList.get(0).getExtParams());
                configs = getModuleConfigs(dto);
            }
        }
        String jsonStr = GSON.toJson(configs);
        String configMd5 = DigestUtils.md5Hex(jsonStr);

        ConfigResult configResult = ConfigResult.builder().moduleList(configs).md5(configMd5)
                .code(AgentResponseCode.SUCCESS)
                .build();
        LOGGER.info("success load module config, size = {}", configResult.getModuleList().size());
        return configResult;
    }

    private List<ModuleConfig> getModuleConfigs(AgentClusterNodeDTO dto) {
        List<Integer> moduleIdList = dto.getModuleIdList();
        List<ModuleConfig> configs = new ArrayList<>();
        if (CollectionUtils.isEmpty(moduleIdList)) {
            return configs;
        }
        for (Integer moduleId : moduleIdList) {
            ModuleConfigEntity moduleConfigEntity = moduleConfigEntityMapper.selectByPrimaryKey(moduleId);
            if (moduleConfigEntity == null) {
                continue;
            }
            ModuleConfig moduleConfig = CommonBeanUtils.copyProperties(moduleConfigEntity, ModuleConfig::new);
            moduleConfig.setId(ModuleType.forType(moduleConfigEntity.getType()).getModuleId());
            moduleConfig.setEntityId(moduleConfigEntity.getId());
            PackageConfigEntity packageConfigEntity =
                    packageConfigEntityMapper.selectByPrimaryKey(moduleConfigEntity.getPackageId());
            moduleConfig.setPackageConfig(CommonBeanUtils.copyProperties(packageConfigEntity, PackageConfig::new));

            ModuleDTO moduleDTO = JsonUtils.parseObject(moduleConfigEntity.getExtParams(), ModuleDTO.class);
            moduleConfig = CommonBeanUtils.copyProperties(moduleDTO, moduleConfig, true);
            Integer restartTime = 0;
            if (Objects.equals(moduleConfigEntity.getType(), ModuleType.AGENT.name())) {
                restartTime = dto.getAgentRestartTime();
            }
            if (Objects.equals(moduleConfigEntity.getType(), ModuleType.INSTALLER.name())) {
                restartTime = dto.getInstallRestartTime();
            }
            moduleConfig.setRestartTime(restartTime);
            moduleConfig.setProcessesNum(1);
            String moduleStr = GSON.toJson(moduleConfig);
            String moduleMd5 = DigestUtils.md5Hex(moduleStr);
            moduleConfig.setMd5(moduleMd5);
            configs.add(moduleConfig);
        }
        return configs;
    }
}
