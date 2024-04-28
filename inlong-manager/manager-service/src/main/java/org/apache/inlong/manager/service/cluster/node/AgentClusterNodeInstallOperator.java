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

import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ModuleType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.entity.ModuleConfigEntity;
import org.apache.inlong.manager.dao.entity.PackageConfigEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.ModuleConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.PackageConfigEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterDTO;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterNodeRequest;
import org.apache.inlong.manager.service.cmd.CommandExecutor;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Service
public class AgentClusterNodeInstallOperator implements InlongClusterNodeInstallOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentClusterNodeInstallOperator.class);

    @Autowired
    private InlongClusterEntityMapper clusterEntityMapper;
    @Autowired
    private CommandExecutor commandExecutor;
    @Autowired
    private ModuleConfigEntityMapper moduleConfigEntityMapper;
    @Autowired
    private PackageConfigEntityMapper packageConfigEntityMapper;

    @Value("${audit.proxy.url}")
    private String auditProxyUrl;
    @Value("${agent.install.path}")
    private String agentInstallPath;
    @Value("${manager.url}")
    private String managerUrl;

    @Override
    public Boolean accept(String clusterType) {
        return getClusterNodeType().equals(clusterType);
    }

    @Override
    public String getClusterNodeType() {
        return ClusterType.AGENT;
    }

    @Override
    public boolean install(ClusterNodeRequest clusterNodeRequest, String operator) {
        LOGGER.info("begin to insert agent inlong cluster node={}", clusterNodeRequest);
        try {
            InlongClusterEntity clusterEntity = clusterEntityMapper.selectById(clusterNodeRequest.getParentId());
            AgentClusterDTO agentClusterDTO = AgentClusterDTO.getFromJson(clusterEntity.getExtParams());
            AgentClusterNodeRequest request = (AgentClusterNodeRequest) clusterNodeRequest;
            commandExecutor.mkdir(request, agentInstallPath);
            String downLoadUrl = getInstallerDownLoadUrl(request);
            String fileName = downLoadUrl.substring(downLoadUrl.lastIndexOf('/') + 1);
            commandExecutor.downLoadPackage(request, agentInstallPath, downLoadUrl);
            commandExecutor.tarPackage(request, fileName, agentInstallPath);
            String confFile = agentInstallPath + "conf/installer.properties";
            Map<String, String> configMap = new HashMap<>();
            configMap.put("agent.local.ip", request.getIp());
            configMap.put("agent.manager.addr", managerUrl);
            configMap.put("agent.manager.auth.secretId", agentClusterDTO.getAuthSecretId());
            configMap.put("agent.manager.auth.secretKey", agentClusterDTO.getAuthSecretKey());
            configMap.put("agent.cluster.tag", clusterEntity.getClusterTags());
            configMap.put("agent.cluster.name", clusterEntity.getName());
            configMap.put("agent.proxys", auditProxyUrl);
            commandExecutor.modifyConfig(request, configMap, confFile);
            String startCmd = agentInstallPath + "bin/installer.sh start";
            commandExecutor.execRemote(request, startCmd);

        } catch (Exception e) {
            String errMsg = String.format("install installer failed for ip=%s", clusterNodeRequest.getIp());
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
        LOGGER.info("success to insert agent inlong cluster node={}", clusterNodeRequest);
        return true;
    }

    @Override
    public boolean unload(InlongClusterNodeEntity clusterNodeEntity, String operator) {
        // todo Provide agent uninstallation capability
        InlongClusterEntity clusterEntity = clusterEntityMapper.selectById(clusterNodeEntity.getParentId());
        return true;
    }

    private String getInstallerDownLoadUrl(AgentClusterNodeRequest request) {
        if (CollectionUtils.isEmpty(request.getModuleIdList())) {
            throw new BusinessException(
                    String.format("install failed when module id list is null for ip=%s, type=%s", request.getIp(),
                            request.getType()));
        }
        for (Integer moduleId : request.getModuleIdList()) {
            ModuleConfigEntity moduleConfigEntity = moduleConfigEntityMapper.selectByPrimaryKey(moduleId);
            if (Objects.equals(moduleConfigEntity.getType(), ModuleType.INSTALLER.name())) {
                PackageConfigEntity packageConfigEntity = packageConfigEntityMapper.selectByPrimaryKey(
                        moduleConfigEntity.getPackageId());
                return packageConfigEntity.getDownloadUrl();
            }
        }
        throw new BusinessException(String.format("cant get installer download url for ip=%s, type=%s", request.getIp(),
                request.getType()));
    }
}
