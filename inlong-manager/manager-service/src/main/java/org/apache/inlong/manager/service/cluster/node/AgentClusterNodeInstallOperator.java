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

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ModuleType;
import org.apache.inlong.manager.common.enums.NodeStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.AESUtils;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.entity.ModuleConfigEntity;
import org.apache.inlong.manager.dao.entity.PackageConfigEntity;
import org.apache.inlong.manager.dao.entity.UserEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.ModuleConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.PackageConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.UserEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterNodeDTO;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterNodeRequest;
import org.apache.inlong.manager.service.cmd.CommandExecutor;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Service
public class AgentClusterNodeInstallOperator implements InlongClusterNodeInstallOperator {

    public static final String INSTALLER_CONF_PATH = "/conf/installer.properties";
    public static final String INSTALLER_START_CMD = "/bin/installer.sh start";
    public static final String INSTALLER_RESTART_CMD = "/bin/installer.sh restart";
    public static final String INSTALLER_STOP_CMD = "/bin/installer.sh restart";
    public static final String AGENT_MANAGER_AUTH_SECRET_ID = "agent.manager.auth.secretId";
    public static final String AGENT_MANAGER_AUTH_SECRET_KEY = "agent.manager.auth.secretKey";
    public static final String AGENT_MANAGER_ADDR = "agent.manager.addr";
    public static final String AGENT_CLUSTER_NAME = "agent.cluster.name";
    public static final String AGENT_CLUSTER_TAG = "agent.cluster.tag";
    public static final String AGENT_LOCAL_IP = "agent.local.ip";
    private static final Logger LOGGER = LoggerFactory.getLogger(AgentClusterNodeInstallOperator.class);
    @Autowired
    private InlongClusterEntityMapper clusterEntityMapper;
    @Autowired
    private CommandExecutor commandExecutor;
    @Autowired
    private ModuleConfigEntityMapper moduleConfigEntityMapper;
    @Autowired
    private PackageConfigEntityMapper packageConfigEntityMapper;
    @Autowired
    private UserEntityMapper userEntityMapper;
    @Autowired
    private InlongClusterNodeEntityMapper clusterNodeEntityMapper;

    @Value("${agent.install.path:inlong/inlong-installer/}")
    private String agentInstallPath;
    @Value("${manager.url:127.0.0.1:8083}")
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
        LOGGER.info("begin to insert agent cluster node={}", clusterNodeRequest);
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = dateFormat.format(now);
        try {
            clusterNodeEntityMapper.updateOperateLogById(clusterNodeRequest.getId(), NodeStatus.INSTALLING.getStatus(),
                    currentTime + InlongConstants.BLANK + "begin to install");
            AgentClusterNodeRequest request = (AgentClusterNodeRequest) clusterNodeRequest;
            deployInstaller(request, operator);
            String startCmd = agentInstallPath + INSTALLER_START_CMD;
            commandExecutor.execRemote(request, startCmd);
            clusterNodeEntityMapper.updateOperateLogById(clusterNodeRequest.getId(),
                    NodeStatus.INSTALL_SUCCESS.getStatus(), currentTime + InlongConstants.BLANK + "success to install");
        } catch (Exception e) {
            clusterNodeEntityMapper.updateOperateLogById(clusterNodeRequest.getId(),
                    NodeStatus.INSTALL_FAILED.getStatus(), currentTime + InlongConstants.BLANK + e.getMessage());
            String errMsg = String.format("install agent cluster node failed for ip=%s", clusterNodeRequest.getIp());
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
        LOGGER.info("success to install agent cluster node={}", clusterNodeRequest);
        return true;
    }

    @Override
    public boolean reInstall(ClusterNodeRequest clusterNodeRequest, String operator) {
        LOGGER.info("begin to reInstall agent cluster node={}", clusterNodeRequest);
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = dateFormat.format(now);
        try {
            clusterNodeEntityMapper.updateOperateLogById(clusterNodeRequest.getId(), NodeStatus.INSTALLING.getStatus(),
                    currentTime + InlongConstants.BLANK + "begin to reinstall");
            AgentClusterNodeRequest request = (AgentClusterNodeRequest) clusterNodeRequest;
            commandExecutor.rmDir(request, agentInstallPath.substring(0, agentInstallPath.lastIndexOf(File.separator)));
            deployInstaller(request, operator);
            String reStartCmd = agentInstallPath + INSTALLER_RESTART_CMD;
            commandExecutor.execRemote(request, reStartCmd);
            clusterNodeEntityMapper.updateOperateLogById(clusterNodeRequest.getId(), NodeStatus.NORMAL.getStatus(),
                    currentTime + InlongConstants.BLANK + "success to reinstall");
        } catch (Exception e) {
            clusterNodeEntityMapper.updateOperateLogById(clusterNodeRequest.getId(),
                    NodeStatus.INSTALL_FAILED.getStatus(), currentTime + InlongConstants.BLANK + e.getMessage());
            String errMsg = String.format("reInstall agent cluster node failed for ip=%s", clusterNodeRequest.getIp());
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
        LOGGER.info("success to re reInstall agent cluster node={}", clusterNodeRequest);
        return true;
    }

    @Override
    public boolean unload(InlongClusterNodeEntity clusterNodeEntity, String operator) {
        try {
            AgentClusterNodeRequest request = CommonBeanUtils.copyProperties(clusterNodeEntity,
                    AgentClusterNodeRequest::new, true);
            AgentClusterNodeDTO agentClusterNodeDTO = AgentClusterNodeDTO.getFromJson(clusterNodeEntity.getExtParams());
            CommonBeanUtils.copyProperties(agentClusterNodeDTO, request, true);
            String stopCmd = agentInstallPath + INSTALLER_STOP_CMD;
            commandExecutor.execRemote(request, stopCmd);
            commandExecutor.rmDir(request, agentInstallPath.substring(0, agentInstallPath.lastIndexOf(File.separator)));
        } catch (Exception e) {
            clusterNodeEntityMapper.updateOperateLogById(clusterNodeEntity.getId(),
                    NodeStatus.UNLOAD_FAILED.getStatus(), e.getMessage());
            String errMsg = String.format("unload agent cluster node failed for ip=%s", clusterNodeEntity.getIp());
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
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
        throw new BusinessException(
                String.format("can't get installer download url for ip=%s, type=%s", request.getIp(),
                        request.getType()));
    }

    private void deployInstaller(AgentClusterNodeRequest request, String operator) throws Exception {
        InlongClusterEntity clusterEntity = clusterEntityMapper.selectById(request.getParentId());
        commandExecutor.mkdir(request, agentInstallPath);
        String downLoadUrl = getInstallerDownLoadUrl(request);
        String fileName = downLoadUrl.substring(downLoadUrl.lastIndexOf('/') + 1);
        commandExecutor.downLoadPackage(request, agentInstallPath, downLoadUrl);
        commandExecutor.tarPackage(request, fileName, agentInstallPath);
        String confFile = agentInstallPath + INSTALLER_CONF_PATH;
        Map<String, String> configMap = new HashMap<>();
        configMap.put(AGENT_LOCAL_IP, request.getIp());
        configMap.put(AGENT_MANAGER_ADDR, managerUrl);
        UserEntity userInfo = userEntityMapper.selectByName(operator);
        Preconditions.expectNotNull(userInfo, "User doesn't exist");
        String secretKey =
                new String(AESUtils.decryptAsString(userInfo.getSecretKey(), userInfo.getEncryptVersion()));
        configMap.put(AGENT_MANAGER_AUTH_SECRET_ID, operator);
        configMap.put(AGENT_MANAGER_AUTH_SECRET_KEY, secretKey);
        configMap.put(AGENT_CLUSTER_TAG, clusterEntity.getClusterTags());
        configMap.put(AGENT_CLUSTER_NAME, clusterEntity.getName());
        commandExecutor.modifyConfig(request, configMap, confFile);
    }
}
