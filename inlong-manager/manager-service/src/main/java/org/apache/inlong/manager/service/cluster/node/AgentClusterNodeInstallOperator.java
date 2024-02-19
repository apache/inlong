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
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterNodeRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AgentClusterNodeInstallOperator implements InlongClusterNodeInstallOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentClusterNodeInstallOperator.class);

    @Autowired
    private InlongClusterEntityMapper clusterEntityMapper;

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
        // todo Provide agent installation capability
        AgentClusterNodeRequest agentNodeRequest = (AgentClusterNodeRequest) clusterNodeRequest;
        InlongClusterEntity clusterEntity = clusterEntityMapper.selectById(clusterNodeRequest.getParentId());
        return true;
    }

    @Override
    public boolean unload(InlongClusterNodeEntity clusterNodeEntity, String operator) {
        // todo Provide agent uninstallation capability
        InlongClusterEntity clusterEntity = clusterEntityMapper.selectById(clusterNodeEntity.getParentId());
        return true;
    }
}
