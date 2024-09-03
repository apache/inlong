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

import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.pojo.cluster.zk.AgentZkClusterDTO;
import org.apache.inlong.manager.pojo.cluster.zk.AgentZkClusterInfo;
import org.apache.inlong.manager.pojo.cluster.zk.AgentZkClusterRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class AgentZkClusterOperator extends AbstractClusterOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentZkClusterOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String clusterType) {
        return getClusterType().equals(clusterType);
    }

    @Override
    public String getClusterType() {
        return ClusterType.AGENT_ZK;
    }

    @Override
    protected void setTargetEntity(ClusterRequest request, InlongClusterEntity targetEntity) {
        AgentZkClusterRequest agentZkClusterRequest = (AgentZkClusterRequest) request;
        CommonBeanUtils.copyProperties(agentZkClusterRequest, targetEntity, true);
        try {
            AgentZkClusterDTO dto =
                    AgentZkClusterDTO.getFromRequest(agentZkClusterRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT,
                    String.format("serialize extParams of agent zk Cluster failure: %s", e.getMessage()));
        }
    }

    @Override
    public ClusterInfo getFromEntity(InlongClusterEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        AgentZkClusterInfo info = new AgentZkClusterInfo();
        CommonBeanUtils.copyProperties(entity, info);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            AgentZkClusterDTO dto = AgentZkClusterDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, info);
        }
        return info;
    }

    @Override
    public Object getClusterInfo(InlongClusterEntity entity) {
        AgentZkClusterInfo agentZkClusterInfo = (AgentZkClusterInfo) this.getFromEntity(entity);
        Map<String, String> map = new HashMap<>();
        map.put("url", agentZkClusterInfo.getUrl());
        return map;
    }
}
