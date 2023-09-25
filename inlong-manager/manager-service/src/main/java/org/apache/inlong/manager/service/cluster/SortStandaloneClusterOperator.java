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
import org.apache.inlong.manager.pojo.cluster.sortstandalone.SortStandaloneClusterDTO;
import org.apache.inlong.manager.pojo.cluster.sortstandalone.SortStandaloneClusterInfo;
import org.apache.inlong.manager.pojo.cluster.sortstandalone.SortStandaloneClusterRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SortStandaloneClusterOperator extends AbstractClusterOperator {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    protected void setTargetEntity(ClusterRequest request, InlongClusterEntity targetEntity) {
        SortStandaloneClusterRequest standaloneRequest = (SortStandaloneClusterRequest) request;
        CommonBeanUtils.copyProperties(standaloneRequest, targetEntity, true);
        try {
            SortStandaloneClusterDTO dto = SortStandaloneClusterDTO.getFromRequest(standaloneRequest,
                    targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
            log.debug("success to set entity for SortStandalone cluster");
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT,
                    String.format("serialize extParams of SortStandalone Cluster failure: %s", e.getMessage()));
        }
    }

    @Override
    public Boolean accept(String clusterType) {
        return getClusterType().equals(clusterType);
    }

    @Override
    public String getClusterType() {
        return ClusterType.SORTSTANDALONE;
    }

    @Override
    public ClusterInfo getFromEntity(InlongClusterEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        SortStandaloneClusterInfo clusterInfo = new SortStandaloneClusterInfo();
        CommonBeanUtils.copyProperties(entity, clusterInfo);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            SortStandaloneClusterDTO dto = SortStandaloneClusterDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, clusterInfo);
        }
        return clusterInfo;
    }

}
