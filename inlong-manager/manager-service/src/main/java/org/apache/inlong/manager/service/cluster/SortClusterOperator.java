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
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.pojo.cluster.sort.cls.SortClsClusterInfo;
import org.apache.inlong.manager.pojo.cluster.sort.es.SortEsClusterInfo;
import org.apache.inlong.manager.pojo.cluster.sort.http.SortHttpClusterInfo;
import org.apache.inlong.manager.pojo.cluster.sort.kafka.SortKafkaClusterInfo;
import org.apache.inlong.manager.pojo.cluster.sort.pulsar.SortPulsarClusterInfo;
import org.apache.inlong.manager.pojo.sort.BaseSortClusterDTO;
import org.apache.inlong.manager.pojo.sort.BaseSortClusterRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

@Service
public class SortClusterOperator extends AbstractClusterOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortClusterOperator.class);

    private static final Set<String> SORT_CLUSTER_SET = new HashSet<String>() {

        {
            add(ClusterType.SORT_HTTP);
            add(ClusterType.SORT_CLS);
            add(ClusterType.SORT_PULSAR);
            add(ClusterType.SORT_ES);
            add(ClusterType.SORT_KAFKA);
        }
    };

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String clusterType) {
        return SORT_CLUSTER_SET.contains(clusterType);
    }

    @Override
    protected void setTargetEntity(ClusterRequest request, InlongClusterEntity targetEntity) {
        BaseSortClusterRequest clusterRequest = (BaseSortClusterRequest) request;
        CommonBeanUtils.copyProperties(clusterRequest, targetEntity, true);
        try {
            BaseSortClusterDTO dto = BaseSortClusterDTO.getFromRequest(clusterRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
            LOGGER.info("success to set entity for sort cluster");
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }

    @Override
    public ClusterInfo getFromEntity(InlongClusterEntity entity) {
        Preconditions.expectNotNull(entity, ErrorCodeEnum.CLUSTER_NOT_FOUND.getMessage());

        ClusterInfo sortClusterInfo;
        switch (entity.getType()) {
            case ClusterType.SORT_HTTP:
                sortClusterInfo = new SortHttpClusterInfo();
                break;
            case ClusterType.SORT_CLS:
                sortClusterInfo = new SortClsClusterInfo();
                break;
            case ClusterType.SORT_PULSAR:
                sortClusterInfo = new SortPulsarClusterInfo();
                break;
            case ClusterType.SORT_ES:
                sortClusterInfo = new SortEsClusterInfo();
                break;
            case ClusterType.SORT_KAFKA:
                sortClusterInfo = new SortKafkaClusterInfo();
                break;
            default:
                throw new BusinessException("unsupported cluster type " + entity.getType());
        }

        CommonBeanUtils.copyProperties(entity, sortClusterInfo);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            BaseSortClusterDTO dto = BaseSortClusterDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, sortClusterInfo);
        }
        return sortClusterInfo;
    }

}
