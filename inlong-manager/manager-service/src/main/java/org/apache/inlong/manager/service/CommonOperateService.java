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

package org.apache.inlong.manager.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterPageRequest;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Common operation service
 */
@Service
public class CommonOperateService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonOperateService.class);

    @Autowired
    public ObjectMapper objectMapper;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;

    /**
     * query some third-party-cluster info according key, such as "pulsar_adminUrl", "cluster_tube_manager", etc.
     *
     * @param key Param name.
     * @return Value of key in database.
     */
    public String getSpecifiedParam(String key) {
        String result = "";
        InlongClusterEntity clusterEntity;
        Gson gson = new Gson();
        Map<String, String> params;

        switch (key) {
            case InlongGroupSettings.PULSAR_SERVICE_URL: {
                clusterEntity = getMQCluster(MQType.PULSAR);
                if (clusterEntity != null) {
                    result = clusterEntity.getUrl();
                }
                break;
            }
            case InlongGroupSettings.PULSAR_ADMIN_URL: {
                clusterEntity = getMQCluster(MQType.PULSAR);
                if (clusterEntity != null) {
                    params = gson.fromJson(clusterEntity.getExtParams(), Map.class);
                    result = params.get(key);
                }
                break;
            }
            case InlongGroupSettings.TUBE_MANAGER_URL:
            case InlongGroupSettings.TUBE_CLUSTER_ID:
            case InlongGroupSettings.TUBE_MASTER_URL: {
                clusterEntity = getMQCluster(MQType.TUBE);
                if (clusterEntity != null) {
                    if (key.equals(InlongGroupSettings.TUBE_MASTER_URL)) {
                        result = clusterEntity.getUrl();
                    } else {
                        params = gson.fromJson(clusterEntity.getExtParams(), Map.class);
                        result = params.get(key);
                    }
                }
                break;
            }
            default:
                LOGGER.warn("case warn key {}", key);
        }
        return result;
    }

    /**
     * Get third party cluster by type.
     *
     * TODO Add data_proxy_cluster_name for query.
     */
    private InlongClusterEntity getMQCluster(MQType type) {
        List<InlongClusterEntity> clusterList = clusterMapper.selectByNameAndType(null,
                InlongGroupSettings.CLUSTER_DATA_PROXY);
        if (CollectionUtils.isEmpty(clusterList)) {
            LOGGER.warn("no data proxy cluster found");
            return null;
        }

        String clusterTag = clusterList.get(0).getClusterTag();
        InlongClusterPageRequest request = new InlongClusterPageRequest();
        request.setClusterTag(clusterTag);
        request.setType(type.getType());
        List<InlongClusterEntity> mqClusterList = clusterMapper.selectByCondition(request);
        if (CollectionUtils.isEmpty(mqClusterList)) {
            LOGGER.warn("no mq cluster found by cluster tag={} and type={}", clusterTag, type);
            return null;
        }

        return mqClusterList.get(0);
    }

    /**
     * Get Pulsar cluster by the given type.
     *
     * @return Pulsar cluster info.
     */
    public PulsarClusterInfo getPulsarClusterInfo(String type) {
        MQType mqType = MQType.forType(type);
        InlongClusterEntity clusterEntity = getMQCluster(mqType);
        if (clusterEntity == null || StringUtils.isBlank(clusterEntity.getExtParams())) {
            throw new BusinessException("pulsar cluster or pulsar ext params is empty");
        }

        PulsarClusterInfo pulsarCluster = PulsarClusterInfo.builder()
                .brokerServiceUrl(clusterEntity.getUrl())
                .token(clusterEntity.getToken())
                .build();
        try {
            Map<String, String> configParams = objectMapper.readValue(clusterEntity.getExtParams(), Map.class);
            String adminUrl = configParams.get(InlongGroupSettings.PULSAR_ADMIN_URL);
            pulsarCluster.setAdminUrl(adminUrl);
        } catch (Exception e) {
            LOGGER.error("parse pulsar cluster info error: ", e);
        }

        Preconditions.checkNotNull(pulsarCluster.getAdminUrl(), "adminUrl is empty, check third party cluster table");
        pulsarCluster.setType(clusterEntity.getType());
        return pulsarCluster;
    }

    /**
     * Check whether the inlong group status is temporary
     *
     * @param groupId Inlong group id
     * @return Inlong group entity, for caller reuse
     */
    public InlongGroupEntity checkGroupStatus(String groupId, String operator) {
        InlongGroupEntity inlongGroupEntity = groupMapper.selectByGroupId(groupId);
        Preconditions.checkNotNull(inlongGroupEntity, "groupId is invalid");

        List<String> managers = Arrays.asList(inlongGroupEntity.getInCharges().split(","));
        Preconditions.checkTrue(managers.contains(operator),
                String.format(ErrorCodeEnum.USER_IS_NOT_MANAGER.getMessage(), operator, managers));

        GroupStatus state = GroupStatus.forCode(inlongGroupEntity.getStatus());
        // Add/modify/delete is not allowed under certain group state
        if (GroupStatus.notAllowedUpdate(state)) {
            LOGGER.error("inlong group status was not allowed to add/update/delete related info");
            throw new BusinessException(ErrorCodeEnum.OPT_NOT_ALLOWED_BY_STATUS);
        }

        return inlongGroupEntity;
    }
}
