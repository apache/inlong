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

import com.google.gson.Gson;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataProxyClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.ThirdPartyClusterEntity;
import org.apache.inlong.manager.dao.mapper.DataProxyClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.ThirdPartyClusterEntityMapper;
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
    private InlongGroupEntityMapper groupMapper;

    @Autowired
    private DataProxyClusterEntityMapper dataProxyClusterMapper;

    @Autowired
    private ThirdPartyClusterEntityMapper thirdPartyClusterMapper;

    /**
     * query some third-party-cluster info according key, such as "pulsar_adminUrl", "cluster_tube_manager", etc.
     *
     * @param key Param name.
     * @return Value of key in database.
     */
    public String getSpecifiedParam(String key) {
        String result = "";
        ThirdPartyClusterEntity clusterEntity;
        Gson gson = new Gson();
        Map<String, String> params;

        switch (key) {
            case Constant.PULSAR_ADMINURL:
            case Constant.PULSAR_SERVICEURL: {
                clusterEntity = getThirdPartyCluster(Constant.MIDDLEWARE_PULSAR);
                if (clusterEntity != null) {
                    if (key.equals(Constant.PULSAR_SERVICEURL)) {
                        result = clusterEntity.getUrl();
                    } else {
                        params = gson.fromJson(clusterEntity.getExtParams(), Map.class);
                        result = params.get(key);
                    }
                }
                break;
            }
            case Constant.CLUSTER_TUBE_MANAGER:
            case Constant.CLUSTER_TUBE_CLUSTER_ID:
            case Constant.TUBE_MASTER_URL: {
                clusterEntity = getThirdPartyCluster(Constant.MIDDLEWARE_TUBE);
                if (clusterEntity != null) {
                    if (key.equals(Constant.TUBE_MASTER_URL)) {
                        result = clusterEntity.getUrl();
                    } else {
                        params = gson.fromJson(clusterEntity.getExtParams(), Map.class);
                        result = params.get(key);
                    }
                }
                break;
            }
        }

        return result;
    }

    /**
     * Get third party cluster by type.
     *
     * TODO Add more condition for query.
     *
     * @param type Cluster type, such as TUBE, PULSAR, etc.
     */
    private ThirdPartyClusterEntity getThirdPartyCluster(String type) {
        InlongGroupPageRequest request = new InlongGroupPageRequest();
        request.setMiddlewareType(type);
        List<InlongGroupEntity> groupEntities = groupMapper.selectByCondition(request);
        if (groupEntities.isEmpty()) {
            LOGGER.warn("no inlong group found by type={}", type);
            return null;
        }

        Integer clusterId = groupEntities.get(0).getProxyClusterId();
        DataProxyClusterEntity dataProxyCluster = dataProxyClusterMapper.selectByPrimaryKey(clusterId);
        if (dataProxyCluster == null) {
            LOGGER.warn("no data proxy cluster found with id={}", clusterId);
            return null;
        }

        String mqSetName = dataProxyCluster.getMqSetName();
        ClusterRequest mqNameRequest = ClusterRequest.builder().mqSetName(mqSetName).build();
        List<ThirdPartyClusterEntity> thirdPartyClusters = thirdPartyClusterMapper.selectByCondition(mqNameRequest);
        if (thirdPartyClusters.isEmpty()) {
            LOGGER.warn("no related third-party-cluster by type={} and mq set name={}", type, mqSetName);
            return null;
        }

        return thirdPartyClusters.get(0);
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

        GroupState state = GroupState.forCode(inlongGroupEntity.getStatus());
        // Add/modify/delete is not allowed under certain group state
        if (!GroupState.isAllowedUpdate(state)) {
            LOGGER.error("inlong group status was not allowed to add/update/delete related info");
            throw new BusinessException(ErrorCodeEnum.OPT_NOT_ALLOWED_BY_STATUS);
        }

        return inlongGroupEntity;
    }

}
