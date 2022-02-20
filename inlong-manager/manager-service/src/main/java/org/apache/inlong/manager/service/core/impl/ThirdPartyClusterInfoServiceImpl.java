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

package org.apache.inlong.manager.service.core.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.ThirdPartyClusterInfoEntity;
import org.apache.inlong.manager.dao.mapper.ThirdPartyClusterInfoMapper;
import org.apache.inlong.manager.service.core.ThirdPartyClusterInfoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Implementation of cluster information service layer interface
 */
@Service
@Slf4j
public class ThirdPartyClusterInfoServiceImpl implements ThirdPartyClusterInfoService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThirdPartyClusterInfoServiceImpl.class);

    @Autowired
    private ThirdPartyClusterInfoMapper clusterInfoMapper;

    @Override
    public List<String> listClusterIpByType(String type) {
        ClusterRequest request = ClusterRequest.builder().type(type).build();
        List<ThirdPartyClusterInfoEntity> clusterInfoEntities = clusterInfoMapper.selectByCondition(request);
        List<String> ipList = new ArrayList<>(clusterInfoEntities.size());
        for (ThirdPartyClusterInfoEntity entity : clusterInfoEntities) {
            ipList.add(entity.getIp());
        }
        return ipList;
    }

    @Override
    public List<ClusterInfo> list(ClusterRequest request) {
        LOGGER.info("begin to list cluster by request={}", request);

        List<ThirdPartyClusterInfoEntity> entityList = clusterInfoMapper.selectByCondition(request);
        List<ClusterInfo> infoList = CommonBeanUtils.copyListProperties(entityList, ClusterInfo::new);

        LOGGER.info("success to get cluster");
        return infoList;
    }

    @Override
    public List<ClusterInfo> getClusterInfoByIdList(List<Integer> clusterIdList) {
        if (CollectionUtils.isEmpty(clusterIdList)) {
            return Collections.emptyList();
        }
        List<ThirdPartyClusterInfoEntity> entityList = clusterInfoMapper.selectByIdList(clusterIdList);
        return CommonBeanUtils.copyListProperties(entityList, ClusterInfo::new);
    }

    @Override
    public Integer save(ClusterInfo clusterInfo, String operator) {
        LOGGER.info("begin to insert a cluster info cluster={}", clusterInfo);
        Preconditions.checkNotNull(clusterInfo, "cluster is empty");
        ThirdPartyClusterInfoEntity entity =
                CommonBeanUtils.copyProperties(clusterInfo, ThirdPartyClusterInfoEntity::new);
        entity.setCreator(operator);
        entity.setCreateTime(new Date());
        clusterInfoMapper.insert(entity);
        LOGGER.info("success to add a cluster");
        return entity.getId();
    }

    @Override
    public Boolean update(ClusterInfo clusterInfo, String operator) {
        LOGGER.info("begin to update common cluster={}", clusterInfo);
        Preconditions.checkNotNull(clusterInfo, "cluster is empty");
        Integer id = clusterInfo.getId();
        Preconditions.checkNotNull(id, "cluster id is empty");
        ThirdPartyClusterInfoEntity entity = clusterInfoMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("cluster not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        CommonBeanUtils.copyProperties(clusterInfo, entity, true);
        entity.setModifier(operator);
        clusterInfoMapper.updateByPrimaryKeySelective(entity);
        LOGGER.info("success to update cluster");
        return true;
    }

    @Override
    public Boolean delete(Integer id, String operator) {
        LOGGER.info("begin to delete cluster by id={}", id);
        Preconditions.checkNotNull(id, "cluster id is empty");
        ThirdPartyClusterInfoEntity entity = clusterInfoMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("cluster not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        entity.setIsDeleted(EntityStatus.IS_DELETED.getCode());
        entity.setStatus(EntityStatus.DELETED.getCode());
        entity.setModifier(operator);
        clusterInfoMapper.updateByPrimaryKey(entity);
        LOGGER.info("success to delete cluster");
        return true;
    }

    @Override
    public ClusterInfo get(Integer id) {
        LOGGER.info("begin to get cluster by id={}", id);
        Preconditions.checkNotNull(id, "cluster id is empty");
        ThirdPartyClusterInfoEntity entity = clusterInfoMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("cluster not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        ClusterInfo clusterInfo = CommonBeanUtils.copyProperties(entity, ClusterInfo::new);
        LOGGER.info("success to get cluster info");
        return clusterInfo;
    }

}
