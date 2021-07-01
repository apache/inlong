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

import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.ClusterInfoEntity;
import org.apache.inlong.manager.dao.mapper.ClusterInfoMapper;
import org.apache.inlong.manager.service.core.ClusterInfoService;
import org.apache.inlong.manager.common.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Implementation of cluster information service layer interface
 *
 */
@Service
@Slf4j
public class ClusterInfoServiceImpl implements ClusterInfoService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInfoServiceImpl.class);

    @Autowired
    private ClusterInfoMapper clusterInfoMapper;

    @Override
    public List<String> listClusterIpByType(String type) {
        ClusterRequest request = ClusterRequest.builder().type(type).build();
        List<ClusterInfoEntity> clusterInfoEntities = clusterInfoMapper.selectByCondition(request);
        List<String> ipList = new ArrayList<>(clusterInfoEntities.size());
        for (ClusterInfoEntity entity : clusterInfoEntities) {
            ipList.add(entity.getIp());
        }
        return ipList;
    }

    @Override
    public List<ClusterInfo> list(ClusterRequest request) {
        LOGGER.info("begin to list cluster by request={}", request);

        List<ClusterInfoEntity> entityList = clusterInfoMapper.selectByCondition(request);
        List<ClusterInfo> infoList = CommonBeanUtils.copyListProperties(entityList, ClusterInfo::new);

        LOGGER.info("success to get cluster");
        return infoList;
    }

    @Override
    public List<ClusterInfo> getClusterInfoByIdList(List<Integer> clusterIdList) {
        if (CollectionUtils.isEmpty(clusterIdList)) {
            return Collections.emptyList();
        }
        List<ClusterInfoEntity> entityList = clusterInfoMapper.selectByIdList(clusterIdList);
        return CommonBeanUtils.copyListProperties(entityList, ClusterInfo::new);
    }

}
