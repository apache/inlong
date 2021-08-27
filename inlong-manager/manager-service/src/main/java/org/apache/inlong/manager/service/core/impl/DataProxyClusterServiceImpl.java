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

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.cluster.DataProxyClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.DataProxyClusterPageRequest;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyConfig;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyIpRequest;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyIpResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataProxyClusterEntity;
import org.apache.inlong.manager.dao.mapper.BusinessEntityMapper;
import org.apache.inlong.manager.dao.mapper.DataProxyClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.SourceFileDetailEntityMapper;
import org.apache.inlong.manager.service.core.DataProxyClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * DataProxy cluster service layer implementation class
 */
@Service
@Slf4j
public class DataProxyClusterServiceImpl implements DataProxyClusterService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataProxyClusterServiceImpl.class);

    @Autowired
    private DataProxyClusterEntityMapper dataProxyClusterMapper;
    @Autowired
    private SourceFileDetailEntityMapper sourceFileDetailMapper;
    @Autowired
    private BusinessEntityMapper businessEntityMapper;

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Integer save(DataProxyClusterInfo clusterInfo, String operator) {
        LOGGER.info("begin to save data proxy cluster={}", clusterInfo);
        Preconditions.checkNotNull(clusterInfo, "data proxy cluster is empty");

        DataProxyClusterEntity entity = CommonBeanUtils.copyProperties(clusterInfo, DataProxyClusterEntity::new);

        entity.setCreator(operator);
        entity.setModifier(operator);
        entity.setCreateTime(new Date());
        dataProxyClusterMapper.insertSelective(entity);

        LOGGER.info("success to save data proxy cluster");
        return entity.getId();
    }

    @Override
    public DataProxyClusterInfo get(Integer id) {
        LOGGER.info("begin to get data proxy cluster by id={}", id);
        Preconditions.checkNotNull(id, "data proxy cluster id is empty");

        DataProxyClusterEntity entity = dataProxyClusterMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("data proxy cluster not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        DataProxyClusterInfo clusterInfo = CommonBeanUtils.copyProperties(entity, DataProxyClusterInfo::new);

        LOGGER.info("success to get data proxy cluster info");
        return clusterInfo;
    }

    @Override
    public PageInfo<DataProxyClusterInfo> listByCondition(DataProxyClusterPageRequest request) {
        LOGGER.info("begin to list data proxy cluster by {}", request);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<DataProxyClusterEntity> entityPage = (Page<DataProxyClusterEntity>) dataProxyClusterMapper
                .selectByCondition(request);
        List<DataProxyClusterInfo> clusterList = CommonBeanUtils
                .copyListProperties(entityPage, DataProxyClusterInfo::new);
        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<DataProxyClusterInfo> page = new PageInfo<>(clusterList);
        page.setTotal(entityPage.getTotal());

        LOGGER.info("success to list data proxy cluster");
        return page;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Boolean update(DataProxyClusterInfo clusterInfo, String operator) {
        LOGGER.info("begin to update data proxy cluster={}", clusterInfo);
        Preconditions.checkNotNull(clusterInfo, "data proxy cluster is empty");
        Integer id = clusterInfo.getId();
        Preconditions.checkNotNull(id, "data proxy cluster id is empty");

        DataProxyClusterEntity entity = dataProxyClusterMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("data proxy cluster not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        CommonBeanUtils.copyProperties(clusterInfo, entity, true);
        entity.setModifier(operator);
        dataProxyClusterMapper.updateByPrimaryKeySelective(entity);

        LOGGER.info("success to update data proxy cluster");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Boolean delete(Integer id, String operator) {
        LOGGER.info("begin to delete data proxy cluster by id={}", id);
        Preconditions.checkNotNull(id, "data proxy cluster id is empty");

        DataProxyClusterEntity entity = dataProxyClusterMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("data proxy cluster not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        entity.setIsDeleted(EntityStatus.IS_DELETED.getCode());
        entity.setStatus(EntityStatus.DELETED.getCode());
        entity.setModifier(operator);
        dataProxyClusterMapper.updateByPrimaryKey(entity);

        LOGGER.info("success to delete data proxy cluster");
        return true;
    }

    @Override
    public List<DataProxyIpResponse> getIpList(DataProxyIpRequest request) {
        LOGGER.debug("begin to get data proxy ip list, request: {}", request);

        List<DataProxyClusterEntity> entityList = dataProxyClusterMapper.selectAll();
        if (entityList == null || entityList.isEmpty()) {
            LOGGER.info("success to get data proxy ip list, but result is empty, request ip={}", request.getIp());
            return null;
        }

        final String requestNetTag = request.getNetTag();
        List<DataProxyIpResponse> responseList = new ArrayList<>();
        for (DataProxyClusterEntity entity : entityList) {
            // Subject to the net tag of any entity
            String netTag = requestNetTag;
            if (StringUtils.isEmpty(netTag)) {
                int innerIp = entity.getIsInnerIp();
                if (innerIp == 1) {
                    netTag = "auto";
                } else {
                    netTag = entity.getNetType();
                }

                if (StringUtils.isEmpty(netTag)) {
                    netTag = "all";
                }
            }

            DataProxyIpResponse response = new DataProxyIpResponse();
            response.setId(entity.getId());
            response.setPort(entity.getPort());
            response.setIp(entity.getAddress());
            response.setNetTag(netTag);

            responseList.add(response);
        }

        LOGGER.info("success to get data proxy ip list, response size={}", responseList.size());
        return responseList;
    }

    @Override
    public List<DataProxyConfig> getConfig() {
        // get all configs with business status of 130, that is, config successful
        List<DataProxyConfig> configList = businessEntityMapper.selectDataProxyConfig();
        if (configList == null) {
            configList = Collections.emptyList();
        }

        return configList;
    }
}
