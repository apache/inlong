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
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GlobalConstants;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.cluster.*;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterNodeEntityMapper;
import org.apache.inlong.manager.service.core.InlongClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.util.Date;
import java.util.List;

/**
 * Inlong cluster service layer implementation
 */
@Service
public class InlongClusterServiceImpl implements InlongClusterService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongClusterServiceImpl.class);

    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongClusterNodeEntityMapper clusterNodeMapper;

    @Override
    public Integer save(InlongClusterRequest request, String operator) {
        LOGGER.debug("begin to save inlong cluster info={}", request);

        // Check request and name
        Preconditions.checkNotNull(request, "inlong cluster info is empty");
        Preconditions.checkNotEmpty(request.getName(), "inlong cluster name is empty");

        //check if cluster already exist
        InlongClusterEntity exist = clusterMapper.selectByUniqueKey(request);
        Preconditions.checkTrue(exist == null, "cluster already exist");
        InlongClusterEntity entity = CommonBeanUtils.copyProperties(request, InlongClusterEntity::new);
        if (operator != null) {
            entity.setCreator(operator);
        }

        Preconditions.checkNotNull(entity.getCreator(), "inlong cluster creator is empty");
        entity.setCreateTime(new Date());
        entity.setIsDeleted(GlobalConstants.UN_DELETED);
        clusterMapper.insert(entity);
        LOGGER.info("success to add a inlong cluster");
        return entity.getId();
    }

    @Override
    public InlongClusterResponse get(Integer id) {
        LOGGER.info("begin to get inlong cluster by id={}", id);

        Preconditions.checkNotNull(id, "inlong cluster id is empty");
        InlongClusterEntity entity = clusterMapper.selectByIdNoDeleted(id);
        if (entity == null) {
            LOGGER.error("inlong cluster not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        InlongClusterResponse inlongClusterResponse = CommonBeanUtils.copyProperties(entity, InlongClusterResponse::new);
        LOGGER.info("success to get inlong cluster info");
        return inlongClusterResponse;
    }

    @Override
    public PageInfo<InlongClusterResponse> list(InlongClusterPageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongClusterEntity> entityPage = (Page<InlongClusterEntity>)
                clusterMapper.selectByCondition(request);
        List<InlongClusterResponse> clusterList = CommonBeanUtils.copyListProperties(entityPage,
                InlongClusterResponse::new);
        PageInfo<InlongClusterResponse> page = new PageInfo<>(clusterList);
        page.setTotal(entityPage.getTotal());

        LOGGER.debug("success to list inlong cluster by {}", request);
        return page;
    }

    @Override
    public Boolean update(InlongClusterRequest request, String operator) {
        Preconditions.checkNotNull(request, "inlong cluster is empty");
        Preconditions.checkNotEmpty(request.getName(), "inlong cluster name is empty");
        InlongClusterEntity exist = clusterMapper.selectByUniqueKey(request);
        Preconditions.checkTrue(exist == null, "cluster already exist");
        Integer id = request.getId();
        Preconditions.checkNotNull(id, "inlong cluster id is empty");
        InlongClusterEntity entity = clusterMapper.selectByIdNoDeleted(id);
        if (entity == null) {
            LOGGER.error("inlong cluster not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        CommonBeanUtils.copyProperties(request, entity, true);
        entity.setModifier(operator);
        clusterMapper.updateById(entity);

        LOGGER.info("success to update inlong cluster={}", request);
        return true;
    }

    @Override
    public Boolean delete(Integer id, String operator) {
        Preconditions.checkNotNull(id, "cluster id is empty");
        InlongClusterEntity entity = clusterMapper.selectByIdNoDeleted(id);
        if (entity == null) {
            LOGGER.error("inlong cluster not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        entity.setIsDeleted(GlobalConstants.DELETED_STATUS);
        entity.setModifier(operator);
        clusterMapper.updateById(entity);
        LOGGER.info("success to delete inlong cluster by id={}", id);
        return true;
    }

    @Override
    public Integer saveNode(ClusterNodeRequest request, String operator) {
        LOGGER.info("begin to insert a inlong cluster node info cluster={}", request);
        Preconditions.checkNotNull(request, "cluster is empty");
        Preconditions.checkNotNull(request.getClusterId(), "inlong cluster node cluster_id is empty");
        InlongClusterNodeEntity exist = clusterNodeMapper.selectByUniqueKey(request);
        Preconditions.checkTrue(exist == null, "inlong cluster node name already exist");

        InlongClusterNodeEntity entity = CommonBeanUtils.copyProperties(request, InlongClusterNodeEntity::new);
        if (operator != null) {
            entity.setCreator(operator);
        }
        Preconditions.checkNotNull(entity.getCreator(), "inlong cluster node creator is empty");
        entity.setParentId(request.getClusterId());
        entity.setCreateTime(new Date());
        entity.setIsDeleted(GlobalConstants.UN_DELETED);
        clusterNodeMapper.insert(entity);
        LOGGER.info("success to add a inlong cluster node");
        return entity.getId();
    }

    @Override
    public ClusterNodeResponse getNode(Integer id) {
        LOGGER.info("begin to get inlong cluster node by id={}", id);
        Preconditions.checkNotNull(id, "cluster node id is empty");
        InlongClusterNodeEntity entity = clusterNodeMapper.selectByIdNoDeleted(id);
        if (entity == null) {
            LOGGER.error("inlong cluster node not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        ClusterNodeResponse clusterNodeResponse = CommonBeanUtils.copyProperties(entity, ClusterNodeResponse::new);
        LOGGER.info("success to get inlong cluster node info");
        return clusterNodeResponse;
    }

    @Override
    public PageInfo<ClusterNodeResponse> listNode(InlongClusterPageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongClusterNodeEntity> entityPage = (Page<InlongClusterNodeEntity>)
                clusterNodeMapper.selectByCondition(request);
        List<ClusterNodeResponse> clusterList = CommonBeanUtils.copyListProperties(entityPage,
                ClusterNodeResponse::new);
        PageInfo<ClusterNodeResponse> page = new PageInfo<>(clusterList);
        page.setTotal(entityPage.getTotal());

        LOGGER.debug("success to list inlong cluster node by {}", request);
        return page;
    }

    @Override
    public Boolean updateNode(ClusterNodeRequest request, String operator) {
        Preconditions.checkNotNull(request, "inlong cluster node is empty");
        Preconditions.checkNotNull(request.getClusterId(), "inlong cluster node cluster_id is empty");
        Integer id = request.getId();
        InlongClusterNodeEntity exist = clusterNodeMapper.selectByUniqueKey(request);
        Preconditions.checkTrue(exist == null, "inlong cluster node name already exist");
        Preconditions.checkNotNull(id, "inlong cluster node id is empty");
        InlongClusterNodeEntity entity = clusterNodeMapper.selectByIdNoDeleted(id);
        if (entity == null) {
            LOGGER.error("cluster not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        CommonBeanUtils.copyProperties(request, entity, true);
        entity.setParentId(request.getClusterId());
        entity.setModifier(operator);
        clusterNodeMapper.updateById(entity);

        LOGGER.info("success to update inlong cluster node ={}", request);
        return true;
    }

    @Override
    public Boolean deleteNode(Integer id, String operator) {
        Preconditions.checkNotNull(id, "inlong cluster node id is empty");
        InlongClusterNodeEntity entity = clusterNodeMapper.selectByIdNoDeleted(id);
        if (entity == null) {
            LOGGER.error("inlong cluster node not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        entity.setIsDeleted(GlobalConstants.DELETED_STATUS);
        entity.setModifier(operator);
        clusterNodeMapper.updateById(entity);
        LOGGER.info("success to delete inlong cluster node by id={}", id);
        return true;
    }
}
