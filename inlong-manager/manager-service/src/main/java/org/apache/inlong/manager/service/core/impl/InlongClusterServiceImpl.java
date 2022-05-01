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
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterPageRequest;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterRequest;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterResponse;
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
import java.util.Objects;

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
        LOGGER.debug("begin to save inlong cluster={}", request);
        // check request
        Preconditions.checkNotNull(request, "inlong cluster info is empty");
        Preconditions.checkNotEmpty(request.getName(), "inlong cluster name is empty");
        Preconditions.checkNotEmpty(request.getClusterTag(), "inlong cluster tag is empty");
        Preconditions.checkNotEmpty(request.getType(), "inlong cluster type is empty");

        // check if cluster already exist
        InlongClusterEntity exist = clusterMapper.selectByUniqueKey(request);
        if (exist != null) {
            String errMsg = String.format("inlong cluster already exist for name=%s cluster tag=%s type=%s)",
                    request.getName(), request.getClusterTag(), request.getType());
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }
        InlongClusterEntity entity = CommonBeanUtils.copyProperties(request, InlongClusterEntity::new);
        entity.setCreator(operator);
        entity.setCreateTime(new Date());
        entity.setIsDeleted(GlobalConstants.UN_DELETED);
        clusterMapper.insert(entity);

        LOGGER.info("success to save inlong cluster={}", request);
        return entity.getId();
    }

    @Override
    public InlongClusterResponse get(Integer id) {
        Preconditions.checkNotNull(id, "inlong cluster id is empty");
        InlongClusterEntity entity = clusterMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("inlong cluster not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        InlongClusterResponse response = CommonBeanUtils.copyProperties(entity, InlongClusterResponse::new);
        LOGGER.debug("success to get inlong cluster info by id={}", id);
        return response;
    }

    @Override
    public PageInfo<InlongClusterResponse> list(InlongClusterPageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongClusterEntity> entityPage = (Page<InlongClusterEntity>)
                clusterMapper.selectByCondition(request);
        List<InlongClusterResponse> clusterList = CommonBeanUtils.copyListProperties(entityPage,
                InlongClusterResponse::new);
        PageInfo<InlongClusterResponse> page = new PageInfo<>(clusterList);
        page.setTotal(clusterList.size());
        LOGGER.debug("success to list inlong cluster by {}", request);
        return page;
    }

    @Override
    public Boolean update(InlongClusterRequest request, String operator) {
        LOGGER.debug("begin to update inlong cluster={}", request);

        Preconditions.checkNotNull(request, "inlong cluster info is empty");
        Preconditions.checkNotEmpty(request.getName(), "inlong cluster name is empty");
        Preconditions.checkNotEmpty(request.getClusterTag(), "inlong cluster tag is empty");
        Preconditions.checkNotEmpty(request.getType(), "inlong cluster type is empty");

        Integer id = request.getId();
        Preconditions.checkNotNull(id, "inlong cluster id is empty");
        InlongClusterEntity exist = clusterMapper.selectByUniqueKey(request);
        if (exist != null && !Objects.equals(id, exist.getId())) {
            String errMsg = String.format("inlong cluster already exist for name=%s cluster tag=%s type=%s",
                    request.getName(), request.getClusterTag(), request.getType());
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongClusterEntity entity = clusterMapper.selectById(id);
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
        InlongClusterEntity entity = clusterMapper.selectById(id);
        if (entity == null || entity.getIsDeleted() > GlobalConstants.UN_DELETED) {
            LOGGER.error("inlong cluster not found by id={}, or was already deleted", id);
            return false;
        }

        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        clusterMapper.updateById(entity);
        LOGGER.info("success to delete inlong cluster by id={}", id);
        return true;
    }

    @Override
    public Integer saveNode(ClusterNodeRequest request, String operator) {
        LOGGER.debug("begin to insert inlong cluster node={}", request);
        Preconditions.checkNotNull(request, "cluster node info is empty");
        Preconditions.checkNotNull(request.getParentId(), "inlong cluster node parent_id is empty");
        Preconditions.checkNotNull(request.getType(), "inlong cluster node type is empty");
        Preconditions.checkNotNull(request.getIp(), "inlong cluster node ip is empty");
        Preconditions.checkNotNull(request.getPort(), "inlong cluster node port is empty");

        // check cluster node if exist
        InlongClusterNodeEntity exist = clusterNodeMapper.selectByUniqueKey(request);
        if (exist != null) {
            String errMsg = String.format("inlong cluster node already exist for type=%s ip=%s port=%s",
                    request.getType(), request.getIp(), request.getPort());
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongClusterNodeEntity entity = CommonBeanUtils.copyProperties(request, InlongClusterNodeEntity::new);
        entity.setCreator(operator);
        entity.setCreateTime(new Date());
        entity.setIsDeleted(GlobalConstants.UN_DELETED);
        clusterNodeMapper.insert(entity);

        LOGGER.info("success to add inlong cluster node={}", request);
        return entity.getId();
    }

    @Override
    public ClusterNodeResponse getNode(Integer id) {
        Preconditions.checkNotNull(id, "cluster node id is empty");
        InlongClusterNodeEntity entity = clusterNodeMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("inlong cluster node not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        ClusterNodeResponse clusterNodeResponse = CommonBeanUtils.copyProperties(entity, ClusterNodeResponse::new);
        LOGGER.debug("success to get inlong cluster node by id={}", id);
        return clusterNodeResponse;
    }

    @Override
    public PageInfo<ClusterNodeResponse> listNode(InlongClusterPageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongClusterNodeEntity> entityPage = (Page<InlongClusterNodeEntity>)
                clusterNodeMapper.selectByCondition(request);
        List<ClusterNodeResponse> nodeList = CommonBeanUtils.copyListProperties(entityPage, ClusterNodeResponse::new);
        PageInfo<ClusterNodeResponse> page = new PageInfo<>(nodeList);
        page.setTotal(nodeList.size());

        LOGGER.debug("success to list inlong cluster node by {}", request);
        return page;
    }

    @Override
    public Boolean updateNode(ClusterNodeRequest request, String operator) {
        LOGGER.debug("begin to update inlong cluster node={}", request);

        Preconditions.checkNotNull(request, "inlong cluster node info is empty");
        Preconditions.checkNotNull(request, "cluster is empty");
        Preconditions.checkNotNull(request.getParentId(), "inlong cluster node parent_id is empty");
        Preconditions.checkNotNull(request.getType(), "inlong cluster node type is empty");
        Preconditions.checkNotNull(request.getIp(), "inlong cluster node ip is empty");
        Preconditions.checkNotNull(request.getPort(), "inlong cluster node port is empty");

        Integer id = request.getId();
        Preconditions.checkNotNull(id, "inlong cluster node id is empty");
        // check cluster node if exist
        InlongClusterNodeEntity exist = clusterNodeMapper.selectByUniqueKey(request);
        if (exist != null && !Objects.equals(id, exist.getId())) {
            String errMsg = String.format("inlong cluster node already exist for type=%s ip=%s port=%s)",
                    request.getType(), request.getIp(), request.getPort());
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongClusterNodeEntity entity = clusterNodeMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("cluster node not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        CommonBeanUtils.copyProperties(request, entity, true);
        entity.setParentId(request.getParentId());
        entity.setModifier(operator);
        clusterNodeMapper.updateById(entity);

        LOGGER.info("success to update inlong cluster node={}", request);
        return true;
    }

    @Override
    public Boolean deleteNode(Integer id, String operator) {
        Preconditions.checkNotNull(id, "inlong cluster node id is empty");
        InlongClusterNodeEntity entity = clusterNodeMapper.selectById(id);
        if (entity == null || entity.getIsDeleted() > GlobalConstants.UN_DELETED) {
            LOGGER.error("inlong cluster node not found by id={}", id);
            return false;
        }
        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        clusterNodeMapper.updateById(entity);
        LOGGER.info("success to delete inlong cluster node by id={}", id);
        return true;
    }

}
