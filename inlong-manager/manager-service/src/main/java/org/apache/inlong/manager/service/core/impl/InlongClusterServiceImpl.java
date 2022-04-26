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

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterPageRequest;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterRequest;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterResponse;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterNodeEntityMapper;
import org.apache.inlong.manager.service.core.InlongClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
        return null;
    }

    @Override
    public InlongClusterResponse get(Integer id) {
        return null;
    }

    @Override
    public PageInfo<InlongClusterResponse> list(InlongClusterPageRequest request) {
        return null;
    }

    @Override
    public Boolean update(InlongClusterRequest request, String operator) {
        return null;
    }

    @Override
    public Boolean delete(Integer id, String operator) {
        return null;
    }

    @Override
    public Integer saveNode(InlongClusterRequest request, String operator) {
        return null;
    }

    @Override
    public ClusterNodeResponse getNode(Integer id) {
        return null;
    }

    @Override
    public PageInfo<ClusterNodeResponse> listNode(InlongClusterPageRequest request) {
        return null;
    }

    @Override
    public Boolean updateNode(ClusterNodeRequest request, String operator) {
        return null;
    }

    @Override
    public Boolean deleteNode(Integer id, String operator) {
        return null;
    }
}
