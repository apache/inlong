/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.tubemq.manager.service;

import static org.apache.tubemq.manager.service.TubeMQHttpConst.DELETE_FAIL;

import java.util.Date;
import javax.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.controller.cluster.request.AddClusterReq;
import org.apache.tubemq.manager.entry.ClusterEntry;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.ClusterRepository;
import org.apache.tubemq.manager.service.interfaces.ClusterService;
import org.apache.tubemq.manager.service.interfaces.NodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class ClusterServiceImpl implements ClusterService {

    @Autowired
    ClusterRepository clusterRepository;

    @Autowired
    NodeService nodeService;

    @Override
    public Boolean addClusterAndMasterNode(AddClusterReq req) {
        ClusterEntry entry = new ClusterEntry();
        entry.setCreateTime(new Date());
        entry.setCreateUser(req.getCreateUser());
        entry.setClusterName(req.getClusterName());
        ClusterEntry retEntry = null;
        try {
            retEntry = clusterRepository.saveAndFlush(entry);
        } catch (Exception e) {
            log.error("create cluster fail with exception", e);
            return false;
        }
        // add master node
        return addMasterNode(req, retEntry);
    }

    @Override
    @Transactional(rollbackOn = Exception.class)
    public void deleteCluster(Integer clusterId) {
        Integer successCode = clusterRepository.deleteByClusterId(clusterId);
        if (successCode.equals(DELETE_FAIL) ) {
            throw new RuntimeException("no such cluster with clusterId = " + clusterId);
        }
    }

    private boolean addMasterNode(AddClusterReq req, ClusterEntry clusterEntry) {
        if (clusterEntry == null) {
            return false;
        }
        NodeEntry nodeEntry = new NodeEntry();
        nodeEntry.setPort(req.getMasterPort());
        nodeEntry.setMaster(true);
        nodeEntry.setClusterId(clusterEntry.getClusterId());
        nodeEntry.setWebPort(req.getMasterWebPort());
        nodeEntry.setIp(req.getMasterIp());
        nodeEntry.setBroker(false);
        return nodeService.addNode(nodeEntry);
    }

}
