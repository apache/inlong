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

package org.apache.inlong.manager.service.core.heartbeat;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.common.enums.ComponentTypeEnum;
import org.apache.inlong.common.heartbeat.HeartbeatMsg;
import org.apache.inlong.manager.common.enums.NodeStatus;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterNodeEntityMapper;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import java.util.List;

@EnableAutoConfiguration
@Slf4j
public class HeartbeatManagerTest extends ServiceBaseTest {

    @Autowired
    HeartbeatManager heartbeatManager;

    @Autowired
    InlongClusterEntityMapper clusterMapper;

    @Autowired
    InlongClusterNodeEntityMapper clusterNodeMapper;

    @Test
    void testReportHeartbeat() throws InterruptedException {
        HeartbeatMsg msg = createHeartbeatMsg();
        heartbeatManager.reportHeartbeat(msg);
        InlongClusterEntity entity = clusterMapper.selectById(1);
        log.info(JsonUtils.toJsonString(entity));
        List<InlongClusterEntity> clusterEntities = clusterMapper.selectByKey(null, msg.getClusterName(),
                msg.getComponentType());
        Assertions.assertTrue(clusterEntities.size() == 1);
        Assertions.assertTrue(clusterEntities.get(0).getName().equals(msg.getClusterName()));
        int clusterId = clusterEntities.get(0).getId();
        ClusterNodeRequest nodeRequest = new ClusterNodeRequest();
        nodeRequest.setParentId(clusterId);
        nodeRequest.setType(msg.getComponentType());
        nodeRequest.setIp(msg.getIp());
        nodeRequest.setPort(msg.getPort());
        InlongClusterNodeEntity clusterNode = clusterNodeMapper.selectByUniqueKey(nodeRequest);
        Assertions.assertTrue(clusterNode != null);
        Assertions.assertTrue(clusterNode.getStatus() == NodeStatus.NORMAL.getStatus());
        heartbeatManager.getHeartbeats().invalidateAll();
        Thread.sleep(1000);
        clusterNode = clusterNodeMapper.selectByUniqueKey(nodeRequest);
        log.debug(JsonUtils.toJsonString(clusterNode));
        Assertions.assertTrue(clusterNode.getStatus() == NodeStatus.HEARTBEAT_TIMEOUT.getStatus());
        heartbeatManager.reportHeartbeat(msg);
        clusterNode = clusterNodeMapper.selectByUniqueKey(nodeRequest);
        Assertions.assertTrue(clusterNode.getStatus() == NodeStatus.NORMAL.getStatus());
    }

    private HeartbeatMsg createHeartbeatMsg() {
        HeartbeatMsg heartbeatMsg = new HeartbeatMsg();
        heartbeatMsg.setIp("127.0.0.1");
        heartbeatMsg.setPort(8008);
        heartbeatMsg.setComponentType(ComponentTypeEnum.Agent.getName());
        heartbeatMsg.setReportTime(System.currentTimeMillis());
        return heartbeatMsg;
    }

}
