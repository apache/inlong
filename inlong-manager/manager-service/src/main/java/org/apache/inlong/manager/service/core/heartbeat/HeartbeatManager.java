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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.common.heartbeat.AbstractHeartbeatManager;
import org.apache.inlong.common.heartbeat.ComponentHeartbeat;
import org.apache.inlong.common.heartbeat.HeartbeatMsg;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterStatus;
import org.apache.inlong.manager.common.enums.NodeStatus;
import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.common.pojo.user.UserRoleCode;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterNodeEntityMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class HeartbeatManager implements AbstractHeartbeatManager {

    @Getter
    private Cache<ComponentHeartbeat, HeartbeatMsg> heartbeats;

    @Getter
    private LoadingCache<ComponentHeartbeat, ClusterInfo> clusterInfos;

    @Autowired
    private InlongClusterEntityMapper clusterMapper;

    @Autowired
    private InlongClusterNodeEntityMapper clusterNodeMapper;

    @PostConstruct
    public void init() {
        long expireTime = heartbeatInterval() * 2;
        heartbeats = Caffeine.newBuilder()
                .expireAfterAccess(expireTime, TimeUnit.SECONDS)
                .removalListener((ComponentHeartbeat k, HeartbeatMsg v, RemovalCause c) -> {
                    evictClusterNode(v);
                }).build();

        clusterInfos = Caffeine.newBuilder()
                .expireAfterAccess(expireTime, TimeUnit.SECONDS)
                .build(k -> fetchCluster(k));
    }

    @Override
    public void reportHeartbeat(HeartbeatMsg heartbeat) {
        ComponentHeartbeat componentHeartbeat = heartbeat.componentHeartbeat();
        ClusterInfo clusterInfo = clusterInfos.get(componentHeartbeat);
        if (clusterInfo == null) {
            log.error("find no cluster by clusterName={} and type={}", componentHeartbeat.getClusterName(),
                    componentHeartbeat.getComponentType());
            return;
        }
        HeartbeatMsg lastHeartbeat = heartbeats.getIfPresent(componentHeartbeat);
        if (lastHeartbeat == null) {
            InlongClusterNodeEntity clusterNode = getClusterNode(clusterInfo, heartbeat);
            if (clusterNode == null) {
                insertClusterNode(clusterInfo, heartbeat);
                log.info("insert node success");
            } else {
                updateClusterNode(clusterNode);
                log.info("update node success");

            }
        }
        heartbeats.put(componentHeartbeat, heartbeat);
    }

    private void evictClusterNode(HeartbeatMsg heartbeat) {
        log.debug("evict cluster node");
        ComponentHeartbeat componentHeartbeat = heartbeat.componentHeartbeat();
        ClusterInfo clusterInfo = clusterInfos.getIfPresent(componentHeartbeat);
        if (clusterInfo == null) {
            log.error("find no cluster by clusterName={} and type={}", componentHeartbeat.getClusterName(),
                    componentHeartbeat.getComponentType());
            return;
        }
        InlongClusterNodeEntity clusterNode = getClusterNode(clusterInfo, heartbeat);
        if (clusterNode == null) {
            log.error("find no cluster node by type={},ip={},port={}",
                    heartbeat.getComponentType(), heartbeat.getIp(), heartbeat.getPort());
            return;
        }
        clusterNode.setStatus(NodeStatus.HEARTBEAT_TIMEOUT.getStatus());
        clusterNodeMapper.updateById(clusterNode);
    }

    private InlongClusterNodeEntity getClusterNode(ClusterInfo clusterInfo, HeartbeatMsg heartbeat) {
        ClusterNodeRequest nodeRequest = new ClusterNodeRequest();
        nodeRequest.setParentId(clusterInfo.getId());
        nodeRequest.setType(heartbeat.getComponentType());
        nodeRequest.setIp(heartbeat.getIp());
        nodeRequest.setPort(heartbeat.getPort());
        InlongClusterNodeEntity clusterNode = clusterNodeMapper.selectByUniqueKey(nodeRequest);
        return clusterNode;
    }

    private void insertClusterNode(ClusterInfo clusterInfo, HeartbeatMsg heartbeat) {
        InlongClusterNodeEntity clusterNode = new InlongClusterNodeEntity();
        clusterNode.setParentId(clusterInfo.getId());
        clusterNode.setType(heartbeat.getComponentType());
        clusterNode.setIp(heartbeat.getIp());
        clusterNode.setPort(heartbeat.getPort());
        clusterNode.setStatus(ClusterStatus.NORMAL.getStatus());
        clusterNode.setCreator(UserRoleCode.ADMIN);
        clusterNode.setModifier(UserRoleCode.ADMIN);
        clusterNode.setIsDeleted(InlongConstants.UN_DELETED);
        clusterNodeMapper.insertOnDuplicateKeyUpdate(clusterNode);
    }

    private void updateClusterNode(InlongClusterNodeEntity clusterNode) {
        clusterNode.setStatus(ClusterStatus.NORMAL.getStatus());
        clusterNodeMapper.updateById(clusterNode);
    }

    private ClusterInfo fetchCluster(ComponentHeartbeat componentHeartbeat) {
        log.debug("fetch cluster");
        final String clusterName = componentHeartbeat.getClusterName();
        final String type = componentHeartbeat.getComponentType();
        List<InlongClusterEntity> entities = clusterMapper.selectByKey(null, clusterName, type);
        if (CollectionUtils.isNotEmpty(entities)) {
            InlongClusterEntity cluster = entities.get(0);
            ClusterInfo clusterInfo = CommonBeanUtils.copyProperties(cluster, ClusterInfo::new);
            return clusterInfo;
        }
        InlongClusterEntity cluster = new InlongClusterEntity();
        cluster.setName(clusterName);
        cluster.setType(type);
        cluster.setInCharges(UserRoleCode.ADMIN);
        cluster.setStatus(ClusterStatus.NORMAL.getStatus());
        cluster.setCreator(UserRoleCode.ADMIN);
        cluster.setModifier(UserRoleCode.ADMIN);
        cluster.setIsDeleted(InlongConstants.UN_DELETED);
        int index = clusterMapper.insertOnDuplicateKeyUpdate(cluster);
        ClusterInfo clusterInfo = CommonBeanUtils.copyProperties(cluster, ClusterInfo::new);
        clusterInfo.setId(index);
        return clusterInfo;
    }
}
