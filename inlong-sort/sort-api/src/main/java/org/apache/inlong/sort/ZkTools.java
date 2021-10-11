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

package org.apache.inlong.sort;

import static org.apache.inlong.sort.protocol.DataFlowStorageInfo.StorageType.ZK;

import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.DataFlowStorageInfo;
import org.apache.inlong.sort.util.ZooKeeperUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tools of ZK is provided as a SDK tool for TDM.
 *
 * <p> TODO, reorganize the maven structure to provide this as a separate API module. </p>
 */
public class ZkTools {

    private static final Logger LOG = LoggerFactory.getLogger(ZkTools.class);

    public static void addDataFlowToCluster(
            String cluster,
            long dataFlowId,
            String zkQuorum,
            String zkRoot) throws Exception {
        final Configuration config = new Configuration();
        config.setString(Constants.ZOOKEEPER_QUORUM, zkQuorum);
        config.setString(Constants.ZOOKEEPER_ROOT, zkRoot);

        final String watchingPath = getNodePathOfDataFlowStorageInfoInCluster(cluster, dataFlowId);

        try (CuratorFramework zkClient = ZooKeeperUtils.startCuratorFramework(config)) {
            if (zkClient.checkExists().forPath(watchingPath) == null) {
                ZooKeeperUtils.createRecursive(zkClient, watchingPath, null, CreateMode.PERSISTENT);
            }
        }

        LOG.info("Add dataFlow with id {} to cluster {} successfully", dataFlowId, cluster);
    }

    public static void removeDataFlowFromCluster(
            String cluster,
            long dataFlowId,
            String zkQuorum,
            String zkRoot) throws Exception {
        final Configuration config = new Configuration();
        config.setString(Constants.ZOOKEEPER_QUORUM, zkQuorum);
        config.setString(Constants.ZOOKEEPER_ROOT, zkRoot);

        final String watchingPath = getNodePathOfDataFlowStorageInfoInCluster(cluster, dataFlowId);

        try (CuratorFramework zkClient = ZooKeeperUtils.startCuratorFramework(config)) {
            if (zkClient.checkExists().forPath(watchingPath) != null) {
                zkClient.delete().forPath(watchingPath);
            }
        }

        LOG.info("Remove dataFlow with id {} to cluster {} successfully", dataFlowId, cluster);
    }

    /**
     * Update DataFlowInfo.
     * @param dataFlowInfo DataFlowInfo to be updated to.
     * @param cluster cluster name of the etl job.
     * @param dataFlowId dataFlow id
     * @param zkQuorum zk quorum
     * @param zkRoot zk root path
     * @throws Exception
     */
    public static void updateDataFlowInfo(
            DataFlowInfo dataFlowInfo,
            String cluster,
            long dataFlowId,
            String zkQuorum,
            String zkRoot) throws Exception {
        final Configuration config = new Configuration();
        config.setString(Constants.ZOOKEEPER_QUORUM, zkQuorum);
        config.setString(Constants.ZOOKEEPER_ROOT, zkRoot);

        final ObjectMapper objectMapper = new ObjectMapper();
        final String dataFlowInfoPath = getNodePathOfDataFlowInfo(dataFlowId);
        final String dataFlowStorageInfoPath = getNodePathOfDataFlowStorageInfoInCluster(cluster, dataFlowId);

        try (CuratorFramework zkClient = ZooKeeperUtils.startCuratorFramework(config)) {
            // update DataFlowInfo
            final byte[] dataFlowInfoData = objectMapper.writeValueAsBytes(dataFlowInfo);
            if (zkClient.checkExists().forPath(dataFlowInfoPath) == null) {
                ZooKeeperUtils.createRecursive(zkClient, dataFlowInfoPath, dataFlowInfoData, CreateMode.PERSISTENT);
            } else {
                zkClient.setData().forPath(dataFlowInfoPath, dataFlowInfoData);
            }

            // update DataFlowStorageInfo
            DataFlowStorageInfo dataFlowStorageInfo = new DataFlowStorageInfo(ZK, dataFlowInfoPath);
            final byte[] dataFlowStorageInfoData = objectMapper.writeValueAsBytes(dataFlowStorageInfo);
            if (zkClient.checkExists().forPath(dataFlowStorageInfoPath) == null) {
                ZooKeeperUtils.createRecursive(
                        zkClient, dataFlowStorageInfoPath, dataFlowStorageInfoData, CreateMode.PERSISTENT);
            } else {
                zkClient.setData().forPath(dataFlowStorageInfoPath, dataFlowStorageInfoData);
            }
        }

        LOG.info("Update DataFlowInfo with id {} on zk successfully", dataFlowId);
    }

    /**
     * Get node path which stores the meta data of DataFlowStorageInfo.
     * If you want to change the path rule here, please change
     * MetaManager#getWatchingPathOfDataFlowsInCluster in core too.
     */
    public static String getNodePathOfDataFlowStorageInfoInCluster(String cluster, long dataFlowId) {
        return "/clusters/" + cluster + "/dataflows/" + dataFlowId;
    }

    /**
     * Get node path which stores the meta data of DataFlowInfo.
     */
    public static String getNodePathOfDataFlowInfo(long dataFlowId) {
        return "/dataflows/" + dataFlowId;
    }
}
