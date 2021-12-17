/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.meta;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.meta.ZookeeperWatcher.ChildrenWatcherListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.DataFlowStorageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * DataFlowsChildrenWatcherListener
 */
public class DataFlowsChildrenWatcherListener implements ChildrenWatcherListener {

    private static final Logger LOG = LoggerFactory.getLogger(DataFlowsChildrenWatcherListener.class);

    private final ObjectMapper objectMapper;

    /**
     * Connection to the used ZooKeeper quorum.
     */
    private final CuratorFramework zookeeperClient;

    private final DataFlowInfoListener metaListener;

    public DataFlowsChildrenWatcherListener(CuratorFramework zookeeperClient, DataFlowInfoListener metaListener) {
        this.objectMapper = new ObjectMapper();
        this.zookeeperClient = zookeeperClient;
        this.metaListener = metaListener;
    }

    @Override
    public void onChildAdded(ChildData childData) throws Exception {
        LOG.info("DataFlow Added event retrieved");

        final byte[] data = childData.getData();
        if (data == null) {
            return;
        }

        synchronized (MetaManager.LOCK) {
            DataFlowStorageInfo dataFlowStorageInfo = objectMapper.readValue(data, DataFlowStorageInfo.class);
            DataFlowInfo dataFlowInfo = getDataFlowInfo(dataFlowStorageInfo);
            metaListener.addDataFlow(dataFlowInfo);
        }
    }

    @Override
    public void onChildUpdated(ChildData childData) throws Exception {
        LOG.info("DataFlow Updated event retrieved");

        final byte[] data = childData.getData();
        if (data == null) {
            return;
        }

        synchronized (MetaManager.LOCK) {
            DataFlowStorageInfo dataFlowStorageInfo = objectMapper.readValue(data, DataFlowStorageInfo.class);
            DataFlowInfo dataFlowInfo = getDataFlowInfo(dataFlowStorageInfo);
            metaListener.updateDataFlow(dataFlowInfo);
        }
    }

    @Override
    public void onChildRemoved(ChildData childData) throws Exception {
        LOG.info("DataFlow Removed event retrieved");

        final byte[] data = childData.getData();
        if (data == null) {
            return;
        }

        synchronized (MetaManager.LOCK) {
            DataFlowStorageInfo dataFlowStorageInfo = objectMapper.readValue(data, DataFlowStorageInfo.class);
            DataFlowInfo dataFlowInfo = getDataFlowInfo(dataFlowStorageInfo);
            metaListener.removeDataFlow(dataFlowInfo);
        }
    }

    @Override
    public void onInitialized(List<ChildData> childData) throws Exception {
        LOG.info("Initialized event retrieved");

        for (ChildData singleChildData : childData) {
            onChildAdded(singleChildData);
        }
    }

    private DataFlowInfo getDataFlowInfo(DataFlowStorageInfo dataFlowStorageInfo) throws Exception {
        switch (dataFlowStorageInfo.getStorageType()) {
            case ZK :
                String zkPath = dataFlowStorageInfo.getPath();
                byte[] data = zookeeperClient.getData().forPath(zkPath);
                return objectMapper.readValue(data, DataFlowInfo.class);
            case HDFS :
                throw new IllegalArgumentException("HDFS dataFlow storage type not supported yet!");
            default :
                throw new IllegalArgumentException("Unsupported dataFlow storage type "
                        + dataFlowStorageInfo.getStorageType());
        }
    }
}