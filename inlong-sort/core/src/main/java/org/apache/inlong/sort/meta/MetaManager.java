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

package org.apache.inlong.sort.meta;

import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.DataFlowStorageInfo;
import org.apache.inlong.sort.util.ZooKeeperUtils;
import org.apache.inlong.sort.util.ZookeeperWatcher;
import org.apache.inlong.sort.util.ZookeeperWatcher.ChildrenWatcherListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the manager of meta. It's a singleton and shared by many components from different
 * threads. So it's thread-safe.
 *
 */
public class MetaManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetaManager.class);

    private static MetaManager instance;

    private static final Object lock = new Object();

    private final CuratorFramework zookeeperClient;

    private final ZookeeperWatcher zookeeperWatcher;

    private final ObjectMapper objectMapper;

    @GuardedBy("lock")
    private final Map<Long, DataFlowInfo> dataFlowInfoMap;

    @GuardedBy("lock")
    private final List<DataFlowInfoListener>  dataFlowInfoListeners;

    public static MetaManager getInstance(Configuration config) throws Exception {
        synchronized (lock) {
            if (instance == null) {
                instance = new MetaManager(config);
                instance.open(config);
            }
            return instance;
        }
    }

    public static void release() throws Exception {
        synchronized (lock) {
            if (instance != null) {
                instance.close();
                instance = null;
            }
        }
    }

    /**
     * If you want to change the path rule here, please change
     * ZkTools#getNodePathOfDataFlowStorageInfoInCluster in api too.
     */
    public static String getWatchingPathOfDataFlowsInCluster(String cluster) {
        return "/clusters/" + cluster + "/dataflows";
    }

    private MetaManager(Configuration config) {
        zookeeperClient = ZooKeeperUtils.startCuratorFramework(config);
        zookeeperWatcher = new ZookeeperWatcher(zookeeperClient);
        objectMapper = new ObjectMapper();
        dataFlowInfoMap = new HashMap<>();
        dataFlowInfoListeners = new ArrayList<>();
    }

    private void open(Configuration config) throws Exception {
        synchronized (lock) {
            final String dataFlowsWatchingPath
                    = getWatchingPathOfDataFlowsInCluster(config.getString(Constants.CLUSTER_ID));
            final DataFlowsChildrenWatcherListener dataFlowsChildrenWatcherListener
                    = new DataFlowsChildrenWatcherListener();
            zookeeperWatcher.registerPathChildrenWatcher(
                    dataFlowsWatchingPath, dataFlowsChildrenWatcherListener, true);
            final List<ChildData> childData = zookeeperWatcher.getCurrentPathChildrenDatum(dataFlowsWatchingPath);
            if (childData != null) {
                dataFlowsChildrenWatcherListener.onInitialized(childData);
            }
        }
    }

    public void close() throws Exception {
        synchronized (lock) {
            zookeeperWatcher.close();
            zookeeperClient.close();
        }
    }

    public void registerDataFlowInfoListener(DataFlowInfoListener dataFlowInfoListener) throws Exception {
        synchronized (lock) {
            dataFlowInfoListeners.add(dataFlowInfoListener);
            for (DataFlowInfo dataFlowInfo : dataFlowInfoMap.values()) {
                try {
                    dataFlowInfoListener.addDataFlow(dataFlowInfo);
                } catch (Exception e) {
                    LOG.warn("Error happens when notifying listener data flow updated", e);
                }
            }
        }

        LOG.info("Register DataFlowInfoListener successfully");
    }

    public DataFlowInfo getDataFlowInfo(long id) {
        synchronized (lock) {
            return dataFlowInfoMap.get(id);
        }
    }

    public interface DataFlowInfoListener {
        void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception;

        void updateDataFlow(DataFlowInfo dataFlowInfo) throws Exception;

        void removeDataFlow(DataFlowInfo dataFlowInfo) throws Exception;
    }

    private class DataFlowsChildrenWatcherListener implements ChildrenWatcherListener {

        @Override
        public void onChildAdded(ChildData childData) throws Exception {
            LOG.info("DataFlow Added event retrieved");

            final byte[] data = childData.getData();
            if (data == null) {
                return;
            }

            synchronized (lock) {
                DataFlowStorageInfo dataFlowStorageInfo = objectMapper.readValue(data, DataFlowStorageInfo.class);
                DataFlowInfo dataFlowInfo = getDataFlowInfo(dataFlowStorageInfo);
                long dataFlowId = dataFlowInfo.getId();

                DataFlowInfo oldDataFlowInfo = dataFlowInfoMap.put(dataFlowId, dataFlowInfo);
                if (oldDataFlowInfo == null) {
                    LOG.info("Try to add dataFlow {}", dataFlowId);
                    for (DataFlowInfoListener dataFlowInfoListener : dataFlowInfoListeners) {
                        try {
                            dataFlowInfoListener.addDataFlow(dataFlowInfo);
                        } catch (Exception e) {
                            LOG.warn("Error happens when notifying listener data flow added", e);
                        }
                    }
                } else {
                    LOG.warn("DataFlow {} should not be exist", dataFlowId);
                    for (DataFlowInfoListener dataFlowInfoListener : dataFlowInfoListeners) {
                        try {
                            dataFlowInfoListener.updateDataFlow(dataFlowInfo);
                        } catch (Exception e) {
                            LOG.warn("Error happens when notifying listener data flow updated", e);
                        }
                    }
                }
            }
        }

        @Override
        public void onChildUpdated(ChildData childData) throws Exception {
            LOG.info("DataFlow Updated event retrieved");

            final byte[] data = childData.getData();
            if (data == null) {
                return;
            }

            synchronized (lock) {
                DataFlowStorageInfo dataFlowStorageInfo = objectMapper.readValue(data, DataFlowStorageInfo.class);
                DataFlowInfo dataFlowInfo = getDataFlowInfo(dataFlowStorageInfo);
                long dataFlowId = dataFlowInfo.getId();

                DataFlowInfo oldDataFlowInfo = dataFlowInfoMap.put(dataFlowId, dataFlowInfo);
                if (oldDataFlowInfo == null) {
                    LOG.warn("DataFlow {} should already be exist.", dataFlowId);
                    for (DataFlowInfoListener dataFlowInfoListener : dataFlowInfoListeners) {
                        try {
                            dataFlowInfoListener.addDataFlow(dataFlowInfo);
                        } catch (Exception e) {
                            LOG.warn("Error happens when notifying listener data flow updated", e);
                        }
                    }
                } else {
                    if (dataFlowInfo.equals(oldDataFlowInfo)) {
                        LOG.info("DataFlowInfo has not been changed, ignore update.");
                        return;
                    }

                    LOG.info("Try to update dataFlow {}.", dataFlowId);

                    for (DataFlowInfoListener dataFlowInfoListener : dataFlowInfoListeners) {
                        try {
                            dataFlowInfoListener.updateDataFlow(dataFlowInfo);
                        } catch (Exception e) {
                            LOG.warn("Error happens when notifying listener data flow updated", e);
                        }
                    }
                }
            }
        }

        @Override
        public void onChildRemoved(ChildData childData) throws Exception {
            LOG.info("DataFlow Removed event retrieved");

            final byte[] data = childData.getData();
            if (data == null) {
                return;
            }

            synchronized (lock) {
                DataFlowStorageInfo dataFlowStorageInfo = objectMapper.readValue(data, DataFlowStorageInfo.class);
                DataFlowInfo dataFlowInfo = getDataFlowInfo(dataFlowStorageInfo);
                long dataFlowId = dataFlowInfo.getId();

                dataFlowInfoMap.remove(dataFlowId);
                for (DataFlowInfoListener dataFlowInfoListener : dataFlowInfoListeners) {
                    try {
                        dataFlowInfoListener.removeDataFlow(dataFlowInfo);
                    } catch (Exception e) {
                        LOG.warn("Error happens when notifying listener data flow deleted", e);
                    }
                }
            }
        }

        @Override
        public void onInitialized(List<ChildData> childData) throws Exception {
            LOG.info("Initialized event retrieved");

            for (ChildData singleChildData : childData) {
                onChildAdded(singleChildData);
            }
        }
    }

    private DataFlowInfo getDataFlowInfo(DataFlowStorageInfo dataFlowStorageInfo) throws Exception {
        switch (dataFlowStorageInfo.getStorageType()) {
            case ZK:
                String zkPath = dataFlowStorageInfo.getPath();
                byte[] data = zookeeperClient.getData().forPath(zkPath);
                return objectMapper.readValue(data, DataFlowInfo.class);
            case HDFS:
                throw new IllegalArgumentException("HDFS dataFlow storage type not supported yet!");
            default:
                throw new IllegalArgumentException("Unsupported dataFlow storage type "
                        + dataFlowStorageInfo.getStorageType());
        }
    }

}
