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

package org.apache.inlong.sort.meta.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.meta.MetaManager;
import org.apache.inlong.sort.meta.MetaWatcher;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.util.ZooKeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ZookeeperMetaWatcher implements AutoCloseable, UnhandledErrorListener, MetaWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperMetaWatcher.class);

    private final Object lock = new Object();

    /**
     * Connection to the used ZooKeeper quorum.
     */
    private CuratorFramework client;

    /**
     * Curator recipe to watch changes of a specific ZooKeeper node.
     */
    @GuardedBy("lock")
    private Map<String, NodeCache> caches;

    @GuardedBy("lock")
    private Map<String, PathChildrenCache> pathChildrenCaches;

    private ConnectionStateListener connectionStateListener = (client, newState) -> handleStateChange(newState);

    /**
     * open
     * 
     * @param  config
     * @param  metaListener
     * @throws Exception
     */
    public void open(Configuration config, DataFlowInfoListener metaListener) throws Exception {
        this.client = ZooKeeperUtils.startCuratorFramework(config);
        this.caches = new HashMap<>();
        this.pathChildrenCaches = new HashMap<>();
        client.getUnhandledErrorListenable().addListener(this);
        client.getConnectionStateListenable().addListener(connectionStateListener);
        synchronized (MetaManager.LOCK) {
            final String dataFlowsWatchingPath = getWatchingPathOfDataFlowsInCluster(
                    config.getString(Constants.CLUSTER_ID));
            DataFlowsChildrenWatcherListener dataFlowsChildrenWatcherListener = new DataFlowsChildrenWatcherListener(
                    this.client, metaListener);
            this.registerPathChildrenWatcher(
                    dataFlowsWatchingPath, dataFlowsChildrenWatcherListener, true);
            final List<ChildData> childData = this.getCurrentPathChildrenDatum(dataFlowsWatchingPath);
            if (childData != null) {
                dataFlowsChildrenWatcherListener.onInitialized(childData);
            }
        }
    }

    /**
     * If you want to change the path rule here, please change ZkTools#getNodePathOfDataFlowStorageInfoInCluster in api
     * too.
     */
    public static String getWatchingPathOfDataFlowsInCluster(String cluster) {
        return "/clusters/" + cluster + "/dataflows";
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing ZookeeperWatcher.");

        synchronized (lock) {
            client.getConnectionStateListenable().removeListener(connectionStateListener);

            try {
                for (NodeCache cache : caches.values()) {
                    cache.close();
                }
                caches.clear();

                for (PathChildrenCache pathChildrenCache : pathChildrenCaches.values()) {
                    pathChildrenCache.close();
                }
                pathChildrenCaches.clear();
            } catch (IOException e) {
                throw new Exception("Could not properly stop the ZooKeeperLeaderRetrievalService.", e);
            }
        }
        this.client.close();
    }

    /**
     * Register a watcher on path.
     *
     * @param  watchingPath the watching path
     * @param  listener     the callback listener
     * @param  initial      if initial is true, it retrieves the data of the watching path synchronously, then invokes
     *                      the listener immediately. Otherwise it returns immediately and invoke the listener
     *                      asynchronously if the node is changed
     * @throws Exception    exception
     */
    public void registerWatcher(
            String watchingPath, WatcherListener listener, boolean initial) throws Exception {

        final NodeCacheListenerAdapter listenerAdapter;
        synchronized (lock) {
            Preconditions.checkState(
                    !caches.containsKey(watchingPath),
                    "Watching path " + watchingPath + " is already registered");

            LOG.info("Start watching path /{}{}", client.getNamespace(), watchingPath);
            final NodeCache nodeCache = new NodeCache(client, watchingPath);
            caches.put(watchingPath, nodeCache);
            listenerAdapter = new NodeCacheListenerAdapter(nodeCache, watchingPath, listener);
            nodeCache.getListenable().addListener(listenerAdapter);
            nodeCache.start(initial);
        }
    }

    public void unregisterWatcher(String watchingPath) throws IOException {
        synchronized (lock) {
            final NodeCache nodeCache = caches.remove(watchingPath);
            if (nodeCache != null) {
                LOG.info("Stop watching path /{}{}", client.getNamespace(), watchingPath);
                nodeCache.close();
            }
        }
    }

    /**
     * Register pathChildren watcher.
     * 
     * @param buildInitial whether build the initial cache or not.
     */
    public void registerPathChildrenWatcher(
            String watchingPath,
            ChildrenWatcherListener childrenWatcherListener,
            boolean buildInitial) throws Exception {

        final PathChildrenCacheListenerAdapter listenerAdapter;
        synchronized (lock) {
            Preconditions.checkState(
                    !pathChildrenCaches.containsKey(watchingPath),
                    "Watching children path " + watchingPath + " is already registered");

            LOG.info("Start watching path /{}{}", client.getNamespace(), watchingPath);
            final PathChildrenCache pathChildrenCache = new PathChildrenCache(client, watchingPath, true);
            listenerAdapter = new PathChildrenCacheListenerAdapter(watchingPath, childrenWatcherListener);
            pathChildrenCaches.put(watchingPath, pathChildrenCache);

            pathChildrenCache.getListenable().addListener(listenerAdapter);

            if (buildInitial) {
                pathChildrenCache.start(StartMode.BUILD_INITIAL_CACHE);
            } else {
                pathChildrenCache.start(StartMode.NORMAL);
            }
        }
    }

    public void unregisterPathChildrenWatcher(String watchingPath) throws IOException {
        synchronized (lock) {
            final PathChildrenCache pathChildrenCache = pathChildrenCaches.get(watchingPath);
            if (pathChildrenCache != null) {
                LOG.info("Stop watching path /{}{}", client.getNamespace(), watchingPath);
                pathChildrenCache.close();
            }
        }
    }

    @Nullable
    public List<ChildData> getCurrentPathChildrenDatum(String path) {
        synchronized (lock) {
            final PathChildrenCache pathChildrenCache = pathChildrenCaches.get(path);
            if (pathChildrenCache == null) {
                return null;
            }
            return pathChildrenCache.getCurrentData();
        }
    }

    @Nullable
    public ChildData getCurrentNodeData(String path) {
        synchronized (lock) {
            final NodeCache nodeCache = caches.get(path);
            if (nodeCache == null) {
                return null;
            }
            return nodeCache.getCurrentData();
        }
    }

    public boolean watcherRegistered(String watchingPath) {
        synchronized (lock) {
            return caches.containsKey(watchingPath)
                    || pathChildrenCaches.containsKey(watchingPath);
        }
    }

    protected void handleStateChange(ConnectionState newState) {
        switch (newState) {
            case CONNECTED :
                LOG.info("Connected to ZooKeeper quorum.");
                break;
            case SUSPENDED :
                LOG.warn("Connection to ZooKeeper suspended.");
                break;
            case RECONNECTED :
                LOG.info("Connection to ZooKeeper was reconnected.");
                break;
            case LOST :
                LOG.warn("Connection to ZooKeeper lost.");
                break;
            default :
                LOG.info("Received Zookeeper new state {}", newState);
                break;
        }
    }

    @Override
    public void unhandledError(String message, Throwable e) {
        LOG.warn("Unhandled error thrown {} ", message, e);
    }

    public interface WatcherListener {

        void nodeChanged(byte[] data) throws Exception;
    }

    private static class NodeCacheListenerAdapter implements NodeCacheListener {

        private final NodeCache nodeCache;

        private final String watchingPath;

        private final WatcherListener listener;

        public NodeCacheListenerAdapter(NodeCache nodeCache, String watchingPath, WatcherListener listener) {
            this.nodeCache = checkNotNull(nodeCache);
            this.watchingPath = checkNotNull(watchingPath);
            this.listener = checkNotNull(listener);
        }

        @Override
        public void nodeChanged() {
            try {
                final ChildData childData = nodeCache.getCurrentData();
                if (childData == null) {
                    LOG.info("Node of {} does not exist or is removed", watchingPath);
                    listener.nodeChanged(null);
                } else {
                    LOG.info("Node of {} updated, content is {}",
                            watchingPath, new String(childData.getData(), UTF_8));
                    listener.nodeChanged(childData.getData());
                }
            } catch (Throwable t) {
                LOG.warn("Exception thrown in callback of node changed listener on {}",
                        watchingPath, t);
            }
        }
    }

    public interface ChildrenWatcherListener {

        void onChildAdded(ChildData childData) throws Exception;

        void onChildUpdated(ChildData childData) throws Exception;

        void onChildRemoved(ChildData childData) throws Exception;

        void onInitialized(List<ChildData> childData) throws Exception;

        default void onConnectionSuspended() {
            LOG.info("Connection suspended event retrieved");
        }

        default void onConnectionReconnected() {
            LOG.info("Connection reconnected event retrieved");
        }

        default void onConnectionLost() {
            LOG.info("Connection lost event retrieved");
        }
    }

    private static class PathChildrenCacheListenerAdapter implements PathChildrenCacheListener {

        private final String watchingPath;

        private final ChildrenWatcherListener childrenWatcherListener;

        public PathChildrenCacheListenerAdapter(
                String watchingPath,
                ChildrenWatcherListener childrenWatcherListener) {
            this.watchingPath = checkNotNull(watchingPath);
            this.childrenWatcherListener = checkNotNull(childrenWatcherListener);
        }

        @Override
        public void childEvent(
                CuratorFramework curatorFramework,
                PathChildrenCacheEvent pathChildrenCacheEvent) {

            try {
                switch (pathChildrenCacheEvent.getType()) {
                    case INITIALIZED :
                        childrenWatcherListener.onInitialized(pathChildrenCacheEvent.getInitialData());
                        break;
                    case CHILD_ADDED :
                        childrenWatcherListener.onChildAdded(pathChildrenCacheEvent.getData());
                        break;
                    case CHILD_UPDATED :
                        childrenWatcherListener.onChildUpdated(pathChildrenCacheEvent.getData());
                        break;
                    case CHILD_REMOVED :
                        childrenWatcherListener.onChildRemoved(pathChildrenCacheEvent.getData());
                        break;
                    case CONNECTION_LOST :
                        childrenWatcherListener.onConnectionLost();
                        break;
                    case CONNECTION_SUSPENDED :
                        childrenWatcherListener.onConnectionSuspended();
                        break;
                    case CONNECTION_RECONNECTED :
                        childrenWatcherListener.onConnectionReconnected();
                        break;
                    default :
                        LOG.error("Undefined pathChildrenCacheEvent");
                }
            } catch (Throwable t) {
                LOG.warn("Exception thrown in callback of path children changed listener on {}",
                        watchingPath, t);
            }
        }
    }
}
