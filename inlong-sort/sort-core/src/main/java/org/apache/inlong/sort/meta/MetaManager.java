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

import static org.apache.inlong.sort.configuration.ConfigOptions.key;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.GuardedBy;

import org.apache.inlong.sort.configuration.ConfigOption;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.meta.zookeeper.ZookeeperMetaWatcher;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.objenesis.instantiator.util.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the manager of meta. It's a singleton and shared by many components from different threads. So it's
 * thread-safe.
 *
 */
public class MetaManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetaManager.class);

    public static final ConfigOption<String> META_WATCHER_TYPE = key("meta.watcher.type")
            .defaultValue(ZookeeperMetaWatcher.class.getName())
            .withDescription("Defines the meta watcher class name.");

    private static MetaManager instance;

    public static final Object LOCK = new Object();

    private MetaWatcher watcher;

    @GuardedBy("lock")
    private final Map<Long, DataFlowInfo> dataFlowInfoMap;

    @GuardedBy("lock")
    private final List<DataFlowInfoListener> dataFlowInfoListeners;

    public static MetaManager getInstance(Configuration config) throws Exception {
        synchronized (LOCK) {
            if (instance == null) {
                instance = new MetaManager(config);
                instance.open(config);
            }
            return instance;
        }
    }

    public static void release() throws Exception {
        synchronized (LOCK) {
            if (instance != null) {
                instance.close();
                instance = null;
            }
        }
    }

    /**
     * Constructor
     * 
     * @param config process start parameters
     */
    private MetaManager(Configuration config) {
        dataFlowInfoMap = new ConcurrentHashMap<>();
        dataFlowInfoListeners = new ArrayList<>();
    }

    /**
     * open
     * 
     * @param  config
     * @throws Exception
     */
    private void open(Configuration config) throws Exception {
        String watcherClass = config.getString(META_WATCHER_TYPE);
        this.watcher = (MetaWatcher) ClassUtils.newInstance(Class.forName(watcherClass));
        this.watcher.open(config, new MetaDataFlowInfoListener(this));
    }

    /**
     * close
     * 
     * @throws Exception
     */
    public void close() throws Exception {
        synchronized (LOCK) {
            if (watcher != null) {
                watcher.close();
            }
        }
    }

    /**
     * registerDataFlowInfoListener
     * 
     * @param  dataFlowInfoListener
     * @throws Exception
     */
    public void registerDataFlowInfoListener(DataFlowInfoListener dataFlowInfoListener) throws Exception {
        synchronized (LOCK) {
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

    /**
     * getDataFlowInfo
     * 
     * @param  id
     * @return
     */
    public DataFlowInfo getDataFlowInfo(long id) {
        synchronized (LOCK) {
            return dataFlowInfoMap.get(id);
        }
    }

    /**
     * addDataFlow
     * 
     * @param  dataFlowInfo
     * @throws Exception
     */
    public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        synchronized (LOCK) {
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

    /**
     * updateDataFlow
     * 
     * @param  dataFlowInfo
     * @throws Exception
     */
    public void updateDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        synchronized (LOCK) {
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

    /**
     * removeDataFlow
     * 
     * @param  dataFlowInfo
     * @throws Exception
     */
    public void removeDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        synchronized (LOCK) {
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

    /**
     * MetaManager DataFlowInfoListener
     */
    public interface DataFlowInfoListener {

        void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception;

        void updateDataFlow(DataFlowInfo dataFlowInfo) throws Exception;

        void removeDataFlow(DataFlowInfo dataFlowInfo) throws Exception;
    }
}
