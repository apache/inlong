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

package org.apache.inlong.agent.plugin.store;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.store.KeyValueEntity;
import org.apache.inlong.agent.store.Store;

import com.google.gson.Gson;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_TAG;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_LOCAL_IP;

/**
 * Store implement based on the ZooKeeper.
 */
public class ZooKeeperImpl implements Store {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperImpl.class);
    private static final Gson GSON = new Gson();
    public static final int SLEEP_MS_BETWEEN_RETRIES = 1000 * 10;
    public static final int MAX_RETRIES = 10;
    public static final int SESSION_TIMEOUT_MS = 1000 * 60 * 5;
    public static final int CONNECTION_TIMEOUT_MS = 1000 * 10;
    public static final String SPLITTER = "/";
    private static CuratorFramework client;
    private String uniqueKey;
    private static final AgentConfiguration conf = AgentConfiguration.getAgentConf();
    private static final String ZK_PRE = "/agent";

    public ZooKeeperImpl(String childPath) {
        uniqueKey = ZK_PRE + getSplitter() + conf.get(AGENT_CLUSTER_TAG) + getSplitter() + conf.get(AGENT_CLUSTER_NAME)
                + getSplitter() + conf.get(AGENT_LOCAL_IP) + getSplitter() + childPath;
    }

    /**
     * get client
     *
     * @return zookeeper client
     */
    private static CuratorFramework getClient() {
        if (client == null) {
            synchronized (ZooKeeperImpl.class) {
                if (client == null) {
                    if (AgentManager.getAgentConfigInfo() == null) {
                        throw new RuntimeException("agent config is null");
                    }
                    RetryPolicy retryPolicy = new RetryNTimes(SLEEP_MS_BETWEEN_RETRIES, MAX_RETRIES);
                    client = CuratorFrameworkFactory.newClient(AgentManager.getAgentConfigInfo().getZkUrl(),
                            SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS, retryPolicy);
                    client.start();
                }
            }
        }
        return client;
    }

    @Override
    public KeyValueEntity get(String key) {
        try {
            byte[] bytes = getClient().getData().forPath(key);
            return bytes == null ? null : GSON.fromJson(new String(bytes), KeyValueEntity.class);
        } catch (NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException("get key value entity error", e);
        }
    }

    @Override
    public void put(KeyValueEntity entity) {
        Stat stat;
        try {
            byte[] data = GSON.toJson(entity).getBytes(StandardCharsets.UTF_8);
            stat = getClient().checkExists().forPath(entity.getKey());
            if (stat == null) {
                getClient().create().creatingParentsIfNeeded()
                        .forPath(entity.getKey(), data);
            } else {
                getClient().setData().forPath(entity.getKey(), data);
            }
        } catch (Exception e) {
            LOGGER.error("Path {}, put has exception",
                    entity.getKey(), e);
        }
    }

    @Override
    public KeyValueEntity remove(String key) {
        Stat stat;
        try {
            stat = getClient().checkExists().forPath(key);
            if (stat != null) {
                getClient().delete().forPath(key);
            }
        } catch (Exception e) {
            LOGGER.error("Path {}, remove has exception", key, e);
        }
        return null;
    }

    @Override
    public List<KeyValueEntity> findAll(String prefix) {
        List<KeyValueEntity> result = new ArrayList<>();
        List<String> nodes = getLeafNodes(prefix);
        for (int i = 0; i < nodes.size(); i++) {
            result.add(get(nodes.get(i)));
        }
        return result;
    }

    private List<String> getLeafNodes(String path) {
        List<String> leafNodes = new ArrayList<>();
        try {
            List<String> children = getClient().getChildren().forPath(path);
            if (children.isEmpty()) {
                leafNodes.add(path);
            } else {
                for (int i = 0; i < children.size(); i++) {
                    leafNodes.addAll(getLeafNodes(path + getSplitter() + children.get(i)));
                }
            }
        } catch (NoNodeException e) {
            return leafNodes;
        } catch (Exception e) {
            LOGGER.error("getLeafNodes path {} error", path, e);
        }
        return leafNodes;
    }

    @Override
    public String getSplitter() {
        return SPLITTER;
    }

    @Override
    public String getUniqueKey() {
        return uniqueKey;
    }

    /**
     * replace keywords, file name /data/log/test.log will be trans to #data#log#test.log
     * to prevent the path depth of zk to be too large
     * @return string after replace
     */
    @Override
    public String replaceKeywords(String source) {
        return source.replace("/", "#");
    }

    @Override
    public void close() throws IOException {
        getClient().close();
    }
}
