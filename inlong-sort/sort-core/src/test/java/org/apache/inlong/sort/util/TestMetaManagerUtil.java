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

package org.apache.inlong.sort.util;

import org.apache.inlong.sort.ZkTools;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.meta.MetaManager;
import org.apache.inlong.sort.protocol.DataFlowInfo;

public abstract class TestMetaManagerUtil {

    protected static ZooKeeperTestEnvironment ZOOKEEPER;

    protected final String cluster = "cluster";

    protected final String zkRoot = "/test";

    protected final Configuration config = new Configuration();

    protected final MetaManager metaManager;

    public TestMetaManagerUtil() throws Exception {
        ZOOKEEPER = new ZooKeeperTestEnvironment(1);
        config.setString(Constants.CLUSTER_ID, cluster);
        config.setString(Constants.ZOOKEEPER_QUORUM, ZOOKEEPER.getConnectString());
        config.setString(Constants.ZOOKEEPER_ROOT, zkRoot);
        metaManager = MetaManager.getInstance(config);
    }

    public Configuration getConfig() {
        return config;
    }

    public void deleteDataFlowInfo(long dataFlowId) throws Exception {
        ZkTools.removeDataFlowFromCluster(cluster, dataFlowId, ZOOKEEPER.getConnectString(), zkRoot);
    }

    public abstract void addOrUpdateDataFlowInfo(long dataFlowId, String... args) throws Exception;

    public abstract void initDataFlowInfo(String... args) throws Exception;

    public abstract DataFlowInfo prepareDataFlowInfo(long dataFlowId, String... args);

    public void cleanup() throws Exception {
        MetaManager.release();
        ZOOKEEPER.shutdown();
    }

}
