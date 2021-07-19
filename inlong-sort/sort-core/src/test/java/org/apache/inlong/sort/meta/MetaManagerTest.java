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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.inlong.sort.ZkTools;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.CsvDeserializationInfo;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo.PartitionStrategy;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.apache.inlong.sort.util.ZooKeeperTestEnvironment;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetaManagerTest {
    private static ZooKeeperTestEnvironment ZOOKEEPER;

    private final String cluster = "cluster";

    private final String zkRoot = "/test";

    @Before
    public void setUp() throws Exception {
        ZOOKEEPER = new ZooKeeperTestEnvironment(1);
    }

    @After
    public void cleanUp() throws Exception {
        MetaManager.release();
        ZOOKEEPER.shutdown();
    }

    private DataFlowInfo prepareDataFlowInfo(
            long dataFlowId) {
        return new DataFlowInfo(
                dataFlowId,
                new TubeSourceInfo("topic" + System.currentTimeMillis(), "ma", "cg",
                        new CsvDeserializationInfo(','), new FieldInfo[0]),
                new ClickHouseSinkInfo("url", "dn", "tn", "un", "pw",
                        false, PartitionStrategy.HASH, "pk", new FieldInfo[0], new String[0],
                        100, 100, 100));
    }

    @Test(timeout = 30000)
    public void testAddAndUpdateAndRemoveDataFlow() throws Exception {
        final Configuration config = new Configuration();
        config.setString(Constants.CLUSTER_ID, cluster);
        config.setString(Constants.ZOOKEEPER_QUORUM, ZOOKEEPER.getConnectString());
        config.setString(Constants.ZOOKEEPER_ROOT, zkRoot);

        final MetaManager metaManager = MetaManager.getInstance(config);
        final TestDataFlowInfoListener testDataFlowInfoListener = new TestDataFlowInfoListener();
        metaManager.registerDataFlowInfoListener(testDataFlowInfoListener);

        // add dataFlow
        ZkTools.addDataFlowToCluster(cluster, 1, ZOOKEEPER.getConnectString(), zkRoot);
        ZkTools.updateDataFlowInfo(
                prepareDataFlowInfo(1),
                cluster,
                1,
                ZOOKEEPER.getConnectString(),
                zkRoot);

        // update dataFlow
        ZkTools.updateDataFlowInfo(
                prepareDataFlowInfo(1),
                cluster,
                1,
                ZOOKEEPER.getConnectString(),
                zkRoot);

        // remove dataFlow
        ZkTools.removeDataFlowFromCluster(cluster, 1, ZOOKEEPER.getConnectString(), zkRoot);

        while (true) {
            List<Object> operations = testDataFlowInfoListener.getOperations();
            if (operations.size() == 1) {
                assertTrue(operations.get(0) instanceof DataFlowInfo);
                assertEquals(1, ((DataFlowInfo) operations.get(0)).getId());
            } else if (operations.size() == 2) {
                assertTrue(operations.get(1) instanceof DataFlowInfo);
                assertEquals(1, ((DataFlowInfo) operations.get(0)).getId());
                break;
            } else if (operations.size() == 3) {
                assertTrue(operations.get(2) instanceof DataFlowInfo);
                assertEquals(1, ((DataFlowInfo) operations.get(0)).getId());
                break;
            } else {
                Thread.sleep(100);
            }
        }
    }

    @Test(timeout = 30000)
    public void testInitializeDataFlow() throws Exception {
        final Configuration config = new Configuration();
        config.setString(Constants.CLUSTER_ID, cluster);
        config.setString(Constants.ZOOKEEPER_QUORUM, ZOOKEEPER.getConnectString());
        config.setString(Constants.ZOOKEEPER_ROOT, zkRoot);

        // add dataFlow
        ZkTools.addDataFlowToCluster(cluster, 1, ZOOKEEPER.getConnectString(), zkRoot);
        ZkTools.updateDataFlowInfo(
                prepareDataFlowInfo(1),
                cluster,
                1,
                ZOOKEEPER.getConnectString(),
                zkRoot);

        // open MetaManager
        final MetaManager metaManager = MetaManager.getInstance(config);

        // register dataFlowInfoListener
        final TestDataFlowInfoListener testDataFlowInfoListener = new TestDataFlowInfoListener();
        metaManager.registerDataFlowInfoListener(testDataFlowInfoListener);

        List<Object> operations = testDataFlowInfoListener.getOperations();
        assertEquals(1, operations.size());
        assertTrue(operations.get(0) instanceof DataFlowInfo);
        assertEquals(1, ((DataFlowInfo) operations.get(0)).getId());

    }

    static class TestDataFlowInfoListener implements DataFlowInfoListener {
        private static final Object lock = new Object();

        List<Object> operations = new ArrayList<>();

        @Override
        public void addDataFlow(DataFlowInfo dataFlowInfo) {
            synchronized (lock) {
                operations.add(dataFlowInfo);
            }
        }

        @Override
        public void updateDataFlow(DataFlowInfo dataFlowInfo) {
            synchronized (lock) {
                operations.add(dataFlowInfo);
            }
        }

        @Override
        public void removeDataFlow(DataFlowInfo dataFlowInfo) {
            synchronized (lock) {
                operations.add(dataFlowInfo);
            }
        }

        public List<Object> getOperations() {
            synchronized (lock) {
                return operations;
            }
        }
    }
}
