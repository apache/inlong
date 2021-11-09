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

package org.apache.inlong.tubemq.server.master;

import com.sleepycat.je.Durability;
import org.apache.inlong.tubemq.server.common.fileconfig.MasterReplicationConfig;
import org.apache.inlong.tubemq.server.common.fileconfig.ZKConfig;
import org.junit.Assert;
import org.junit.Test;

public class MasterConfigTest {
    @Test
    public void loadFileSectAttributes() {

    }

    @Test
    public void testNormalConfig() {
        final MasterConfig masterConfig = new MasterConfig();
        masterConfig.loadFromFile(this.getClass().getResource("/master-normal.ini").getPath());

        Assert.assertEquals("127.0.0.1", masterConfig.getHostName());
        Assert.assertEquals(8000, masterConfig.getPort());
        Assert.assertEquals(8080, masterConfig.getWebPort());
        Assert.assertEquals(30000, masterConfig.getConsumerBalancePeriodMs());
        Assert.assertEquals(60000, masterConfig.getFirstBalanceDelayAfterStartMs());
        Assert.assertEquals(30000, masterConfig.getConsumerBalancePeriodMs());
        Assert.assertEquals(45000, masterConfig.getProducerHeartbeatTimeoutMs());
        Assert.assertEquals(25000, masterConfig.getBrokerHeartbeatTimeoutMs());
        Assert.assertEquals("abc", masterConfig.getConfModAuthToken());
        Assert.assertEquals("resources", masterConfig.getWebResourcePath());
        Assert.assertEquals("var/meta_data_1", masterConfig.getMetaDataPath());

        final ZKConfig zkConfig = masterConfig.getZkConfig();
        Assert.assertEquals("/tubemq", zkConfig.getZkNodeRoot());
        Assert.assertEquals("localhost:2181", zkConfig.getZkServerAddr());
        Assert.assertEquals(30000, zkConfig.getZkSessionTimeoutMs());
        Assert.assertEquals(30000, zkConfig.getZkConnectionTimeoutMs());
        Assert.assertEquals(5000, zkConfig.getZkSyncTimeMs());
        Assert.assertEquals(5000, zkConfig.getZkCommitPeriodMs());

        final MasterReplicationConfig repConfig = masterConfig.getReplicationConfig();
        Assert.assertEquals("gp1", repConfig.getRepGroupName());
        Assert.assertEquals("tubemqMasterGroupNode1", repConfig.getRepNodeName());
        Assert.assertEquals(9999, repConfig.getRepNodePort());
        Assert.assertEquals("127.0.0.1:9999", repConfig.getRepHelperHost());
        Assert.assertEquals(Durability.SyncPolicy.WRITE_NO_SYNC, repConfig.getMetaLocalSyncPolicy());
        Assert.assertEquals(Durability.SyncPolicy.SYNC, repConfig.getMetaReplicaSyncPolicy());
        Assert.assertEquals(Durability.ReplicaAckPolicy.ALL, repConfig.getRepReplicaAckPolicy());
        Assert.assertEquals(15000, repConfig.getRepStatusCheckTimeoutMs());
    }

    @Test
    public void testOptionalReplicationConfig() {
        final MasterConfig masterConfig = new MasterConfig();
        masterConfig.loadFromFile(this.getClass()
            .getResource("/master-replication-optional.ini").getPath());

        Assert.assertEquals(masterConfig.getMetaDataPath(), "var/meta_data");

        final MasterReplicationConfig repConfig = masterConfig.getReplicationConfig();
        Assert.assertEquals("tubemqMasterGroup", repConfig.getRepGroupName());
        Assert.assertEquals("tubemqMasterGroupNode1", repConfig.getRepNodeName());
        Assert.assertEquals(9001, repConfig.getRepNodePort());
        Assert.assertEquals("127.0.0.1:9001", repConfig.getRepHelperHost());
        Assert.assertEquals(Durability.SyncPolicy.SYNC, repConfig.getMetaLocalSyncPolicy());
        Assert.assertEquals(Durability.SyncPolicy.WRITE_NO_SYNC, repConfig.getMetaReplicaSyncPolicy());
        Assert.assertEquals(Durability.ReplicaAckPolicy.SIMPLE_MAJORITY, repConfig.getRepReplicaAckPolicy());
        Assert.assertEquals(10000, repConfig.getRepStatusCheckTimeoutMs());
    }

    @Test
    public void testReplicationConfigBackwardCompatibility() {
        final MasterConfig masterConfig = new MasterConfig();
        masterConfig.loadFromFile(this.getClass()
            .getResource("/master-replication-compatibility.ini").getPath());

        Assert.assertEquals("var/tubemqMasterGroup/master_data", masterConfig.getMetaDataPath());

        final MasterReplicationConfig repConfig = masterConfig.getReplicationConfig();
        Assert.assertEquals("gp1", repConfig.getRepGroupName());
        Assert.assertEquals("tubemqMasterGroupNode1", repConfig.getRepNodeName());
        Assert.assertEquals(9999, repConfig.getRepNodePort());
        Assert.assertEquals("127.0.0.1:9999", repConfig.getRepHelperHost());
        Assert.assertEquals(Durability.SyncPolicy.WRITE_NO_SYNC, repConfig.getMetaLocalSyncPolicy());
        Assert.assertEquals(Durability.SyncPolicy.SYNC, repConfig.getMetaReplicaSyncPolicy());
        Assert.assertEquals(Durability.ReplicaAckPolicy.ALL, repConfig.getRepReplicaAckPolicy());
        Assert.assertEquals(15000, repConfig.getRepStatusCheckTimeoutMs());
    }
}
