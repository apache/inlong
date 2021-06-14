/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.master;

import com.sleepycat.je.Durability;

import org.apache.tubemq.corebase.config.Configuration;
import org.apache.tubemq.corebase.config.TLSConfig;
import org.apache.tubemq.corebase.config.TlsConfItems;
import org.apache.tubemq.server.common.fileconfig.MasterReplicationConfig;
import org.apache.tubemq.server.common.fileconfig.ZKConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MasterConfigTest {

    private String targetConfFilePath;
    private MasterConfig masterConfig;
    private String tlsPlaceholderInMaster = "tlsPlaceholderInMaster";

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

    @Before
    public void init() {
        this.targetConfFilePath = MasterConfigTest.class.getClassLoader()
                .getResource("master-for-new-tls-test.ini").getPath();
    }

    @Test
    public void testTlsConfiguration() {
        testOldTlsConfig();
        testNewTlsConfig();
    }

    private void testNewTlsConfig() {
        masterConfig = new MasterConfig();
        masterConfig.loadFromFile(targetConfFilePath);
        Configuration configuration = masterConfig.getTlsConfiguration();
        Assert.assertTrue(configuration.getBoolean(TlsConfItems.TLS_ENABLE));
        Assert.assertEquals(80889, (int) configuration.getInteger(TlsConfItems.TLS_PORT));
        Assert.assertEquals(tlsPlaceholderInMaster, configuration.getString(TlsConfItems.TLS_KEY_STORE_PATH));
        Assert.assertEquals(tlsPlaceholderInMaster, configuration.get(TlsConfItems.TLS_KEY_STORE_PASSWORD));
        Assert.assertTrue(configuration.get(TlsConfItems.TLS_TWO_WAY_AUTH_ENABLE));
        Assert.assertEquals(tlsPlaceholderInMaster, configuration.get(TlsConfItems.TLS_TRUST_STORE_PATH));
        Assert.assertEquals(tlsPlaceholderInMaster, configuration.get(TlsConfItems.TLS_TRUST_STORE_PASSWORD));

        Configuration newConfiguration = new Configuration();
        newConfiguration.addAll(configuration);
        newConfiguration.set(TlsConfItems.TLS_PORT, 1);
        newConfiguration.set("newConfigItem1", "newConfigValue");
        masterConfig.setTlsConfiguration(newConfiguration);
        Assert.assertEquals(1, masterConfig.getTlsConfiguration().get(TlsConfItems.TLS_PORT), 0);
        Assert.assertNull(masterConfig.getTlsConfiguration().getString("newConfigItem1", null));
    }

    private void testOldTlsConfig() {
        masterConfig = new MasterConfig();
        masterConfig.loadFromFile(targetConfFilePath);
        TLSConfig tlsConfig = masterConfig.getTlsConfig();
        Assert.assertTrue(tlsConfig.isTlsEnable());
        Assert.assertTrue(tlsConfig.isTlsTwoWayAuthEnable());
        Assert.assertEquals(tlsPlaceholderInMaster, tlsConfig.getTlsKeyStorePassword());
        Assert.assertEquals(tlsPlaceholderInMaster, tlsConfig.getTlsKeyStorePath());
        Assert.assertEquals(tlsPlaceholderInMaster, tlsConfig.getTlsTrustStorePassword());
        Assert.assertEquals(tlsPlaceholderInMaster, tlsConfig.getTlsTrustStorePath());
        Assert.assertEquals(80889, tlsConfig.getTlsPort(), 0);

        tlsConfig.setTlsPort(2);
        tlsConfig.setTlsEnable(false);
        masterConfig.setTlsConfiguration(tlsConfig);
        Assert.assertEquals(2, masterConfig.getTlsConfig().getTlsPort(), 0);
        Assert.assertFalse(masterConfig.isTlsEnable());
    }
}
