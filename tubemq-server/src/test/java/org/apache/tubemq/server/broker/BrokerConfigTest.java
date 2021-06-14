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

package org.apache.tubemq.server.broker;

import org.apache.tubemq.corebase.config.Configuration;
import org.apache.tubemq.corebase.config.TLSConfig;
import org.apache.tubemq.corebase.config.TlsConfItems;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BrokerConfigTest {

    private String targetConfFilePath;
    private BrokerConfig brokerConfig;
    private String tlsPlaceholderInBroker = "tlsPlaceholderInBroker";

    @Before
    public void init() {
        this.targetConfFilePath = BrokerConfigTest.class.getClassLoader()
                .getResource("broker-for-new-tls-test.ini").getPath();
    }

    @Test
    public void testTlsConfiguration() {
        testOldTlsConfig();
        testNewTlsConfig();
    }

    private void testNewTlsConfig() {
        brokerConfig = new BrokerConfig();
        brokerConfig.loadFromFile(targetConfFilePath);
        Configuration configuration = brokerConfig.getTlsConfiguration();
        Assert.assertTrue(configuration.getBoolean(TlsConfItems.TLS_ENABLE));
        Assert.assertEquals(80888, (int) configuration.getInteger(TlsConfItems.TLS_PORT));
        Assert.assertEquals(tlsPlaceholderInBroker, configuration.getString(TlsConfItems.TLS_KEY_STORE_PATH));
        Assert.assertEquals(tlsPlaceholderInBroker, configuration.get(TlsConfItems.TLS_KEY_STORE_PASSWORD));
        Assert.assertTrue(configuration.get(TlsConfItems.TLS_TWO_WAY_AUTH_ENABLE));
        Assert.assertEquals(tlsPlaceholderInBroker, configuration.get(TlsConfItems.TLS_TRUST_STORE_PATH));
        Assert.assertEquals(tlsPlaceholderInBroker, configuration.get(TlsConfItems.TLS_TRUST_STORE_PASSWORD));

        Configuration newConfiguration = new Configuration();
        newConfiguration.addAll(configuration);
        newConfiguration.set(TlsConfItems.TLS_PORT, 1);
        newConfiguration.set("newConfigItem1", "newConfigValue");
        brokerConfig.setTlsConfiguration(newConfiguration);
        Assert.assertEquals(1, brokerConfig.getTlsPort(), 0);
        Assert.assertNull(brokerConfig.getTlsConfiguration().getString("newConfigItem1", null));
    }

    private void testOldTlsConfig() {
        brokerConfig = new BrokerConfig();
        brokerConfig.loadFromFile(targetConfFilePath);
        TLSConfig tlsConfig = brokerConfig.getTlsConfig();
        Assert.assertTrue(tlsConfig.isTlsEnable());
        Assert.assertTrue(tlsConfig.isTlsTwoWayAuthEnable());
        Assert.assertEquals(tlsPlaceholderInBroker, tlsConfig.getTlsKeyStorePassword());
        Assert.assertEquals(tlsPlaceholderInBroker, tlsConfig.getTlsKeyStorePath());
        Assert.assertEquals(tlsPlaceholderInBroker, tlsConfig.getTlsTrustStorePassword());
        Assert.assertEquals(tlsPlaceholderInBroker, tlsConfig.getTlsTrustStorePath());
        Assert.assertEquals(80888, tlsConfig.getTlsPort(), 0);

        tlsConfig.setTlsPort(2);
        tlsConfig.setTlsEnable(false);
        brokerConfig.setTlsConfiguration(tlsConfig);
        Assert.assertEquals(2, brokerConfig.getTlsPort());
        Assert.assertFalse(brokerConfig.isTlsEnable());
    }
}
