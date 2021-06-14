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

package org.apache.tubemq.corebase.config;

import java.util.Objects;

import org.apache.tubemq.corebase.config.constants.TLSCfgConst;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationLoaderTest {

    public static final String TLS_PLACEHOLDER_OF_BROKER = "tlsPlaceholderInBroker";
    public static final String TLS_PLACEHOLDER_OF_MASTER = "tlsPlaceholderInMaster";

    public String getDefaultConfFilePath(String filename) {
        return Objects.requireNonNull(this.getClass().getClassLoader().getResource(filename)).getPath();
    }

    @Test
    public void testLoadFromBrokerIni() {
        Configuration configuration = ConfigurationUtils
                .loadConfiguration(getDefaultConfFilePath("broker.ini"), TLSCfgConst.SECT_TOKEN_TLS);
        Assert.assertTrue(configuration.getBoolean(TlsConfItems.TLS_ENABLE));
        Assert.assertEquals(80888, (int) configuration.getInteger(TlsConfItems.TLS_PORT));
        Assert.assertEquals(TLS_PLACEHOLDER_OF_BROKER, configuration.getString(TlsConfItems.TLS_KEY_STORE_PATH));
        Assert.assertEquals(TLS_PLACEHOLDER_OF_BROKER, configuration.get(TlsConfItems.TLS_KEY_STORE_PASSWORD));
        Assert.assertTrue(configuration.get(TlsConfItems.TLS_TWO_WAY_AUTH_ENABLE));
        Assert.assertEquals(TLS_PLACEHOLDER_OF_BROKER, configuration.get(TlsConfItems.TLS_TRUST_STORE_PATH));
        Assert.assertEquals(TLS_PLACEHOLDER_OF_BROKER, configuration.get(TlsConfItems.TLS_TRUST_STORE_PASSWORD));
    }

    @Test
    public void testLoadFromMasterIni() {
        Configuration configuration = ConfigurationUtils
                .loadConfiguration(getDefaultConfFilePath("master.ini"), TLSCfgConst.SECT_TOKEN_TLS);
        Assert.assertFalse(configuration.getBoolean(TlsConfItems.TLS_ENABLE));
        assert configuration.getInteger(TlsConfItems.TLS_PORT) == 80889;
        Assert.assertEquals(TLS_PLACEHOLDER_OF_MASTER, configuration.getString(TlsConfItems.TLS_KEY_STORE_PATH));
        Assert.assertEquals(TLS_PLACEHOLDER_OF_MASTER, configuration.get(TlsConfItems.TLS_KEY_STORE_PASSWORD));
        Assert.assertTrue(configuration.get(TlsConfItems.TLS_TWO_WAY_AUTH_ENABLE));
        Assert.assertEquals(TLS_PLACEHOLDER_OF_MASTER, configuration.get(TlsConfItems.TLS_TRUST_STORE_PATH));
        Assert.assertEquals(TLS_PLACEHOLDER_OF_MASTER, configuration.get(TlsConfItems.TLS_TRUST_STORE_PASSWORD));
    }
}
