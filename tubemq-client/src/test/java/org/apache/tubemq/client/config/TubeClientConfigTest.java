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

package org.apache.tubemq.client.config;

import org.junit.Assert;
import org.junit.Test;

public class TubeClientConfigTest {

    @Test
    public void testTlsConfiguration() {
        TubeClientConfig tubeClientConfig = new TubeClientConfig("127.0.0.1:9000");
        tubeClientConfig.setTLSEnableInfo("trustStorePathPlaceholder",
                "trustStorePasswordPlaceholder");

        Assert.assertTrue(tubeClientConfig.isTlsEnable());
        Assert.assertEquals("trustStorePathPlaceholder", tubeClientConfig.getTrustStorePath());
        Assert.assertEquals("trustStorePasswordPlaceholder", tubeClientConfig.getTrustStorePassword());
        Assert.assertFalse(tubeClientConfig.isEnableTLSTwoWayAuthentic());

        Assert.assertEquals(tubeClientConfig.getKeyStorePassword(), "");
        Assert.assertEquals(tubeClientConfig.getKeyStorePath(), "");

        tubeClientConfig.toJsonString();
    }
}
