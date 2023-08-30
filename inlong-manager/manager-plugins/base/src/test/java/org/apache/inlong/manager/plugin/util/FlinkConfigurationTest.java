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

package org.apache.inlong.manager.plugin.util;

import org.apache.inlong.manager.plugin.flink.dto.FlinkConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class FlinkConfigurationTest {

    private FlinkConfig flinkConfig;
    private String address;
    private Integer port;
    private Integer jobManagerPort;
    private String savepointDirectory;
    private Integer parallelism;
    private boolean drain;
    private String auditProxyHosts;
    // flink version
    private String version;

    @Test
    public void getFlinkConfigTest() {
        try {
            FlinkConfiguration flinkConfiguration = new FlinkConfiguration();
            flinkConfig = flinkConfiguration.getFlinkConfig();
            address = flinkConfig.getAddress();
            port = flinkConfig.getPort();
            jobManagerPort = flinkConfig.getJobManagerPort();
            savepointDirectory = flinkConfig.getSavepointDirectory();
            parallelism = flinkConfig.getParallelism();
            auditProxyHosts = flinkConfig.getAuditProxyHosts();
            drain = flinkConfig.isDrain();
            version = flinkConfig.getVersion();
            Assertions.assertEquals(address, "127.0.0.1");
            Assertions.assertEquals(port, 8081);
            Assertions.assertEquals(jobManagerPort, 6123);
            Assertions.assertEquals(savepointDirectory, "file:///data/inlong-sort/savepoints");
            Assertions.assertEquals(parallelism, 1);
            Assertions.assertEquals(auditProxyHosts, "127.0.0.1:10081");
            Assertions.assertEquals(drain, false);
            Assertions.assertEquals(version, "1.13");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void setFlinkConfig() {
        try {
            Map<String, String> flinkConfigMap = new HashMap<>();
            flinkConfigMap.put("address", "0.0.0.0");
            flinkConfigMap.put("port", "8080");
            flinkConfigMap.put("jobManagerPort", "6000");
            flinkConfigMap.put("savepointDirectory", "file:///data/inlong-sort/savepoints/test");
            flinkConfigMap.put("parallelism", "100");
            flinkConfigMap.put("auditProxyHosts", "0.0.0.0:10081");
            flinkConfigMap.put("drain", "true");
            flinkConfigMap.put("version", "1.15");
            FlinkConfiguration flinkConfiguration = new FlinkConfiguration();
            flinkConfiguration.setFlinkConfig(flinkConfigMap);
            flinkConfig = flinkConfiguration.getFlinkConfig();
            address = flinkConfig.getAddress();
            port = flinkConfig.getPort();
            jobManagerPort = flinkConfig.getJobManagerPort();
            savepointDirectory = flinkConfig.getSavepointDirectory();
            parallelism = flinkConfig.getParallelism();
            auditProxyHosts = flinkConfig.getAuditProxyHosts();
            drain = flinkConfig.isDrain();
            version = flinkConfig.getVersion();
            Assertions.assertEquals(address, "0.0.0.0");
            Assertions.assertEquals(port, 8080);
            Assertions.assertEquals(jobManagerPort, 6000);
            Assertions.assertEquals(savepointDirectory, "file:///data/inlong-sort/savepoints/test");
            Assertions.assertEquals(parallelism, 100);
            Assertions.assertEquals(auditProxyHosts, "0.0.0.0:10081");
            Assertions.assertEquals(drain, true);
            Assertions.assertEquals(version, "1.15");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
