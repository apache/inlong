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

package org.apache.inlong.audit.tool.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.inlong.audit.tool.config.ConfigConstants.DEFAULT_PROMETHEUS_PORT;
import static org.apache.inlong.audit.tool.config.ConfigConstants.KEY_PROMETHEUS_PORT;

/**
 * App Config
 */
public class AppConfig {

    private Properties properties;

    public AppConfig() {
        properties = new Properties();
        loadProperties();

    }

    private void loadProperties() {
        try {
            properties.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load application properties", e);
        }
    }

    public String getManagerUrl() {
        return properties.getProperty("manager.url");
    }

    public String getAlertPolicyConfig() {
        return properties.getProperty("alert.policy.config");
    }

    public Map<String, Object> getPrometheusConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("prometheus.enabled", Boolean.parseBoolean(properties.getProperty("prometheus.enabled", "false")));
        config.put("prometheus.endpoint", properties.getProperty("prometheus.endpoint", "http://localhost:9090/api/v1/write"));
        Integer defaultPrometheusPort = DEFAULT_PROMETHEUS_PORT;
        config.put(KEY_PROMETHEUS_PORT, Integer.parseInt(properties.getProperty(KEY_PROMETHEUS_PORT, defaultPrometheusPort.toString())));
        System.out.println("Prometheus port: " + config.get(KEY_PROMETHEUS_PORT));
        return config;
    }
    public Properties getProperties(){
        return properties;
    }

}