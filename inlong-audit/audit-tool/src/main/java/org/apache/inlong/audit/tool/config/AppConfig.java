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

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.inlong.audit.tool.config.ConfigConstants.*;

/**
 * App Config
 */
@Getter
public class AppConfig {

    private final Properties properties;

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

    public Map<String, Object> getPrometheusConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(KEY_PROMETHEUS_ENABLED,
                Boolean.parseBoolean(properties.getProperty(KEY_PROMETHEUS_ENABLED, "false")));
        config.put(KEY_PROMETHEUS_ENDPOINT,
                properties.getProperty(KEY_PROMETHEUS_ENDPOINT, "http://localhost:9090/api/v1/write"));
        config.put(KEY_PROMETHEUS_PORT,
                Integer.parseInt(properties.getProperty(KEY_PROMETHEUS_PORT,
                        String.valueOf(DEFAULT_PROMETHEUS_PORT))));
        return config;
    }

    public String getSecretId() {
        return properties.getProperty("audit.secretId");
    }

    public String getSecretKey() {
        return properties.getProperty("audit.secretKey");
    }
}